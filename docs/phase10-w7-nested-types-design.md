# Phase 10 W7: Nested Types (Lists, Structs, Maps)

**Date**: 2025-11-04
**Status**: Design Phase
**Goal**: Write nested Parquet structures (lists, structs, maps) with correct repetition/definition level encoding
**Timeline**: 3-4 weeks

---

## Executive Summary

W7 implements the final major feature for Phase 10: **nested type writing**. This enables writing:
- Lists (arrays) of primitives: `[[Int32]]`
- Multi-level lists: `[[[String]]]`
- Structs (groups): `{name: String, age: Int32}`
- Maps: `[String: Int32]`
- Nested combinations: lists of structs, structs with list fields, etc.

**Key Challenge**: Correctly computing and encoding **repetition levels** and **definition levels** for nested structures.

**Approach**: Implement the inverse of the reader's `ArrayReconstructor` - flatten nested Swift structures into values + levels.

---

## Background: Repetition & Definition Levels

**Reference**: This implementation mirrors Apache Arrow C++'s `LevelInfo` and `ArrayWriter::WriteValues` behavior. See Arrow C++ documentation:
- `cpp/src/parquet/level_conversion.h` - Level computation algorithms
- `cpp/src/parquet/column_writer.cc` - `ArrayWriter::WriteValues()` implementation
- Arrow docs: https://arrow.apache.org/docs/format/Columnar.html#nested-types

### Definition Levels (Already Implemented in W4/W5)

**Purpose**: Distinguish NULL, empty, and present values

For a list field with `maxDefinitionLevel = 2`:
- `def = 0`: NULL list
- `def = 1`: Empty list (present, zero elements)
- `def = 2`: List with values

**Example**:
```swift
let lists: [[Int32]?] = [
    [1, 2],   // Present, has values → def = 2, 2
    [],       // Present, empty → def = 1
    nil       // NULL → def = 0
]
// Definition levels: [2, 2, 1, 0]
```

### Repetition Levels (NEW in W7)

**Purpose**: Indicate list boundaries in repeated fields

For a list field with `maxRepetitionLevel = 1`:
- `rep = 0`: Start of new list (new top-level element)
- `rep = 1`: Continuation of current list (additional element)

**Example**:
```swift
let lists: [[Int32]] = [
    [1, 2, 3],  // rep = 0 for "1", rep = 1 for "2", rep = 1 for "3"
    [4, 5],     // rep = 0 for "4", rep = 1 for "5"
    []          // rep = 0 (empty list still creates one entry)
]
// Values:     [1, 2, 3, 4, 5]
// Rep levels: [0, 1, 1, 0, 1]  (continuation of lists)
// Def levels: [2, 2, 2, 2, 2, 1]  (last entry is empty list, def=1)
```

### Multi-Level Lists

For `maxRepetitionLevel = 2` (e.g., `[[[Int32]]]`):
- `rep = 0`: New outer list (top-level boundary)
- `rep = 1`: New middle list (within same outer list)
- `rep = 2`: Continuation of innermost list (same middle list)

**Example** (using table format for alignment):
```swift
let lists: [[[Int32]]] = [
    [[1, 2], [3]],   // Outer list with 2 inner lists
    [[4]]            // New outer list with 1 inner list
]

Index:      0  1  2  3
values:     1  2  3  4
repLevels:  0  2  1  0
defLevels:  3  3  3  3

Explanation:
- Index 0: rep=0 (new outer list [[1,2],[3]]), def=3 (value present), value=1
- Index 1: rep=2 (continue innermost [1,2]), def=3 (value present), value=2
- Index 2: rep=1 (new middle list [3] within same outer), def=3 (value present), value=3
- Index 3: rep=0 (new outer list [[4]]), def=3 (value present), value=4
```

**Critical Rule**: Emit at least one (rep, def) tuple per logical parent, even for empty lists. This matches Arrow C++'s `ArrayWriter::WriteValues()` behavior.

---

## Architecture Design

### Component Overview

```
┌─────────────────────────────────────┐
│      ListColumnWriter               │
│  - Accepts nested Swift arrays      │
│  - Computes rep/def levels          │
│  - Flattens to primitive values     │
│  - Delegates to primitive writer    │
└─────────────────────────────────────┘
             │
             ├─> LevelComputer
             │   - computeRepetitionLevels()
             │   - computeDefinitionLevels()
             │
             ├─> LevelEncoder (from W4)
             │   - RLE/bit-packed hybrid encoding
             │
             └─> Int32ColumnWriter (reuse)
                 - Writes flattened primitive values
```

### Level Computation Algorithm

**Core Insight**: For each primitive value in the nested structure, compute:
1. **Repetition level**: How deep in the nesting is this a "continuation"?
2. **Definition level**: Is the value present, or is an ancestor NULL/empty?

**Implementation Strategy**:
- Recursive traversal of nested structure
- Track current depth and ancestor state
- Emit (value, repLevel, defLevel) tuples
- Flatten tuples for writing

---

## Implementation Plan

### Phase 1: Repetition Level Encoding (Week 1)

**Goal**: Extend W4's RLE encoder to handle repetition levels

#### Tasks:
- [x] RLE encoder already exists from W4 (used for definition levels)
- [ ] Add `ColumnWriter.writeRepetitionLevels()` helper
- [ ] Test encoding/decoding round-trip with reader's RLE decoder

**Deliverable**: Can encode repetition levels to RLE format

---

### Phase 2: Single-Level Lists (Week 1-2)

**Goal**: Write `[[T]]` (list of primitives)

#### 2.1: Level Computation

Implement `LevelComputer` utility:

```swift
struct LevelComputer {
    /// Compute repetition and definition levels for a single-level list
    ///
    /// **CRITICAL**: Empty/NULL lists produce level entries WITHOUT value entries!
    ///
    /// Input:  [[1, 2], [], [3]]
    /// Output (note: 4 level entries, 3 values):
    ///   Index:      0  1  2  3
    ///   values:     1  2  -  3      (- = no value for empty list)
    ///   repLevels:  0  1  0  0      (0=new list, 1=continuation, 0=empty list, 0=new list)
    ///   defLevels:  2  2  1  2      (2=value present, 1=empty list, 2=value present)
    ///
    /// Entry at index 2 represents the empty list []:
    ///   - rep=0 (new list)
    ///   - def=1 (list present but empty)
    ///   - NO value emitted
    ///
    /// This ensures: level_count >= value_count (equality only when no empty/NULL lists)
    static func computeLevelsForList<T>(
        lists: [[T]?],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int
    ) -> (values: [T], repLevels: [UInt16], defLevels: [UInt16])
}
```

**Algorithm**:
```swift
for (listIndex, list) in lists.enumerated() {
    if list == nil {
        // NULL list
        repLevels.append(0)
        defLevels.append(0)
        continue
    }

    if list.isEmpty {
        // Empty list (present, zero elements)
        repLevels.append(0)
        defLevels.append(1)
        continue
    }

    // List with values
    for (valueIndex, value) in list.enumerated() {
        values.append(value)
        repLevels.append(valueIndex == 0 ? 0 : 1)  // 0=new list, 1=continuation
        defLevels.append(2)  // Value present
    }
}
```

#### 2.2: ListColumnWriter Implementation

```swift
public final class ListColumnWriter<T> {
    private let primitiveWriter: ColumnWriter  // Reuse Int32ColumnWriter, etc.
    private let column: Column
    private let properties: WriterProperties

    public func writeValues(_ lists: [[T]]) throws {
        // 1. Compute levels
        let (values, repLevels, defLevels) = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: column.maxDefinitionLevel,
            maxRepetitionLevel: column.maxRepetitionLevel
        )

        // 2. Write levels (encoded as RLE)
        try writeRepetitionLevels(repLevels)
        try writeDefinitionLevels(defLevels)

        // 3. Write primitive values
        try primitiveWriter.writeValues(values)
    }
}
```

#### 2.3: Integration with RowGroupWriter

Update `RowGroupWriter` to support list columns:

```swift
extension RowGroupWriter {
    public func listColumnWriter<T>(at index: Int) throws -> ListColumnWriter<T> {
        let column = schema.column(at: index)
        guard column.repetitionType == .repeated else {
            throw WriterError.invalidColumnType("Expected repeated column")
        }

        // Create primitive writer for list elements
        let primitiveWriter = try primitiveColumnWriter(column.elementColumn)

        return ListColumnWriter(
            primitiveWriter: primitiveWriter,
            column: column,
            properties: properties
        )
    }
}
```

#### 2.4: Testing

**Unit Tests**:
- `testSingleLevelListLevelComputation`: Verify rep/def levels for `[[1,2],[3]]`
- `testEmptyListLevels`: Verify empty list gets `rep=0, def=1`
- `testNullListLevels`: Verify NULL list gets `rep=0, def=0`

**Integration Tests**:
- Write `[[Int32]]` file → read back with reader → verify arrays match
- Test with nullable lists: `[[Int32]?]`
- Test with nullable elements: `[[Int32?]]`

**Deliverable**: Can write single-level lists

---

### Phase 3: Multi-Level Lists (Week 2)

**Goal**: Write `[[[T]]]`, `[[[[T]]]]`, etc.

#### 3.1: Recursive Level Computation

Extend `LevelComputer` for arbitrary nesting:

```swift
struct LevelComputer {
    /// Recursively compute levels for multi-level lists
    ///
    /// Example: [[[1, 2], [3]], [[4]]]
    ///   - maxRepLevel = 2 (3 nesting levels: outer, middle, inner)
    ///   - Values: [1, 2, 3, 4]
    ///   - RepLevels: [0, 2, 1, 0]
    ///     - 0: New outer list
    ///     - 2: Continue innermost list
    ///     - 1: New middle list (second inner list in same outer)
    ///     - 0: New outer list
    static func computeLevelsRecursive<T>(
        nestedLists: Any,  // Type-erased nested structure
        currentDepth: Int,
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        context: inout LevelContext
    ) -> (values: [T], repLevels: [UInt16], defLevels: [UInt16])
}
```

**Algorithm**:
- Track current nesting depth
- For each element:
  - If it's another list, recurse with `depth + 1`
  - If it's a primitive, emit (value, repLevel, defLevel)
- Repetition level = depth at which we're continuing a list

#### 3.2: Type-Safe Multi-Level API

Challenge: Swift doesn't support arbitrary nesting generically. Options:

**Option A: Fixed Depth APIs**:
```swift
func writeDoubleLists(_ lists: [[[T]]]) throws
func writeTripleLists(_ lists: [[[[T]]]]) throws
```

**Option B: Protocol-Based Nesting** (Recommended):
```swift
protocol NestedListWritable {
    associatedtype Element
    var elements: [Element] { get }
}

extension Array: NestedListWritable where Element: NestedListWritable {
    var elements: [Element] { return self }
}
```

**Option C: Type-Erased `Any`**:
```swift
func writeNestedLists(_ lists: Any, depth: Int) throws
```

**Recommendation**: Start with Option A (fixed depth) for 2-3 levels, add Option C later if needed.

#### 3.3: Testing

**Multi-Level Tests**:
- `testDoubleLevelLists`: `[[[Int32]]]` → verify correct rep levels
- `testTripleLevelLists`: `[[[[String]]]]`
- `testMixedEmptyAndNull`: Empty and NULL lists at various depths

**Deliverable**: Can write multi-level lists up to 3-4 levels deep

---

### Phase 4: Structs (Week 3)

**Goal**: Write struct columns

#### 4.1: Struct Representation

```swift
// User provides struct data
struct Person {
    let name: String
    let age: Int32
}

let people: [Person?] = [
    Person(name: "Alice", age: 30),
    nil,
    Person(name: "Bob", age: 25)
]
```

#### 4.2: What W7 Delivers for Structs

**⚠️ CRITICAL: W7 does NOT ship a `StructColumnWriter` class.**

**What W7 Delivers**:
- Documentation of the manual field writing pattern
- Integration tests showing struct writing with existing column writers
- **Zero new writer code** - users directly invoke child column writers

**Why**: Swift lacks built-in struct reflection, making automatic field extraction complex. The manual API using existing column writers is simple, explicit, and sufficient for 1.0.

**Parity with Arrow C++**: Arrow C++ also delegates to child writers (`ArrayWriter::Child(i)`). We're not diverging—we're staying at the "write each field manually" level, which matches Arrow's low-level API.

#### 4.3: Manual Field Writing Pattern (Existing Capability)

Users write struct fields by calling child column writers directly:

```swift
// Given schema:
//   message User {
//     optional string name;
//     optional int32 age;
//   }

// User data
struct User {
    let name: String?
    let age: Int32?
}
let users = [
    User(name: "Alice", age: 30),
    User(name: nil, age: 25),     // NULL name
    User(name: "Bob", age: nil)   // NULL age
]

// Extract field arrays (user responsibility)
let names = users.map { $0.name }
let ages = users.map { $0.age }

// Write using existing column writers
let rowGroup = try writer.createRowGroup()
let nameWriter = try rowGroup.stringColumnWriter(at: 0)  // Column index for "name"
let ageWriter = try rowGroup.int32ColumnWriter(at: 1)    // Column index for "age"

try nameWriter.writeOptionalValues(names)
try ageWriter.writeOptionalValues(ages)

try rowGroup.close()
```

**Field Alignment Guarantee**: User ensures all field arrays have the same length (one entry per struct instance). Misaligned lengths will cause incorrect data or errors during file close.

**Nested Structs**: Same pattern applies - write each leaf field column independently.

#### 4.4: Testing (Documentation + Validation Only)

**W7 Tests**:
- `testManualStructWriting`: Document the pattern with integration test
- `testStructFieldAlignment`: Verify error/warning when field counts mismatch
- `testNestedStructWriting`: Document nested struct pattern
- Round-trip: Write struct fields manually → read with `StructValue` → verify

**No New Implementation**: These tests use existing column writers, validating the documented pattern.

**Deliverable**: Documentation + tests for manual struct writing (no new writer code)

**Post-1.0 Enhancement**: A `StructColumnWriter` with Codable-based automatic field extraction could be added as a convenience wrapper if users request it (tracked in roadmap).

---

### Phase 5: Maps (Week 3-4)

**Goal**: Write map columns

**Architecture**: Maps reuse list/struct machinery. A map is just a repeated group of key-value structs.

#### 5.1: Map Schema

Parquet maps are represented as repeated groups (same as Arrow C++):
```
map<K, V> =
  repeated group map {
    required K key;      // Keys are always required
    optional V value;    // Values may be NULL
  }
```

**Behavior**: `MapColumnWriter` delegates to the list level computation (for the repeated group) and primitive writers (for keys and values). No special map logic—just reuse existing components.

#### 5.2: MapColumnWriter

```swift
public final class MapColumnWriter<K, V> {
    private let keyWriter: ColumnWriter
    private let valueWriter: ColumnWriter

    public func writeMaps(_ maps: [[K: V]?]) throws {
        // 1. Flatten maps to key-value pairs
        var keys: [K] = []
        var values: [V?] = []
        var repLevels: [UInt16] = []
        var defLevels: [UInt16] = []

        for map in maps {
            guard let map = map else {
                // NULL map
                repLevels.append(0)
                defLevels.append(0)
                continue
            }

            if map.isEmpty {
                // Empty map
                repLevels.append(0)
                defLevels.append(1)
                continue
            }

            for (index, (key, value)) in map.enumerated() {
                keys.append(key)
                values.append(value)
                repLevels.append(index == 0 ? 0 : 1)
                defLevels.append(2)
            }
        }

        // 2. Write key and value columns with same rep/def levels
        try keyWriter.writeValuesWithLevels(keys, repLevels: repLevels, defLevels: defLevels)
        try valueWriter.writeValuesWithLevels(values, repLevels: repLevels, defLevels: defLevels)
    }
}
```

#### 5.3: Testing

- `testSimpleMap`: `[String: Int32]`
- `testEmptyMap`: Empty map vs NULL map
- `testNullableMapValues`: Map with NULL values
- Round-trip with reader's `MapValue`

**Deliverable**: Can write maps

---

## Testing Strategy

### Unit Tests

**Level Computation Tests** (`LevelComputerTests.swift`):
- Single-level list levels
- Multi-level list levels
- Empty list handling
- NULL list handling
- Mixed scenarios

**Writer Tests** (`NestedWriterTests.swift`):
- ListColumnWriter basic functionality
- Multi-level ListColumnWriter
- StructColumnWriter field writing
- MapColumnWriter key-value pairs

### Integration Tests

**Round-Trip Tests** (`NestedIntegrationTests.swift`):
- Write lists → read back → verify arrays match
- Write structs → read back → verify fields match
- Write maps → read back → verify key-value pairs match
- Test with complex nested combinations:
  - List of structs: `[{name: String, scores: [Int32]}]`
  - Struct with list field
  - Map with list values

**Compatibility Tests**:
- Write nested files → read with PyArrow → verify correctness
- Compare level encoding with PyArrow-generated files

### Test Data Sources

Reuse existing reader fixtures:
- `Tests/ParquetTests/Fixtures/nested_lists.parquet`
- `Tests/ParquetTests/Fixtures/nested_structs.parquet`
- `Tests/ParquetTests/Fixtures/nested_maps.parquet`

Generate reference data:
```python
# PyArrow script to generate test files
import pyarrow as pa
import pyarrow.parquet as pq

# List example
data = {
    'lists': [
        [1, 2, 3],
        [],
        [4, 5]
    ]
}
table = pa.table(data)
pq.write_table(table, 'single_level_lists.parquet')
```

Compare our writer output byte-for-byte with PyArrow.

---

## Performance Considerations

### Level Computation Overhead

**Challenge**: Computing levels requires traversing entire nested structure

**Optimization**:
- Compute levels once, cache results
- Use stack-based iteration instead of recursion for deep nesting
- Pre-allocate level arrays to avoid reallocation

### Memory Usage

**Challenge**: Flattening nested structures duplicates data in memory

**Mitigation**:
- Process in batches (e.g., 1000 top-level elements at a time)
- Stream level computation for very large datasets

### Buffer Management

Levels and values must be buffered until page is written:
- Track buffer size
- Flush to page when buffer reaches threshold
- Coordinate rep/def/value buffers

---

## Error Handling

### Schema Validation

Validate nested structure matches schema:
- Depth matches max repetition level
- Field types match declared types
- Required fields are present

### Level Validation

Sanity checks on computed levels:
- All rep levels ≤ maxRepetitionLevel
- All def levels ≤ maxDefinitionLevel
- Level arrays same length as value array (accounting for NULLs)

### Type Mismatches

```swift
enum WriterError: Error {
    case nestedStructureMismatch(expected: String, actual: String)
    case invalidNestingDepth(max: Int, actual: Int)
    case missingRequiredField(fieldName: String)
}
```

---

## Dependencies

### Internal (Existing)

- ✅ RLE encoder (W4) - for level encoding
- ✅ Definition level computation (W5) - extend for nested
- ✅ ColumnWriter primitives (W1-W5)
- ✅ Schema representation with repetition types

### Reader Components (Reference)

- `ArrayReconstructor` - inverse algorithm reference
- `StructValue` - struct reading reference
- `MapValue` - map reading reference
- Nested type tests - validation reference

### New Components

- `LevelComputer` - rep/def level computation
- `ListColumnWriter` - list writing
- `StructColumnWriter` - struct writing
- `MapColumnWriter` - map writing

---

## Risks & Mitigation

### Risk 1: Level Computation Bugs (HIGH)

**Impact**: Incorrect levels → unreadable files or data corruption

**Mitigation**:
- Extensive unit tests with known level sequences
- Cross-validate against reader's ArrayReconstructor
- Round-trip tests: write → read → verify
- Fuzzing with random nested structures

### Risk 2: Multi-Level List Complexity (HIGH)

**Impact**: Algorithm errors for deep nesting

**Mitigation**:
- Start with single-level, validate thoroughly
- Add one level at a time
- Test each depth independently
- Reference PyArrow for edge cases

### Risk 3: Struct Field Extraction (MEDIUM)

**Impact**: Difficult API for users

**Mitigation**:
- Start with manual field writing (simple, explicit)
- Defer automatic extraction to post-1.0
- Document clear examples

### Risk 4: Map Ordering (LOW)

**Impact**: Swift Dictionary order is undefined

**Mitigation**:
- Document that map order may not be preserved
- Consider OrderedDictionary option
- Match reader behavior

---

## Success Criteria

### W7 Complete When:

**New Code Delivered**:
- ✅ `LevelComputer` utility for rep/def level computation
- ✅ `ListColumnWriter<T>` for single-level lists: `[[T]]`
- ✅ Multi-level list support: `[[[T]]]`, `[[[[T]]]]`
- ✅ `MapColumnWriter<K, V>` for maps

**Documentation/Tests (No New Writer Code)**:
- ✅ **Structs: Documented manual field pattern only** (no `StructColumnWriter` class)
- ✅ Integration tests validating struct pattern with existing column writers

**Correctness**:
- ✅ Empty/NULL lists correctly produce level entries without value entries
- ✅ All round-trip tests pass (write → read → verify)
- ✅ Files readable by PyArrow
- ✅ Level encoding matches PyArrow byte-for-byte (validated with fixtures)
- ✅ No regressions in existing tests (388+ tests still pass)

---

## Timeline

**Week 1** (Days 1-5):
- Phase 1: Repetition level encoding (reuse W4 RLE encoder)
- Phase 2: Single-level list implementation (`LevelComputer` + `ListColumnWriter`)
- Testing: Single-level lists (unit + integration)

**Week 2** (Days 6-10):
- Phase 3: Multi-level lists (recursive level computation, 2-4 levels deep)
- Testing: Multi-level list round-trips

**Week 3** (Days 11-15):
- Phase 4: Struct documentation (integration tests showing manual field pattern)
- Phase 5: Map writing (`MapColumnWriter` implementation)
- Testing: Map round-trips + struct pattern validation

**Week 4** (Days 16-20, buffer):
- Phase 5: Map writing (finish)
- Integration testing: Complex nested scenarios
- PyArrow compatibility validation
- Bug fixes and polish

**Total**: 3-4 weeks (with 1 week buffer for complexity)

---

## Post-W7 (Future Work)

**Deferred to Post-1.0**:
- Automatic struct field extraction (Codable integration)
- Lists of structs builder API
- Large lists (Int64 offsets)
- Performance optimizations (SIMD, parallel level computation)
- Nested bloom filters
- Nested statistics

---

## Next Steps

**Immediate**:
1. Create `LevelComputer.swift` utility
2. Extend RLE encoder for repetition levels
3. Implement single-level list algorithm
4. Create `ListColumnWriter<T>`
5. Write first integration test: `[[Int32]]`

**Ready to start implementation?**
