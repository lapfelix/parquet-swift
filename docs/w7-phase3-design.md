# W7 Phase 3: Multi-Level List Support (Design)

## Overview

Phase 3 extends list writing to support nested lists (lists of lists) with `maxRepetitionLevel > 1`.

**Examples:**
- 2-level: `[[[Int32]?]?]` (list of optional lists of optional Int32)
- 3-level: `[[[[Int32]?]?]?]` (list of optional lists of optional lists...)
- 4-level: And so on...

## Current Status (Phase 2)

✅ **Completed:**
- Single-level lists (`[[Int32]?]`) with maxRepetitionLevel = 1
- LevelComputer handles 1-level flattening
- ListColumnWriter (Int32, Int64, String) for single-level lists
- 7 integration tests + 2 real schema tests
- 410 total tests passing

## Phase 3 Requirements

### 1. Extend LevelComputer for Multi-Level Lists

**Current:**
```swift
static func computeLevelsForList<T>(
    lists: [[T]?],  // Single-level only
    maxDefinitionLevel: Int,
    maxRepetitionLevel: Int,
    repeatedAncestorDefLevel: Int,
    nullListDefLevel: Int
) -> (values: [T], definitionLevels: [UInt16], repetitionLevels: [UInt16])
```

**New Method Needed:**
```swift
static func computeLevelsForNestedList(
    lists: Any,  // Type-erased nested arrays [[[T]?]?] or deeper
    maxDefinitionLevel: Int,
    maxRepetitionLevel: Int,
    repeatedAncestorDefLevels: [Int],  // Array for each nesting level
    nullListDefLevels: [Int]           // Nullable thresholds for each level
) -> (values: [Any], definitionLevels: [UInt16], repetitionLevels: [UInt16])
```

**Algorithm (Inverse of ArrayReconstructor.reconstructNestedArrays):**

1. Use a stack to traverse nested arrays depth-first
2. For each element:
   - Determine its nesting level
   - Emit appropriate repetition level:
     - `rep = 0` for new top-level list
     - `rep = 1` for continuation in outer list
     - `rep = 2` for continuation in 2nd-level list
     - etc.
   - Emit appropriate definition level based on NULL/empty/present state at each level
3. Collect leaf values in flattened order

**Example (2-level list):**
```swift
Input: [[[1, 2], [3]], [[4]]]  // 2 outer lists, first has 2 inner lists, second has 1

// Traversal:
// [0][0][0] = 1  → rep=0, def=maxDef (new outer list, new inner list, value)
// [0][0][1] = 2  → rep=2, def=maxDef (continue inner list, value)
// [0][1][0] = 3  → rep=1, def=maxDef (continue outer list, new inner list, value)
// [1][0][0] = 4  → rep=0, def=maxDef (new outer list, new inner list, value)

values = [1, 2, 3, 4]
repLevels = [0, 2, 1, 0]
defLevels = [maxDef, maxDef, maxDef, maxDef]  // All non-null
```

### 2. Schema Support for Multi-Level Lists

**Schema Structure for 2-level list:**
```
optional group outer_list (LIST)          ← maxRep=0, maxDef=1
  repeated group list                     ← maxRep=1, maxDef=1
    optional group element (LIST)         ← maxRep=1, maxDef=2
      repeated group list                 ← maxRep=2, maxDef=2
        optional int32 element            ← maxRep=2, maxDef=4 (physical column)
```

**repeatedAncestorDefLevels:**
- Index 0 (outermost): defLevel when outer list is present but empty = 1
- Index 1 (inner): defLevel when inner list is present but empty = 3

**nullListDefLevels:**
- Index 0: defLevel when outer list is NULL = 0
- Index 1: defLevel when inner list is NULL = 2

### 3. Writer API

**Option A: Separate Multi-Level Writer**
```swift
class Int32NestedListColumnWriter {
    func writeValues(_ lists: Any, maxRepetitionLevel: Int) throws
}
```

**Option B: Extend Existing Writer** (PREFERRED)
```swift
extension Int32ListColumnWriter {
    /// Write nested lists (maxRepetitionLevel > 1)
    func writeNestedValues(
        _ lists: Any,  // Type-erased nested arrays
        repeatedAncestorDefLevels: [Int],
        nullListDefLevels: [Int]
    ) throws -> Int
}
```

### 4. Test Coverage

**Unit Tests (LevelComputer):**
- 2-level lists: `[[[1,2],[3]], [[4]]]`
- 2-level with NULLs: `[[[1]], nil, [nil], [[]]]`
- 3-level lists: `[[[[1,2]]], [[[3]]]]`
- Empty and NULL at various levels

**Integration Tests:**
- Write and read 2-level Int32 lists
- Write and read 2-level String lists
- Write and read 3-level lists
- Mixed NULL/empty/present at multiple levels

## Implementation Phases

### Phase 3.1: Core Algorithm (LevelComputer)
- Implement `computeLevelsForNestedList`
- Add unit tests (10-15 tests)
- Validate against reader's ArrayReconstructor

### Phase 3.2: Writer Integration
- Extend ListColumnWriter with `writeNestedValues`
- Add to RowGroupWriter factory methods
- Schema validation for multi-level lists

### Phase 3.3: Integration Tests
- Write→read→verify for 2-level, 3-level, 4-level
- Real schema tests (parsed from Parquet files)
- Edge cases (all NULL, all empty, deep nesting)

## Estimated Complexity

**Lines of Code:**
- LevelComputer extension: ~200 lines
- ListColumnWriter extension: ~100 lines
- Tests: ~400 lines
- **Total: ~700 lines**

**Time Estimate:** 4-6 hours

## Success Criteria

✅ LevelComputer correctly flattens 2-level, 3-level, 4-level lists
✅ ListColumnWriter accepts nested arrays via `writeNestedValues()`
✅ Data round-trips correctly through write→read→verify
✅ Works with real Parquet schemas (not just hand-built)
✅ All tests pass (target: 420+ tests)

## Deferred to Future Phases

- Lists of structs (Phase 4)
- Maps (Phase 5)
- Validation with PyArrow (W7 Final)
