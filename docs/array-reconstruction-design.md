# Array Reconstruction Design

## Overview

This document describes the design for implementing array reconstruction using repetition levels in parquet-swift.

## Background

We've completed the infrastructure for decoding repetition levels from Parquet pages. Now we need to use those levels to reconstruct arrays from flat value sequences.

### Current State
- ✅ Repetition levels are decoded from pages
- ✅ Stored in `currentRepetitionLevels: [UInt16]?`
- ✅ All 5 column readers (Int32, Int64, Float, Double, String) support this
- ❌ Arrays are not reconstructed - values returned as flat sequences

### Goal
- Use repetition levels to group flat values into nested arrays
- Support columns with `maxRepetitionLevel = 1` (simple repeated fields)
- Return `[[T?]]` instead of `[T?]` for repeated columns

## Phase 1 Scope

**Support**: Simple repeated fields with `maxRepetitionLevel = 1`

Example schemas:
```
message Test {
  repeated group items {
    required int32 id;        // maxRepetitionLevel = 1
    optional string name;      // maxRepetitionLevel = 1
  }
}
```

**Defer**: Multi-level repetition (lists of lists, nested repeated groups)

## Key Concepts

### Repetition Levels
- **rep_level = 0**: Start of new list/record
- **rep_level = maxRepetitionLevel**: Continuation of current list
- **rep_level > maxRepetitionLevel**: Filtered out (deeper nesting)

### Definition Levels with Repeated Fields
- **def_level < repeated_ancestor_def_level**: Skip (ancestor list is null/empty)
- **def_level >= maxDefinitionLevel**: Value is present (non-null element)
- **def_level < maxDefinitionLevel**: Value is null (within non-empty list)

### repeated_ancestor_def_level

The minimum definition level indicating the closest repeated ancestor is non-empty.

**Computation from Schema**:
1. Walk the schema path from root → leaf
2. Accumulate definition level contributions (+1 for optional/repeated)
3. When encountering the first `.repeated` node, that accumulated def level is the `repeatedAncestorDefLevel`

**Examples**:
```
repeated int32 numbers;
  → maxDefinitionLevel = 1, repeatedAncestorDefLevel = 1

repeated group items {
  optional int32 value;
}
  → maxDefinitionLevel = 2 (repeated +1, optional +1)
  → repeatedAncestorDefLevel = 1 (the repeated group's def level)

optional repeated group items {
  required int32 id;
}
  → maxDefinitionLevel = 2 (optional +1, repeated +1)
  → repeatedAncestorDefLevel = 2 (the repeated group's def level)
```

This will be exposed as a computed property on `Column`:
```swift
extension Column {
    public var repeatedAncestorDefLevel: Int?  // nil if not repeated
}
```

## Algorithm

Based on Apache Arrow C++ `DefRepLevelsToListInfo`:

```swift
func reconstructArrays<T>(
    values: [T],               // Non-null payloads only (no Optional wrapper)
    definitionLevels: [UInt16], // One per logical value (including nulls)
    repetitionLevels: [UInt16], // One per logical value (including nulls)
    maxDefinitionLevel: Int,
    maxRepetitionLevel: Int,
    repeatedAncestorDefLevel: Int
) throws -> [[T?]] {
    var result: [[T?]] = []
    var currentList: [T?] = []
    var valueIndex = 0

    for i in 0..<definitionLevels.count {
        let defLevel = definitionLevels[i]
        let repLevel = repetitionLevels[i]

        // Skip values that belong to empty/null ancestor lists
        if defLevel < repeatedAncestorDefLevel {
            continue
        }

        // Invalid: rep level should never exceed max for this column
        if repLevel > maxRepetitionLevel {
            throw ColumnReaderError.internalError(
                "Repetition level \(repLevel) exceeds column max \(maxRepetitionLevel)"
            )
        }

        if repLevel == maxRepetitionLevel {
            // Continuation of current list
            if defLevel >= maxDefinitionLevel {
                // Non-null element
                currentList.append(values[valueIndex])
                valueIndex += 1
            } else {
                // Null element (def_level between ancestor and max)
                currentList.append(nil)
            }
        } else {
            // Start of new list (repLevel < maxRepetitionLevel)
            // Finish previous list
            if i > 0 {
                result.append(currentList)
            }

            // Start new list
            currentList = []

            // Add first element if present
            if defLevel >= maxDefinitionLevel {
                currentList.append(values[valueIndex])
                valueIndex += 1
            } else if defLevel >= repeatedAncestorDefLevel {
                // List is non-empty but first element is null
                currentList.append(nil)
            }
            // else: empty list (defLevel == repeatedAncestorDefLevel - 1)
        }
    }

    // Add final list
    if !currentList.isEmpty || definitionLevels.count > 0 {
        result.append(currentList)
    }

    return result
}
```

### Example Walkthrough

**Data**: `[[1, 2], [], [3]]`

```
rep_levels: [0, 1, 0, 0]  // 0=new list
def_levels: [1, 1, 0, 1]  // 0=empty list, 1=present value
values: [1, 2, 3]
maxDefinitionLevel = 1
repeatedAncestorDefLevel = 1
```

Process:
- i=0: rep=0 (new list), def=1 (present) → start list, add values[0]=1
- i=1: rep=1 (continue), def=1 (present) → add values[1]=2
- i=2: rep=0 (new list), def=0 (empty list) → append [1,2], start [], skip value
- i=3: rep=0 (new list), def=1 (present) → append [], start list, add values[2]=3
- End: append [3]

Result: `[[1, 2], [], [3]]` ✅

## API Design

### Option 1: Separate Methods (Recommended)

```swift
// Existing methods for flat columns (maxRepetitionLevel = 0)
public func readAll() throws -> [Int32?]
public func readBatch(count: Int) throws -> [Int32?]
public func readOne() throws -> Int32??

// New methods for repeated columns (maxRepetitionLevel > 0)
public func readAllRepeated() throws -> [[Int32?]]
public func readBatchRepeated(count: Int) throws -> [[Int32?]]
// Note: readOne() doesn't make sense for repeated columns
```

**Pros**:
- No breaking changes
- Clear separation of concerns
- User can check `maxRepetitionLevel` and call appropriate method

**Cons**:
- More methods to maintain
- User must know which method to call

### Validation

```swift
public func readAllRepeated() throws -> [[Int32?]] {
    guard maxRepetitionLevel > 0 else {
        throw ColumnReaderError.unsupportedFeature(
            "Column is not repeated (maxRepetitionLevel = 0). Use readAll() instead."
        )
    }
    // ... implementation
}

public func readAll() throws -> [Int32?] {
    guard maxRepetitionLevel == 0 else {
        throw ColumnReaderError.unsupportedFeature(
            "Column is repeated (maxRepetitionLevel > 0). Use readAllRepeated() instead."
        )
    }
    // ... implementation
}
```

## Implementation Plan

### Step 1: Add ArrayReconstructor Helper

Create `Sources/Parquet/Reader/ArrayReconstructor.swift`:

```swift
/// Helper for reconstructing arrays from flat values and def/rep levels
struct ArrayReconstructor {
    static func reconstructArrays<T>(
        values: [T?],
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int
    ) throws -> [[T?]]

    /// Calculate repeated_ancestor_def_level from schema
    static func repeatedAncestorDefLevel(
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int
    ) -> Int
}
```

### Step 2: Update Int32ColumnReader

Add `readAllRepeated()` method:

```swift
public func readAllRepeated() throws -> [[Int32?]] {
    guard maxRepetitionLevel > 0 else {
        throw ColumnReaderError.unsupportedFeature(
            "Column is not repeated (maxRepetitionLevel = 0). Use readAll() instead."
        )
    }

    // Read all flat values and levels
    var allValues: [Int32?] = []
    var allDefLevels: [UInt16] = []
    var allRepLevels: [UInt16] = []

    while let value = try readOne() {
        allValues.append(value)
        // Collect def/rep levels from current page
        // (need to track which levels correspond to which values)
    }

    // Reconstruct arrays
    return try ArrayReconstructor.reconstructArrays(
        values: allValues,
        definitionLevels: allDefLevels,
        repetitionLevels: allRepLevels,
        maxDefinitionLevel: maxDefinitionLevel,
        maxRepetitionLevel: maxRepetitionLevel
    )
}
```

### Step 3: Update Other Column Readers

Apply same pattern to Int64, Float, Double, String readers.

### Step 4: Add Tests

Create test files with repeated columns:
- Simple repeated int32
- Repeated with nulls
- Empty lists
- Mix of empty and non-empty lists

### Step 5: Update Documentation

- Update README.md: Change 🚧 to ✅ for repeated columns
- Update docs/limitations.md: Document full support
- Update column reader docstrings

## Testing Strategy

### Test Cases

1. **Simple repeated column**: `[[1, 2], [3]]`
2. **Repeated with nulls**: `[[Some(1), None], [Some(2)]]`
3. **Empty lists**: `[[1], [], [2]]`
4. **All empty**: `[[], [], []]`
5. **Single large list**: `[[1, 2, 3, ..., 100]]`

### Test Files

Need to generate Parquet files with repeated columns using PyArrow:

```python
import pyarrow as pa
import pyarrow.parquet as pq

# Simple repeated int32
schema = pa.schema([
    pa.field('numbers', pa.list_(pa.int32()))
])

data = [[1, 2], [3], [], [4, 5, 6]]
table = pa.table({'numbers': data}, schema=schema)
pq.write_table(table, 'repeated_int32.parquet')
```

## Implementation Details

### 1. repeatedAncestorDefLevel Computation

Implemented as a computed property on `Column`:

```swift
extension Column {
    /// Definition level at which the nearest repeated ancestor is "present but empty".
    /// Returns nil if the column is not repeated (`maxRepetitionLevel == 0`).
    public var repeatedAncestorDefLevel: Int? {
        guard maxRepetitionLevel > 0 else { return nil }

        // Build the path from root → leaf
        var nodes: [SchemaElement] = []
        var current: SchemaElement? = element
        while let node = current {
            nodes.append(node)
            current = node.parent
        }
        nodes.reverse()  // root first, leaf last
        guard nodes.count >= 2 else { return nil }

        var defLevel = 0

        // Walk the path (skip root at index 0)
        for node in nodes[1...] {
            guard let repetition = node.repetitionType else { continue }

            switch repetition {
            case .repeated:
                defLevel += 1  // list itself contributes 1 def level
                return defLevel  // this list's def level is what we need
            case .optional:
                defLevel += 1
            case .required:
                break
            }
        }

        return nil  // No repeated ancestor found
    }
}
```

This matches Arrow's `definition_level_of_list` logic.

### 2. Values Array Alignment

**Critical**: The `values` parameter contains **only non-null payloads**:
- No `Optional` wrapper on the array type
- `values.count <= definitionLevels.count` (nulls are implicit in levels)
- Increment `valueIndex` **only** when `defLevel >= maxDefinitionLevel`

This matches how the existing column readers work:
```swift
private var nonNullValuesRead: Int = 0  // offset into decoded data stream
```

## Open Questions

1. ~~**repeated_ancestor_def_level calculation**~~ ✅ Resolved: Use computed property on Column

2. **readBatchRepeated()**: How to handle partial arrays across batch boundaries?
   - Option A: Only return complete arrays in batch
   - Option B: Buffer partial array across batches
   - **Recommendation**: Option A for simplicity

3. **Null lists vs empty lists**: How to represent?
   - `nil` = null list
   - `[]` = empty list
   - Return type: `[[T?]?]` (outer optional for null lists)
   - For Phase 1: Assume lists are always present (ignore list nullability)

## References

- Apache Arrow C++: `level_conversion.h` / `level_conversion.cc`
- Parquet Format Spec: [Nested Encoding](https://github.com/apache/parquet-format/blob/master/Encodings.md#nested-encoding)
- Our implementation: `Sources/Parquet/Reader/Int32ColumnReader.swift:264-304`
