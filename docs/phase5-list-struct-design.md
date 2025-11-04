# Phase 5 Design: Lists of Structs with Complex Children

**Date**: 2025-11-04
**Status**: Design Phase
**Goal**: Support `list<struct { map/list }>` - lists of structs containing maps or lists

---

## Problem Statement

### Current Limitation

Phase 4.5 completed support for root-level structs with complex children:
```swift
// ✅ WORKS in Phase 4.5
struct { int32 id; map<string, int64> attrs; }
let structs = try rowGroup.readStruct(at: ["user"])
let attrs = structs[0]?.get("attrs", as: [AnyHashable: Any?].self)
```

But **lists of such structs** are not yet supported:
```swift
// ❌ FAILS with error in Phase 4.5
list<struct { int32 id; map<string, int64> attrs; }>
let lists = try rowGroup.readRepeatedStruct(at: ["users", "list", "element"])
// Error: "Structs in lists containing repeated or map/list fields are not yet supported"
```

### Why It's Hard

The current `readRepeatedStruct()` implementation (RowGroupReader+Struct.swift:1088-1096):
- Only reads PRIMITIVE fields via `readFieldColumnsWithLevels()`
- For complex children (maps/lists), it only captures the FIRST entry
- Multi-entry structures are INCOMPLETE

**Example bug:**
```
Struct with map field {a:1, b:2, c:3}
Current behavior: Only {key:"a", value:1} appears in StructValue
Expected: Full map {a:1, b:2, c:3} should be accessible
```

---

## Arrow C++ Pattern Analysis

### How Arrow C++ Handles list<struct>

From `cpp-levelinfo-analysis.md` lines 356-378:

1. **Outer ListReader** (for the list dimension):
   - Processes with `rep_level = 1` (list's repetition level)
   - Skips `rep_levels > 1` (those belong to nested children)
   - Only processes `rep_level ∈ {0, 1}`

2. **StructReader** (for each struct in the list):
   - Uses DefRepLevelsToBitmap to compute struct validity
   - Tells each child to `BuildArray(values_read)`
   - Children properly reconstruct their values

3. **Inner readers** (for map/list children within struct):
   - Process their own repetition levels
   - Filter condition: `if (rep_levels[x] > level_info.rep_level) continue;`
   - Prevents double-counting

**Key insight**: Each level of hierarchy only processes entries at its own level or higher.

---

## Proposed Solution

### Approach: Hybrid Reconstruction

Combine Phase 4.5's struct-with-complex-children logic with existing list reconstruction:

1. **List reconstruction** (keep existing `reconstructRepeatedStructs()`):
   - Group struct instances by repetition level
   - Determine list boundaries (rep_level = 0 for new row)

2. **Struct reconstruction** (extend `reconstructStructValueAt()`):
   - For each struct position, compute its "slice range" in flattened data
   - Use Phase 4.5 child readers to read complex children
   - Pass the correct range/bounds to child readers

3. **Child reconstruction** (reuse Phase 4.5 readers):
   - `readMapChild()` - already handles maps with truncation
   - `readListChild()` - already handles lists with truncation
   - But need to pass struct-specific bounds, not values_read

### Challenge: Determining Struct Boundaries

For each struct in the list, we need to know:
- **Start index**: Where does this struct's data begin in the flattened column?
- **End index**: Where does it end?

This requires scanning levels to find:
- `rep_level == listRepLevel + 1` → continuation of current struct (more map entries)
- `rep_level == listRepLevel` or `0` → new struct or new row

---

## Implementation Plan

### Option A: Per-Struct Reconstruction (Recommended)

**Idea**: For each struct instance, slice the levels and reconstruct its complex children

```swift
func readRepeatedStructWithComplexChildren(
    at path: [String],
    element: SchemaElement
) throws -> [[StructValue?]?] {
    // 1. Read levels from representative column
    let (defLevels, repLevels) = try readRepresentativeColumnLevels(...)

    // 2. Group struct instances by list (existing logic)
    let structRanges = computeStructRanges(
        repLevels: repLevels,
        listRepLevel: listRepLevel
    )
    // Returns: [[(startIdx, endIdx)]] - for each row, ranges of each struct

    // 3. For each row:
    var result: [[StructValue?]?] = []
    for rowStructRanges in structRanges {
        var structs: [StructValue?] = []

        // 4. For each struct in this row:
        for (startIdx, endIdx) in rowStructRanges {
            // 5. Compute struct validity for this range
            let structDefLevels = Array(defLevels[startIdx..<endIdx])
            let structRepLevels = Array(repLevels[startIdx..<endIdx])

            var validityOutput = ValidityBitmapOutput()
            try defRepLevelsToBitmap(
                definitionLevels: structDefLevels,
                repetitionLevels: structRepLevels,
                levelInfo: structLevelInfo,
                output: &validityOutput
            )

            if !validityOutput.validBits[0] {
                structs.append(nil)  // NULL struct
                continue
            }

            // 6. Read complex children for this struct's range
            var fieldData: [String: Any?] = [:]
            for child in element.children {
                if child.isMap {
                    let mapValues = try readMapChildInRange(
                        child,
                        range: startIdx..<endIdx
                    )
                    fieldData[child.name] = mapValues
                } else if child.isList {
                    // Similar for lists
                } else {
                    // Scalar fields
                }
            }

            structs.append(StructValue(element: element, fieldData: fieldData))
        }

        result.append(structs)
    }

    return result
}
```

**Pros**:
- ✅ Clear separation of concerns
- ✅ Reuses Phase 4.5 child readers
- ✅ Easy to reason about

**Cons**:
- ⚠️ Requires slicing levels multiple times
- ⚠️ Need to implement range-based child readers

### Option B: Read All Children Upfront

**Idea**: Read all complex children as full arrays, then slice by struct ranges

```swift
func readRepeatedStructWithComplexChildren(...) -> [[StructValue?]?] {
    // 1. Read ALL complex children (full row group)
    var allMapChildren: [String: [[MapEntry]?]] = [:]
    for child in element.children where child.isMap {
        allMapChildren[child.name] = try readMap(at: childPath)
    }

    // 2. Compute struct ranges in the flattened data
    let structRanges = computeStructRanges(...)

    // 3. For each struct, slice the appropriate range from allMapChildren
    // ...slice based on structRanges...
}
```

**Pros**:
- ✅ Simpler implementation
- ✅ Reuses existing readers directly

**Cons**:
- ❌ Memory inefficient (reads entire row group even if only need a few rows)
- ❌ Hard to map flattened column data to struct instances

---

## Recommended Implementation: Option A

**Rationale**:
- Matches Arrow C++ pattern more closely
- More memory efficient
- Cleaner separation of concerns
- Can be extended to deeper nesting later

**Key Functions to Implement**:

1. **computeStructRanges()**: Scan rep levels to find struct boundaries
   ```swift
   // Returns array of struct ranges per row
   // [[StructValue?]?] → [[(start: Int, end: Int)?]?]
   func computeStructRanges(
       repLevels: [UInt16],
       defLevels: [UInt16],
       listRepLevel: Int,
       repeatedAncestorDefLevel: Int
   ) -> [[(start: Int, end: Int)?]?]
   ```

2. **readMapChildInRange()**: Read map for specific level range
   ```swift
   func readMapChildInRange(
       _ child: SchemaElement,
       range: Range<Int>
   ) throws -> [[MapEntry]?]
   ```

3. **readListChildInRange()**: Read list for specific level range

4. **readScalarFieldInRange()**: Read scalar for specific range

---

## Edge Cases to Handle

1. **NULL struct in list**: `defLevel <= repeatedAncestorDefLevel`
2. **Empty map/list within struct**: Child has no entries
3. **NULL map/list within struct**: Child is explicitly NULL
4. **Nested NULL handling**: List NULL vs struct NULL vs field NULL

---

## Test Cases

### Simple Cases
1. `list<struct { map<string, int> attrs }>`
   - List with 1 struct with 2 map entries
   - List with 2 structs, each with different maps
   - Empty list
   - NULL list
   - List with NULL struct

2. `list<struct { list<int> values }>`
   - Similar test patterns

### Complex Cases
3. Multiple complex fields:
   ```
   list<struct {
       map<string, int> attrs;
       list<string> tags;
   }>
   ```

4. Nested structures:
   ```
   list<struct {
       struct inner { map<string, int> data; }
   }>
   ```

---

## Success Criteria

- ✅ Can read `list<struct { map<K,V> }>`
- ✅ Can read `list<struct { list<T> }>`
- ✅ Map/list children fully reconstructed (not truncated)
- ✅ Correct NULL handling at all levels
- ✅ All 352+ existing tests still pass
- ✅ New comprehensive test suite for Phase 5

---

## Timeline Estimate

- **Design & Planning**: 0.5 days ✅ (this document)
- **computeStructRanges() implementation**: 1 day
- **Range-based child readers**: 1-2 days
- **Integration & refactoring**: 1 day
- **Test fixture creation**: 0.5 days
- **Test implementation**: 1 day
- **Bug fixes & polish**: 1-2 days

**Total**: 6-8 days (~2-3 weeks part-time)

---

## Next Steps

1. ✅ Complete design (this document)
2. Implement `computeStructRanges()` - find struct boundaries in rep levels
3. Implement range-based child readers
4. Integrate with existing `readRepeatedStruct()`
5. Create test fixtures
6. Write comprehensive tests
7. Update documentation
