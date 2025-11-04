# Arrow C++ LevelInfo Implementation - Complete Guide

**Date**: 2025-11-03
**Purpose**: Research how Apache Arrow C++ handles complex nested types (structs with maps/lists, map<k, list<v>>, list<map>, multi-level repetition)

---

## Quick Reference

> **tl;dr**: For experienced developers familiar with the concepts. See detailed sections below for implementation guidance.

### LevelInfo Metadata
- Defined in `third_party/arrow/cpp/src/parquet/level_conversion.h`.
- Captures per-node thresholds:
  - `def_level`: def-level at which value is present.
  - `rep_level`: repetition level aligned with closest repeated ancestor.
  - `repeated_ancestor_def_level`: def-level that distinguishes "ancestor list empty" vs "child exists".
  - `null_slot_usage`: slots consumed by nulls (needed for fixed-size lists).
- `LevelInfo::IncrementOptional()` / `IncrementRepeated()` update the above when descending schema nodes.
- `LevelInfo::ComputeLevelInfo(ColumnDescriptor*)` walks from a leaf node back to root to fill in values for a column.

### Def/Rep Level Helpers
- Implemented in `level_conversion.cc`.
- `DefRepLevelsToListInfo` is the core ~80-line template that reconstructs list offsets + validity bits from parallel def/rep arrays using a `LevelInfo`.
  - Skips rows when `def < repeated_ancestor_def_level` (ancestor null) or `rep > level_info.rep_level` (belongs to deeper nesting).
  - `rep == level_info.rep_level` ⇒ continuation of current list (increment offset only).
  - `rep < level_info.rep_level` ⇒ start of new list; increments cumulative offsets and writes validity bit based on `def >= level_info.def_level - 1`.
  - Throws on overflow or if `values_read` exceeds `values_read_upper_bound`.
- Wrappers:
  - `DefRepLevelsToList` (int32/int64 offsets) for Arrow `ListArray` / map reconstruction.
  - `DefRepLevelsToBitmap` bumps def/rep levels and reuses ListInfo with null offsets to get a validity bitmap for structs that have repeated children.
  - `DefLevelsToBitmap` handles the simple case with no repeated ancestor.

### How Readers Use It
- **ListReader**: Pulls def/rep streams from child column reader, allocates validity + offsets buffers sized to `length_upper_bound`, calls `DefRepLevelsToList`, then asks child reader to materialize flattened values. Works for both Arrow List and Map (since Map is just `list<struct<key,value>>`).
- **StructReader**: If the struct (or any child) is repeated, it gathers def/rep levels from the first child and runs `DefRepLevelsToBitmap` to get the struct validity bitmap. Otherwise it only needs def-levels and calls `DefLevelsToBitmap`. Children are then told to `BuildArray(validity_io.values_read)` so all arrays align.

### Schema Traversal
- `parquet/arrow/schema.cc` threads a `LevelInfo` down the tree inside `GroupToSchemaField` / `NodeToSchemaField`.
- Each recursion clones the current LevelInfo, calls `IncrementOptional` / `IncrementRepeated` as needed, and attaches the updated struct to each leaf, so every ColumnDescriptor already knows its LevelInfo without ad-hoc inspection.

### What Swift Needs (Summary)
1. **Level metadata:** either port `LevelInfo` or compute equivalent per schema node (`def_level`, `rep_level`, `repeated_ancestor_def_level`).
2. **DefRepLevelsToListInfo:** translate the helper (plus wrappers) into Swift to rebuild list offsets/validity and struct bitmaps.
3. **Reader wiring:** Struct reader should emulate C++ `StructReader` (single child supplies def/rep, use bitmap helper, children build arrays to the same logical length). `readMap` should treat maps as `list<struct<key,value>>` and delegate to the list helper.
4. **Schema traversal:** extend current Schema/Column types so each node exposes the same thresholds (instead of brittle heuristics).

**Time**: 12-18 hours of focused work (see detailed phase breakdown in Section 8).

---

## Detailed Analysis

### Overview

The Arrow C++ implementation uses a sophisticated **LevelInfo** system combined with specialized algorithms like **DefRepLevelsToListInfo** to correctly reconstruct nested Parquet structures. This document analyzes their approach to inform the Swift implementation.

---

## 1. The Problem Being Solved

### Multi-Level Repetition Challenge

Parquet uses definition and repetition levels to encode nested structures:

```
Schema: list<map<string, int64>>

Definition levels:
  0 = NULL outer list
  1 = present but empty outer list
  2 = present list with map present (map may be empty)
  3 = map present with key-value pair present
  4 = value within map present (not NULL)

Repetition levels:
  0 = new row (new outer list)
  1 = new list element (new map in same list)
  2 = continuation of map (new key-value pair in same map)
```

**The challenge**: Given a flat stream of values with these levels, reconstruct the proper nested structure:
- `repLevel = 0` → start new row
- `repLevel = 1` → start new map **in the same list**
- `repLevel = 2` → add entry **to the current map**

**Current Swift bug**: Treats `repLevel < maxRepLevel` as "start new list", so `repLevel=1` incorrectly starts a new row instead of a new map in the same row.

---

## 2. C++ Data Structures

### LevelInfo Structure

Located in: `third_party/arrow/cpp/src/parquet/level_conversion.h:28-154`

```cpp
struct LevelInfo {
  // How many slots a null element consumes (>1 for FixedSizeList)
  int32_t null_slot_usage = 1;

  // Definition level at which field is not-null
  // For lists: indicates present (possibly null) child value
  int16_t def_level = 0;

  // Repetition level for this element or closest repeated ancestor
  // Any rep_level < this indicates new list OR empty list
  int16_t rep_level = 0;

  // Definition level indicating closest repeated ancestor is not empty
  // Used to discriminate between null vs excluded entirely
  int16_t repeated_ancestor_def_level = 0;

  // Methods:
  void IncrementOptional();  // Increments def_level
  int16_t IncrementRepeated();  // Increments both rep_level and def_level
  static LevelInfo ComputeLevelInfo(const ColumnDescriptor* descr);
};
```

### Key Insight: repeated_ancestor_def_level

**Purpose**: Discriminates between:
- **NULL value**: `defLevel < def_level` but `>= repeated_ancestor_def_level`
- **Excluded entirely**: `defLevel < repeated_ancestor_def_level`

**Example**: `list(struct(f0: int))`

```
Definition levels:
  0 = null list                      ← excluded
  1 = present but empty list         ← repeated_ancestor_def_level for struct/int
  2 = list with null struct          ← null value
  3 = non-null struct but null int   ← null value
  4 = present integer                ← present value
```

Data: `[null, [], [null], [{f0: null}], [{f0: 1}]]`
Def levels: `[0, 1, 2, 3, 4]`

**Struct array reconstruction**:
- `repeated_ancestor_def_level = 2`
- Skip entries with `defLevel < 2` (levels 0, 1)
- Struct array length = 3: `[not-set, set, set]`
- Int array length = 3: `[N/A, null, 1]`

### ValidityBitmapInputOutput Structure

Located in: `third_party/arrow/cpp/src/parquet/level_conversion.h:157-176`

```cpp
struct ValidityBitmapInputOutput {
  // Input: max values expected
  int64_t values_read_upper_bound = 0;

  // Output: actual values added
  int64_t values_read = 0;

  // Input/Output: null count
  int64_t null_count = 0;

  // Output: validity bitmap to populate (may be null for structs)
  uint8_t* valid_bits = nullptr;

  // Input: offset into valid_bits
  int64_t valid_bits_offset = 0;
};
```

---

## 3. Core Algorithms

### Algorithm 1: DefRepLevelsToListInfo

**Location**: `third_party/arrow/cpp/src/parquet/level_conversion.cc:46-127`

**Purpose**: Reconstructs list offsets and validity bitmap from definition/repetition levels

**Key Logic**:

```cpp
template <typename OffsetType>
void DefRepLevelsToListInfo(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output,
    OffsetType* offsets) {

  for (int x = 0; x < num_def_levels; x++) {
    // CRITICAL: Filter out values that belong to ancestor empty/null lists
    if (def_levels[x] < level_info.repeated_ancestor_def_level ||
        rep_levels[x] > level_info.rep_level) {
      continue;  // Skip this value
    }

    if (rep_levels[x] == level_info.rep_level) {
      // Continuation of EXISTING list at THIS level
      if (offsets != nullptr) {
        *offsets += 1;  // Increment current offset
      }
    } else {
      // rep_levels[x] < level_info.rep_level
      // Start of NEW list (ancestor empty lists filtered above)

      if (offsets != nullptr) {
        ++offsets;  // Move to next offset
        *offsets = *(offsets - 1);  // Initialize to previous (cumulative)

        // Check if this list has elements
        if (def_levels[x] >= level_info.def_level) {
          *offsets += 1;  // Add first element
        }
      }

      // Update validity bitmap
      if (valid_bits_writer) {
        // def_level - 1 distinguishes empty lists from null lists
        if (def_levels[x] >= level_info.def_level - 1) {
          valid_bits_writer->Set();  // List present (may be empty)
        } else {
          output->null_count++;
          valid_bits_writer->Clear();  // List NULL
        }
      }
    }
  }
}
```

**Critical observations**:

1. **Filtering**: `rep_levels[x] > level_info.rep_level` skips nested children
   - For `list<map>`, when reading the list, skip map entries (they have higher rep_level)

2. **Level comparison**: `rep_levels[x] == level_info.rep_level`
   - Exact match = continuation of current list
   - Less than = start new list

3. **Offsets can be nullptr**: For structs with repeated children, don't need offsets until reaching actual list children

### Algorithm 2: DefRepLevelsToBitmap

**Location**: `third_party/arrow/cpp/src/parquet/level_conversion.cc:168-177`

**Purpose**: Reconstruct validity bitmap for **structs** where all descendants contain lists

**Key insight**: Reuses `DefRepLevelsToListInfo` with `offsets=nullptr` and adjusted levels

```cpp
void DefRepLevelsToBitmap(
    const int16_t* def_levels,
    const int16_t* rep_levels,
    int64_t num_def_levels,
    LevelInfo level_info,
    ValidityBitmapInputOutput* output) {

  // IMPORTANT: Bump levels because this is for parent structs
  // DefRepLevelsToListInfo assumes it's for the list itself
  level_info.rep_level += 1;
  level_info.def_level += 1;

  // Call with offsets=nullptr (structs don't need offsets)
  DefRepLevelsToListInfo<int32_t>(
      def_levels, rep_levels, num_def_levels,
      level_info, output,
      /*offsets=*/nullptr);
}
```

### Algorithm 3: DefLevelsToBitmap

**Location**: `third_party/arrow/cpp/src/parquet/level_conversion.cc:131-148`

**Purpose**: Convert definition levels to validity bitmap for **non-list arrays** and **structs without list descendants**

**Simpler than DefRepLevelsToListInfo** because no repetition level handling needed.

---

## 4. How C++ Handles Each Case

### Case 1: struct { map }

**StructReader** implementation: `third_party/arrow/cpp/src/parquet/arrow/reader.cc:706-842`

#### Detection Logic (lines 715-725):

```cpp
// Try to find a child WITHOUT repeated descendants (simpler reconstruction)
auto result = std::find_if(
    children_.begin(), children_.end(),
    [](const std::unique_ptr<ColumnReaderImpl>& child) {
      return !child->IsOrHasRepeatedChild();
    });

if (result != children_.end()) {
  def_rep_level_child_ = result->get();
  has_repeated_child_ = false;  // Can use simpler DefLevelsToBitmap
} else if (!children_.empty()) {
  def_rep_level_child_ = children_.front().get();
  has_repeated_child_ = true;   // Must use DefRepLevelsToBitmap
}
```

**Key**: `IsOrHasRepeatedChild()` is a **recursive** check:
- `ListReader::IsOrHasRepeatedChild()` always returns `true` (line 597)
- `StructReader::IsOrHasRepeatedChild()` returns `has_repeated_child_` (line 728)
- Propagates up the tree

#### BuildArray Logic (lines 796-803):

```cpp
if (has_repeated_child_) {
  // Struct contains maps/lists - need both def AND rep levels
  RETURN_NOT_OK(GetDefLevels(&def_levels, &num_levels));
  RETURN_NOT_OK(GetRepLevels(&rep_levels, &num_levels));

  // Use DefRepLevelsToBitmap to reconstruct struct validity
  DefRepLevelsToBitmap(def_levels, rep_levels, num_levels,
                      level_info_, &validity_io);
} else if (filtered_field_->nullable()) {
  // Struct only has primitives - only need def levels
  RETURN_NOT_OK(GetDefLevels(&def_levels, &num_levels));
  DefLevelsToBitmap(def_levels, num_levels, level_info_, &validity_io);
}
```

**Result**:
- Structs with maps/lists **ARE supported** in Arrow C++
- Uses `DefRepLevelsToBitmap` which properly filters by `repeated_ancestor_def_level`
- Each child (including the map) reads its own data correctly

### Case 2: map<k, list<v>>

Maps are encoded as `list<struct<key, value>>` in Parquet.

**Reconstruction flow**:
1. **ListReader** (outer map list) calls `DefRepLevelsToListInfo`
   - Sets `level_info.rep_level` to the map's repetition level
   - Reconstructs map offsets (how many key-value pairs per map)
2. **StructReader** (key_value struct) detects `value` child is a list
   - Sets `has_repeated_child_ = true`
   - Uses `DefRepLevelsToBitmap` for struct validity
3. **ListReader** (value list) calls `DefRepLevelsToListInfo` again
   - Sets `level_info.rep_level` to the value list's repetition level
   - Reconstructs value list offsets

**Critical**: Each level uses its own `level_info.rep_level`:
- Map entries: `rep_level = N`
- Value list elements: `rep_level = N+1`
- Filter condition `rep_levels[x] > level_info.rep_level` skips deeper nesting

### Case 3: list<map<...>>

**Reconstruction flow**:
1. **ListReader** (outer list) calls `DefRepLevelsToListInfo`
   - `level_info.rep_level = 1` (list's repetition level)
   - `rep_levels[x] == 1` → new map in same list
   - `rep_levels[x] == 0` → new list (new row)
   - `rep_levels[x] > 1` → **skipped** (map entries belong to nested map)
2. **ListReader** (map, encoded as list<struct>) calls `DefRepLevelsToListInfo`
   - `level_info.rep_level = 2` (map's repetition level)
   - `rep_levels[x] == 2` → new key-value pair in same map
   - `rep_levels[x] < 2` → new map (already handled by outer list)
3. **StructReader** (key_value) reads key and value fields

**Key insight**: The filtering condition prevents double-counting:
```cpp
if (rep_levels[x] > level_info.rep_level) {
  continue;  // Skip, belongs to child
}
```

When the outer list processes entries, it skips `rep_level=2` entries (those belong to the map).

---

## 5. Level Tracking: How It Works

### Computing LevelInfo

**Location**: `third_party/arrow/cpp/src/parquet/level_conversion.h:125-140`

```cpp
static LevelInfo ComputeLevelInfo(const ColumnDescriptor* descr) {
  LevelInfo level_info;
  level_info.def_level = descr->max_definition_level();
  level_info.rep_level = descr->max_repetition_level();

  // Compute repeated_ancestor_def_level by walking up tree
  int16_t min_spaced_def_level = descr->max_definition_level();
  const Node* node = descr->schema_node().get();

  // Walk up until we hit a repeated node
  while (node && !node->is_repeated()) {
    if (node->is_optional()) {
      min_spaced_def_level--;  // Subtract optional node's contribution
    }
    node = node->parent();
  }

  level_info.repeated_ancestor_def_level = min_spaced_def_level;
  return level_info;
}
```

**Example**: `list(struct(f0: optional int))`

```
Leaf column: f0
- max_def_level = 4
- max_rep_level = 1

Walk up:
  1. f0 (optional int) → def -= 1 → min = 3
  2. struct → no change → min = 3
  3. list (repeated) → STOP

repeated_ancestor_def_level = 3? NO! The code subtracts BEFORE the repeated node.

Actually, let me re-read...

Walk up from f0:
  - f0 is optional → min_spaced_def_level = 4 - 1 = 3
  - parent is struct (not optional, not repeated) → min = 3
  - parent is list (repeated) → STOP

repeated_ancestor_def_level = 3
```

Wait, the example in LevelInfo says:
```
list(struct(f0: int))
  0 = null list
  1 = present but empty list.
  2 = a null value in the list
  3 = a non null struct but null integer.
  4 = a present integer.

repeated_ancestor_def_level = 2
```

So for struct and integer:
- Values with `defLevel < 2` are excluded (belong to null/empty ancestor list)
- Values with `defLevel >= 2` belong in the struct/int arrays

This makes sense: the definition level when the list is **present but empty** is 1.
So `repeated_ancestor_def_level = 1 + 1 = 2` (the def level when list has content).

### Per-Reader LevelInfo

Each reader in the tree has its own `LevelInfo`:
- **Leaf column reader**: Uses `ComputeLevelInfo(descr)` with full max levels
- **List reader**: Uses `LevelInfo` with `rep_level` set to THIS list's level
- **Struct reader**: Uses `LevelInfo` for struct presence

**Building the tree** (from schema to readers):
- Each repeated node calls `level_info.IncrementRepeated()` → bumps both rep and def
- Each optional node calls `level_info.IncrementOptional()` → bumps def
- Child readers get modified `LevelInfo` corresponding to their depth

---

## 6. Pseudocode for Swift Implementation

### Step 1: Add LevelInfo to Column

```swift
struct Column {
  // Existing fields...
  let maxDefinitionLevel: Int
  let maxRepetitionLevel: Int

  // NEW: Add repeated_ancestor_def_level
  let repeatedAncestorDefLevel: Int?
}
```

Compute in schema building:
```swift
func computeRepeatedAncestorDefLevel(for column: Column) -> Int? {
  guard column.maxRepetitionLevel > 0 else {
    return nil  // No repeated ancestor
  }

  var minSpacedDefLevel = column.maxDefinitionLevel
  var node = column.schemaNode

  // Walk up until we hit a repeated node
  while let current = node, current.repetitionType != .repeated {
    if current.repetitionType == .optional {
      minSpacedDefLevel -= 1
    }
    node = current.parent
  }

  return minSpacedDefLevel
}
```

### Step 2: Implement DefRepLevelsToListInfo

```swift
func reconstructListOffsets(
  defLevels: [UInt16],
  repLevels: [UInt16],
  levelInfo: LevelInfo
) -> ([Int32], Int) {  // (offsets, values_read)

  var offsets: [Int32] = [0]  // Start with offset 0
  var validBits: [Bool] = []
  var nullCount = 0

  for i in 0..<defLevels.count {
    let defLevel = Int(defLevels[i])
    let repLevel = Int(repLevels[i])

    // CRITICAL FILTER: Skip excluded values
    if defLevel < levelInfo.repeatedAncestorDefLevel ||
       repLevel > levelInfo.repLevel {
      continue
    }

    if repLevel == levelInfo.repLevel {
      // Continuation of existing list
      offsets[offsets.count - 1] += 1
    } else {
      // Start of new list (repLevel < levelInfo.repLevel)
      let previousOffset = offsets.last!
      var newOffset = previousOffset

      // Check if list has elements
      if defLevel >= levelInfo.defLevel {
        newOffset += 1
      }
      offsets.append(newOffset)

      // Update validity
      if defLevel >= levelInfo.defLevel - 1 {
        validBits.append(true)  // Present (possibly empty)
      } else {
        validBits.append(false)  // NULL
        nullCount += 1
      }
    }
  }

  return (offsets, validBits.count)
}
```

### Step 3: Update readRepeatedStruct for Structs with Complex Children

```swift
func readRepeatedStruct(at path: [String]) throws -> [[StructValue?]?] {
  let element = schema.element(at: path)

  // Check if struct has repeated children (maps/lists)
  let hasRepeatedChild = element.children.contains { child in
    hasRepeatedOrComplexDescendants(child)
  }

  if hasRepeatedChild {
    // Use DefRepLevelsToBitmap approach
    return try readRepeatedStructWithComplexChildren(at: path)
  } else {
    // Use current simpler approach
    return try readRepeatedStructSimple(at: path)
  }
}

func readRepeatedStructWithComplexChildren(at path: [String]) throws -> [[StructValue?]?] {
  let fieldReaders = try readFieldColumnsWithLevels(fieldColumns)

  // Get levels from any field (all share same struct-level def/rep)
  let defLevels = fieldReaders[0].definitionLevels
  let repLevels = fieldReaders[0].repetitionLevels

  // Build LevelInfo for THIS struct
  let levelInfo = LevelInfo(
    defLevel: repeatedAncestorDefLevel + 1,  // Struct presence level
    repLevel: column.maxRepetitionLevel,      // Struct's rep level
    repeatedAncestorDefLevel: repeatedAncestorDefLevel
  )

  // Use DefRepLevelsToListInfo with offsets=nil (just get validity)
  let structValidity = reconstructStructValidity(
    defLevels: defLevels,
    repLevels: repLevels,
    levelInfo: levelInfo
  )

  // Build struct values using validity
  var result: [[StructValue?]?] = []
  var currentList: [StructValue?] = []

  // Each child reader reconstructs its own data
  // (maps and lists inside will use their own DefRepLevelsToListInfo)

  // ... implementation continues
}
```

---

## 7. Key Insights for Swift Implementation

### 1. Recursive Detection is Correct

The current Swift fail-fast approach using `hasRepeatedOrComplexDescendants()` mirrors Arrow C++'s `IsOrHasRepeatedChild()`.

### 2. Two Paths Based on has_repeated_child

Arrow C++ uses different reconstruction methods:
- **No repeated children**: `DefLevelsToBitmap` (simpler, only def levels)
- **Has repeated children**: `DefRepLevelsToBitmap` (uses both def and rep levels)

Swift should follow same pattern:
- **Structs with only primitives**: Current implementation works
- **Structs with maps/lists**: Need `DefRepLevelsToBitmap` equivalent

### 3. LevelInfo is Essential

The `repeated_ancestor_def_level` field is CRITICAL for:
- Filtering excluded values: `defLevel < repeated_ancestor_def_level`
- Distinguishing NULL from empty: `defLevel == repeated_ancestor_def_level`

**Swift already has this**: `Column.repeatedAncestorDefLevel` exists and is computed correctly!

### 4. Per-Level Filtering

The key to multi-level reconstruction is:
```cpp
if (rep_levels[x] > level_info.rep_level) {
  continue;  // Skip, belongs to child
}
```

Each reader processes ONLY its own level's values, skipping deeper nesting.

### 5. Offsets Can Be Null

For structs containing lists/maps, pass `offsets=nullptr` to `DefRepLevelsToListInfo`.
Only actual list readers need offset arrays.

---

## 8. Implementation Roadmap for Swift

### Phase 4.1: Add LevelInfo Support ✅ (COMPLETE)

1. ✅ Already have: `Column.repeatedAncestorDefLevel` and `Column.repeatedAncestorDefLevels`
2. ✅ Created `LevelInfo` struct matching C++:
   ```swift
   struct LevelInfo {
     let defLevel: Int
     let repLevel: Int
     let repeatedAncestorDefLevel: Int
     var hasNullableValues: Bool
   }
   ```
3. ✅ Added `LevelInfo.from(column:)` factory method supporting multi-level repetition
4. ✅ Updated `ArrayReconstructor` to use `LevelInfo` with validation
5. ✅ Added comprehensive tests (11 tests, including schema-driven tests for single and multi-level lists)

**Key Implementation Details**:
- Single-level repetition (`maxRep=1`): Uses `column.repeatedAncestorDefLevel`
- Multi-level repetition (`maxRep>1`): Uses `column.repeatedAncestorDefLevels[maxRep-1]` for innermost level
- Returns `nil` for flat columns (no repetition)
- Validates level info matches actual data

### Phase 4.2: Port DefRepLevelsToListInfo ✅ (COMPLETE)

**Date Completed**: 2025-11-03

1. ✅ Implemented core algorithm with filtering
2. ✅ Added `offsets: inout [Int32]?` parameter (nullable for structs)
3. ✅ Added validity bitmap reconstruction via ValidityBitmapOutput struct
4. ✅ Added guardrails: `valuesReadUpperBound` and Int32 overflow checks
5. ✅ Tested with list<list<T>> and various edge cases
6. ✅ Added 12 comprehensive tests covering simple lists, empty lists, NULL lists, NULL elements, nested lists, required lists, and guardrails

**Key Implementation Details**:
- **Filtering**: `rep > repLevel` skips nested children (always applied)
- **Filtering**: `def < repeatedAncestorDefLevel` skips continuation values from NULL ancestors (only on `rep == repLevel` branch)
- **New lists**: Always create offset and validity entries (even for NULL lists)
- **Offset increments**: Only when `def > repeatedAncestorDefLevel` (list has content)
- **Validity**: true if `def >= repeatedAncestorDefLevel`, false otherwise
- **Performance**: In-place offset mutation with no copy-on-write penalty
- **Correctness**: Properly distinguishes NULL lists, empty lists, and lists with content

**Issues Fixed During Implementation**:
- Required lists with `repeatedAncestorDefLevel = 0` now handled correctly
- NULL lists create validity entries (not filtered out)
- NULL-first elements counted correctly (e.g., `[[NULL, 1]]`)
- Array copying eliminated for performance

### Phase 4.3: Port DefRepLevelsToBitmap (2-3 hours)

1. Implement as wrapper around `DefRepLevelsToListInfo`
2. Pass `offsets=nil` and adjust level_info
3. Use for struct validity when `has_repeated_child = true`

### Phase 4.4: Update StructReader (2-3 hours)

1. Add `hasRepeatedChild` detection (already have `hasRepeatedOrComplexDescendants`)
2. Branch on `hasRepeatedChild`:
   - `false`: Use current `DefLevelsToBitmap` approach
   - `true`: Use new `DefRepLevelsToBitmap` approach
3. Remove fail-fast errors
4. Each child reader reconstructs its own data

### Phase 4.5: Update MapReader (1-2 hours)

1. Remove fail-fast error for `map<k, list<v>>`
2. Let list value reader use `DefRepLevelsToListInfo`
3. Test with fixtures

### Phase 4.6: Testing (2-3 hours)

1. Update `NestedMapReaderTests` - remove `XCTExpectFailure`
2. Add tests for `map<k, list<v>>`
3. Add tests for `struct { map }`, `struct { list }`
4. Verify all edge cases (NULL, empty, nested)

**Total estimated time**: **12-18 hours** (refined from original 8-12)

---

## 9. References

### C++ Source Files

- **`level_conversion.h`**: LevelInfo structure definition
- **`level_conversion.cc`**: DefRepLevelsToListInfo, DefRepLevelsToBitmap, DefLevelsToBitmap
- **`reader.cc`**: StructReader, ListReader, MapReader implementations
- **`reader_internal.cc`**: Helper functions

### Key C++ Lines

- `level_conversion.h:28-154` - LevelInfo struct
- `level_conversion.cc:46-127` - DefRepLevelsToListInfo algorithm
- `level_conversion.cc:168-177` - DefRepLevelsToBitmap wrapper
- `reader.cc:715-725` - Recursive repeated child detection
- `reader.cc:796-803` - Struct reconstruction branching
- `reader.cc:597` - ListReader always has repeated child

### Swift Equivalent Files

- `Sources/Parquet/Schema/Column.swift` - Column with `repeatedAncestorDefLevel`
- `Sources/Parquet/Reader/ArrayReconstructor.swift` - Current list reconstruction
- `Sources/Parquet/Reader/RowGroupReader+Struct.swift` - Struct reading
- `Sources/Parquet/Reader/RowGroupReader+Map.swift` - Map reading

---

## 10. Conclusion

The Arrow C++ implementation provides a clear blueprint for supporting complex nested types:

1. **LevelInfo** tracks per-reader level thresholds
2. **DefRepLevelsToListInfo** filters and reconstructs using `repeated_ancestor_def_level`
3. **Recursive detection** (`IsOrHasRepeatedChild`) determines reconstruction strategy
4. **Each reader processes only its level** via `repLevel > level_info.rep_level` filtering

Swift implementation progress:
- ✅ `Column.repeatedAncestorDefLevel` and `repeatedAncestorDefLevels` computed correctly
- ✅ Recursive `hasRepeatedOrComplexDescendants()` detection
- ✅ `ArrayReconstructor` for lists (can be extended)
- ✅ **Phase 4.1 COMPLETE**: `LevelInfo` struct with factory method and comprehensive tests
- ✅ **Phase 4.2 COMPLETE**: `DefRepLevelsToListInfo` with filtering and validity bitmap reconstruction
- 🚧 Need `DefRepLevelsToBitmap` for structs (Phase 4.3)
- 🚧 Need branching in StructReader based on `hasRepeatedChild` (Phase 4.4)

**Next steps**: Implement Phase 4.3-4.6 following the roadmap above.
