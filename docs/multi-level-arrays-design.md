# Multi-Level Array Reconstruction Design

## Overview

This document outlines the design for generalizing the current single-level array reconstruction to support arbitrary nesting depths (Phase 4.1).

**Status**: Design finalized and ready for implementation ✅

## Design Revisions (Based on Feedback)

This design addresses critical issues raised during review:

### 1. Null vs Empty List Semantics ✅
**Issue**: Original design had open question about how to represent null lists vs empty lists.

**Resolution**:
- Follow Apache Arrow semantics exactly
- Empty list `[]`: Present with zero elements
- Null list `nil`: Not present, appears as nil in parent array
- Added comprehensive section on detection and representation

### 2. API Backward Compatibility ✅
**Issue**: Returning `Any` from `readAllRepeated()` would break existing callers expecting `[[T?]]`.

**Resolution**:
- Keep `readAllRepeated() -> [[T?]]` for single-level (maxRep=1) unchanged
- Add new `readAllNested() -> Any` for multi-level (maxRep>1)
- No breaking changes for existing code

### 3. repeatedAncestorDefLevel == 0 Handling ✅
**Issue**: Need to extend the recent fix (guard on `repeatedAncestorDefLevel > 0`) to multi-level.

**Resolution**:
- Same guard pattern applies at each nesting level
- Documented explicitly in null semantics section
- Added to implementation strategy

### 4. Test Coverage for Null Lists ✅
**Issue**: Current tests focus on null elements, not null lists themselves.

**Resolution**:
- Added explicit test fixtures for null lists at intermediate levels
- Test cases include assertions: `result[1] == nil` (not `[]`)
- Cover mixed null/empty scenarios: `[[[]], nil, [[1]]]`

### 5. Single-Level Bug Fix Required ✅
**Issue**: Current single-level code treats null lists as empty lists.

**Resolution**:
- Added Phase 0 to fix before multi-level implementation
- Changes return type to `[[T?]?]` - breaking but correct
- Added test case to verify fix

## Current State (Single-Level Arrays)

**Status**: ✅ Complete
**File**: `Sources/Parquet/Reader/ArrayReconstructor.swift`
**Capability**: Handles `maxRepetitionLevel = 1` (e.g., `[[1, 2], [3]]`)

### Current Algorithm

The existing algorithm uses:
- Single `currentList` to accumulate elements
- `needsAppend` flag to track when to emit
- Checks `repLevel < maxRepetitionLevel` to detect new list

**Key Pattern**:
```swift
if repLevel < maxRepetitionLevel {  // rep=0 means new list
    // Start new list
    if needsAppend {
        result.append(currentList)
    }
    currentList = []
    needsAppend = true
}
```

This works because with max=1, there are only two rep levels:
- `rep=0`: Start new outer list
- `rep=1`: Continue current list

## Target State (Multi-Level Arrays)

**Goal**: Handle `maxRepetitionLevel > 1` (e.g., `[[[1, 2]], [[3]]]`)

### Challenge

With 3 levels (max=2), we have three rep levels:
- `rep=0`: Start new outermost list
- `rep=1`: Start new middle list
- `rep=2`: Continue innermost list

Need to track which level we're at and when to close/open lists.

## Null vs Empty List Semantics (CRITICAL)

### Arrow Semantics

In Apache Arrow (and Parquet), null lists and empty lists are **distinct**:

1. **Empty List**: `[]`
   - Produces empty child array (length = 0)
   - List itself is present, just contains no elements
   - `defLevel >= repeatedAncestorDefLevel`

2. **Null List**: `nil`
   - No child values produced
   - Appears as `nil` in parent array
   - `defLevel < repeatedAncestorDefLevel`

### Example

```python
# PyArrow data
data = [
    [1, 2],      # Normal list
    [],          # Empty list (present, zero elements)
    None,        # Null list (not present)
    [3]          # Normal list
]
```

**Expected representation**:
```swift
[
    [1, 2],      // Normal
    [],          // Empty (not nil!)
    nil,         // Null
    [3]          // Normal
]
```

### Current Single-Level Behavior

**Problem**: Current `ArrayReconstructor.reconstructArrays()` **drops null lists entirely**:

```swift
// Line 96-100
if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
    result.append([])  // Appends empty, not nil!
    needsAppend = false
    continue
}
```

This is **incorrect** - it treats null lists as empty lists.

### Correct Multi-Level Semantics

**Decision**: Extend null handling to properly represent null lists at each level:

```swift
if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
    // This is a NULL list (not empty)
    if needsAppend {
        result.append(currentList)  // Finish previous list
    }
    result.append(nil)  // Append nil for null list
    currentList = []
    needsAppend = false
    continue
}

// Empty list: defLevel >= repeatedAncestorDefLevel but no child values
// Handled naturally by rep level transitions
```

### Per-Level Null Detection

For multi-level nesting, each level can have null or empty lists:

**Example**: `[[[1]], None, [[]], [[2]]]`

```
Index | Rep | Def | Value | Interpretation
------|-----|-----|-------|---------------
0     | 0   | 3   | 1     | L0[0] = [[1]]
1     | 0   | 0   | -     | L0[1] = None (null list at L0)
2     | 0   | 2   | -     | L0[2] = [[]] (empty L1)
3     | 0   | 3   | 2     | L0[3] = [[2]]
```

**Key insight**: `defLevel` tells us **which level** is null:
- `defLevel = 0`: Top-level list is null
- `defLevel = 1`: Middle-level list is null (top is present)
- `defLevel = 2`: Bottom-level list is null (top and middle present)
- `defLevel = 3`: All lists present, value is present

### repeatedAncestorDefLevel == 0 Handling

**Critical**: When `repeatedAncestorDefLevel == 0` (top-level list with required parent):
- `defLevel < repeatedAncestorDefLevel` check **never fires** (0 < 0 is false)
- This is **correct** - there can be no null ancestor
- Empty list detection must use different criteria

**Current fix in single-level** (line 96):
```swift
if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
    // Only check when repeatedAncestorDefLevel > 0
}
```

**Must extend to multi-level**: Same guard applies at each nesting level.

### Implementation Strategy

1. **Track null slots explicitly**: When `defLevel` indicates null, append `nil` to parent
2. **Empty lists implicit**: When rep level starts new list with zero elements
3. **Per-level detection**: Check `defLevel` against each level's threshold
4. **Guard on repeatedAncestorDefLevel > 0**: Only apply null-detection when meaningful

## Design Approach

### Core Insight from Exploration

> "Repetition levels encode **which level to repeat at**. The transition from `rep=1` to `rep=0` means 'close level 1 and start new level 0'. The transition from `rep=2` to `rep=1` means 'close level 2, keep level 1 open, start new level 2'."

### Algorithm Structure

Replace single `currentList` with `listStack`:

```swift
// Stack where listStack[0] = outermost, listStack[maxRep] = innermost
var listStack: [Any] = Array(repeating: [], count: maxRepetitionLevel + 1)
```

On each element:
1. **Detect level transitions**: Compare current vs previous rep level
2. **Close lists**: When rep decreases, close levels between previous and current
3. **Add value**: Append to innermost level (listStack[maxRep])
4. **Open lists**: When rep increases, lists are implicitly opened

### Example Walkthrough

**Data**: `[[[1, 2], [3]], [[4]]]`

**Levels**:
```
Index | Rep | Def | Value | Action
------|-----|-----|-------|-------
0     | 0   | 3   | 1     | Start L0, L1, L2; add 1
1     | 2   | 3   | 2     | Continue L2; add 2
2     | 1   | 3   | 3     | Close L2→L1; open new L2; add 3
3     | 0   | 3   | 4     | Close L2,L1→L0; open L1,L2; add 4
EOF   | -   | -   | -     | Close all levels
```

**State after each**:
```
After 0: listStack = [[], [[1]]]           // L0 open with L1 containing L2=[1]
After 1: listStack = [[], [[1, 2]]]        // Added 2 to L2
After 2: listStack = [[], [[1,2], [3]]]    // Closed L2, opened new L2, added 3
After 3: listStack = [[[[1,2],[3]]], [[4]]] // Closed L1, opened new L1,L2
```

## Implementation Plan

### Phase 0: Fix Single-Level Null Handling (0.5 days)

**CRITICAL**: Before implementing multi-level, fix existing bug in single-level:

**File**: `Sources/Parquet/Reader/ArrayReconstructor.swift` (lines 96-100)

**Current code** (WRONG):
```swift
if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
    result.append([])  // Wrong: treats null list as empty!
    needsAppend = false
    continue
}
```

**Fixed code** (CORRECT):
```swift
if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
    // This is a NULL list, not empty
    if needsAppend {
        result.append(currentList)  // Finish previous list first
    }
    result.append(nil)  // Append nil for null list
    currentList = []
    needsAppend = false
    continue
}
```

**Impact**: Changes return type from `[[T?]]` to `[[T?]?]` - **breaking change!**

**Testing**: Add fixture with null lists and verify:
```swift
// PyArrow: [[1, 2], None, [3]]
// Expected: [[1, 2], nil, [3]]
// Current (wrong): [[1, 2], [], [3]]
```

### Phase 1: Core Algorithm (3-4 days)

**File**: `Sources/Parquet/Reader/ArrayReconstructor.swift`

Add new method alongside existing `reconstructArrays`:

```swift
static func reconstructNestedArrays<T>(
    values: [T],
    definitionLevels: [UInt16],
    repetitionLevels: [UInt16],
    maxDefinitionLevel: Int,
    maxRepetitionLevel: Int,
    repeatedAncestorDefLevel: Int
) throws -> Any
```

**Key Changes**:
1. Use list stack instead of single list
2. Track `previousRepLevel` to detect transitions
3. Close levels when `repLevel < previousRepLevel`
4. Handle empty/null lists correctly

### Phase 2: Column Reader Integration (2-3 days)

**Files**: All 5 column readers (Int32, Int64, Float, Double, String)

Update `readAllRepeated()` to delegate to generalized reconstructor:

```swift
public func readAllRepeated() throws -> Any {
    // ... existing code to collect values/levels ...

    if maxRepetitionLevel == 1 {
        // Use existing single-level algorithm
        return try ArrayReconstructor.reconstructArrays(...)
    } else {
        // Use new multi-level algorithm
        return try ArrayReconstructor.reconstructNestedArrays(...)
    }
}
```

**Alternative**: Replace entirely once confident in new algorithm.

### Phase 3: Test Fixtures (2 days)

**File**: `Tests/ParquetTests/Fixtures/generate_nested_fixtures.py` (extend existing)

Generate fixtures for:

**Basic 2-Level**:
- `nested_int32_2level_simple.parquet`: `[[[1, 2]], [[3]]]`
- `nested_int32_2level_empty_inner.parquet`: `[[[]], [[1]]]` (empty inner list)
- `nested_int32_2level_empty_outer.parquet`: `[[], [[1]]]` (empty outer list)

**Critical: Null List Tests**:
- `nested_int32_2level_null_inner.parquet`: `[[[1]], None, [[2]]]` (null inner list at L1)
- `nested_int32_2level_null_element.parquet`: `[[[None, 1]]]` (null element, not null list)
- `nested_int32_2level_mixed_null_empty.parquet`: `[[[]], None, [[1]]]` (mix empty and null)

**3-Level**:
- `nested_int32_3level_simple.parquet`: `[[[[1]]]]`
- `nested_int32_3level_null.parquet`: `[[[[1]]], None, [[[[2]]]]]` (null at L2)

**All Types**:
- Repeat critical patterns for Int64, Float, Double, String

**Total**: ~15-20 fixtures covering null/empty semantics at each level

### Phase 4: Integration Tests (2-3 days)

**File**: `Tests/ParquetTests/Reader/NestedColumnTests.swift` (new)

Test cases:

**Basic 2-Level**:
- `testTwoLevelInt32Simple()`: `[[[1, 2]], [[3]]]`
- `testTwoLevelInt32EmptyInner()`: `[[[]], [[1]]]` - Empty inner lists
- `testTwoLevelInt32EmptyOuter()`: `[[], [[1]]]` - Empty outer list

**Critical: Null List Tests**:
- `testTwoLevelInt32NullInner()`: `[[[1]], nil, [[2]]]` - Null list at L1
  - **Assert**: `result[1] == nil` (not `[]`!)
- `testTwoLevelInt32NullElement()`: `[[[nil, 1]]]` - Null element (not list)
  - **Assert**: `result[0]![0]![0] == nil`
- `testTwoLevelInt32MixedNullEmpty()`: `[[[]], nil, [[1]]]` - Mix empty and null
  - **Assert**: `result[0]! == []` and `result[1] == nil`

**3-Level**:
- `testThreeLevelInt32Simple()`: `[[[[1]]]]`
- `testThreeLevelInt32WithNull()`: `[[[[1]]], nil, [[[[2]]]]]` - Null at L2

**All Types**:
- Repeat critical patterns for Int64, Float, Double, String

**Error Cases**:
- `testReadAllRepeatedRejectsMultiLevel()`: Verify single-level method rejects maxRep > 1
- `testReadAllNestedRejectsSingleLevel()`: Verify multi-level method rejects maxRep = 1

**Total**: ~20-25 test methods with explicit null/empty assertions

## Type Safety Challenge

### Problem

Return type depends on `maxRepetitionLevel`:
- max=1: `[[T?]]`
- max=2: `[[[T?]]]`
- max=3: `[[[[T?]]]]`

Swift's type system can't express this variability.

### Solutions

**Option A: Return `Any` (Recommended for Phase 4.1)**

```swift
public func readAllRepeated() throws -> Any {
    // Return type depends on maxRepetitionLevel
    // Caller must cast based on schema
}
```

**Pros**: Simple, flexible, matches exploration recommendation
**Cons**: Loses type safety

**Option B: Overloaded Methods (Future Phase 5)**

```swift
public func readAllRepeated() throws -> [[T?]]              // max=1
public func readAllNested2() throws -> [[[T?]]]             // max=2
public func readAllNested3() throws -> [[[[T?]]]]           // max=3
```

**Pros**: Type safe
**Cons**: Caller needs to know depth upfront

**Option C: Generic Wrapper (Future Phase 5)**

```swift
public enum NestedArray<T> {
    case level1([[T?]])
    case level2([[[T?]]])
    case level3([[[[T?]]]])
}
```

**Pros**: Type safe with pattern matching
**Cons**: Verbose

**Decision**: Use Option A for Phase 4.1, defer type safety to Phase 5.

## API Changes

### Current API (Single-Level)

```swift
let column = try rowGroup.int32Column(at: 0)
let arrays: [[Int32?]] = try column.readAllRepeated()
// Works for maxRepetitionLevel = 1
```

### Proposed API (Multi-Level) - BACKWARD COMPATIBLE

**Decision**: Keep existing signature for single-level, use conditional return type:

```swift
// In column readers (Int32ColumnReader, etc.)
public func readAllRepeated() -> [[T?]] {
    guard maxRepetitionLevel == 1 else {
        fatalError("Use readAllNested() for maxRepetitionLevel > 1")
    }
    // Existing single-level algorithm
    return try ArrayReconstructor.reconstructArrays(...)
}

public func readAllNested() -> Any {
    guard maxRepetitionLevel > 1 else {
        fatalError("Use readAllRepeated() for maxRepetitionLevel = 1")
    }
    // New multi-level algorithm
    return try ArrayReconstructor.reconstructNestedArrays(...)
}
```

**Rationale**:
- No breaking changes for existing callers
- Clear API separation: `readAllRepeated()` for single-level, `readAllNested()` for multi-level
- Caller checks `maxRepetitionLevel` from schema to know which to call
- Future: Could add convenience method that dispatches automatically

**Migration Path**:
```swift
// Current code (still works)
if column.maxRepetitionLevel == 1 {
    let arrays: [[Int32?]] = try column.readAllRepeated()
}

// New multi-level code
if column.maxRepetitionLevel > 1 {
    let nested: Any = try column.readAllNested()
    // Cast based on schema knowledge
    if column.maxRepetitionLevel == 2 {
        let arrays2 = nested as! [[[Int32?]]]
    }
}
```

## Error Handling

### New Error Cases

1. **Unsupported depth**: `maxRepetitionLevel > 10` (arbitrary sanity limit)
2. **Inconsistent levels**: Rep level jumps by more than 1
3. **Stack underflow**: Trying to close level 0

### Error Messages

```swift
throw ColumnReaderError.unsupportedFeature(
    "Nesting depth \(maxRepetitionLevel) exceeds maximum supported (10)"
)

throw ColumnReaderError.internalError(
    "Repetition level jumped from \(prev) to \(curr) (expected increment of 1)"
)
```

## Testing Strategy

### Unit Tests (ArrayReconstructor)

Test the reconstructor in isolation:
- Various rep/def level patterns
- Empty lists at each level
- Null elements
- Edge cases (all empty, all null, single element)

### Integration Tests (Column Readers)

Test full read path:
- Generate PyArrow fixtures
- Read with column readers
- Verify exact output matches expected

### Edge Cases

1. All lists empty: `[[], [[]]]`
2. Deepest nulls: `[[[None]]]`
3. Unbalanced depth: Some branches deeper than others (if allowed by schema)

## Performance Considerations

### Memory

List stack size = `maxRepetitionLevel + 1` (trivial)

### Time Complexity

- Single pass through values
- O(maxRep) work per element to close levels
- Total: O(N * maxRep) where N = number of values

For typical data (maxRep ≤ 3): Essentially O(N)

## Documentation Updates

1. **README.md**: Update features list
   - Change "✅ Repeated columns (single-level)" → "✅ Repeated columns (multi-level nested arrays)"

2. **docs/limitations.md**: Update nested types status
   - Mark multi-level arrays as complete
   - Keep structs/maps as pending

3. **API Guide**: Add nested array examples

## Success Criteria

Phase 4.1 is complete when:
- ✅ ArrayReconstructor handles N levels (tested up to 3)
- ✅ All 5 column types support multi-level
- ✅ PyArrow fixtures for 2 and 3 levels exist
- ✅ 20+ integration tests pass
- ✅ Documentation updated
- ✅ No regression in single-level tests (all 285 tests still pass)

## Timeline

**Total: 10-15 days (2-3 weeks)**

- **Phase 0** - Fix single-level null handling: 0.5 days
- **Phase 1** - Core algorithm: 3-4 days
- **Phase 2** - Column reader integration: 2-3 days
- **Phase 3** - Test fixtures: 2 days
- **Phase 4** - Integration tests: 2-3 days
- Documentation: 1-2 days

## Design Decisions (RESOLVED)

### 1. Null Lists vs Empty Lists ✅

**Decision**: Follow Arrow semantics exactly:
- **Empty list**: `[]` - Present with zero elements
- **Null list**: `nil` - Not present, appears as nil in parent array

**Implementation**:
- `defLevel < repeatedAncestorDefLevel` → Append `nil` to parent
- Empty lists handled by rep level transitions with zero elements
- Guard on `repeatedAncestorDefLevel > 0` to avoid false positives

### 2. API Backward Compatibility ✅

**Decision**: Keep existing `readAllRepeated()` signature for single-level:
- `readAllRepeated() -> [[T?]]` for `maxRepetitionLevel == 1` (unchanged)
- `readAllNested() -> Any` for `maxRepetitionLevel > 1` (new)

**Rationale**: No breaking changes for existing callers

### 3. Maximum Depth Limit ✅

**Decision**: Yes, enforce limit of 10 levels (sanity check)

**Rationale**:
- Real-world data rarely exceeds 3-4 levels
- Prevents accidental stack overflow
- Can be increased if needed

### 4. Type System Workaround ✅

**Decision**: Return `Any` for multi-level, defer type safety to Phase 5

**Rationale**: Swift can't express variable-depth nesting; pragmatic choice for Phase 4.1

## References

- Exploration analysis: `/tmp/nested_types_analysis.md`
- Implementation examples: `/tmp/nested_implementation_examples.md`
- Current implementation: `Sources/Parquet/Reader/ArrayReconstructor.swift:39-157`
- Dremel paper: https://research.google/pubs/dremel-interactive-analysis-of-web-scale-datasets/
