// LevelComputer.swift - Compute repetition and definition levels for nested types
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Computes repetition and definition levels for nested structures (lists, maps, structs).
///
/// This is the **inverse** of the reader's `ArrayReconstructor` - it flattens nested Swift
/// structures into flat value sequences with level information that Parquet can encode.
///
/// # Background
///
/// Based on Apache Arrow C++'s `LevelInfo` and `ArrayWriter::WriteValues()` implementation:
/// - `cpp/src/parquet/level_conversion.h` - Level computation algorithms
/// - `cpp/src/parquet/column_writer.cc` - ArrayWriter implementation
///
/// # Key Principle
///
/// **Critical**: Empty and NULL lists produce level entries WITHOUT value entries.
///
/// This ensures `level_count >= value_count` (equality only when no empty/NULL lists).
///
/// # Example
///
/// ```swift
/// let lists: [[Int32]] = [[1, 2], [], [3]]
///
/// let result = LevelComputer.computeLevelsForList(
///     lists: lists,
///     maxDefinitionLevel: 2,
///     maxRepetitionLevel: 1
/// )
///
/// // Result (note: 4 level entries, 3 values):
/// // Index:      0  1  2  3
/// // values:     1  2  -  3      (- = no value for empty list)
/// // repLevels:  0  1  0  0      (0=new list, 1=continuation, 0=empty, 0=new)
/// // defLevels:  2  2  1  2      (2=value present, 1=empty list, 2=value present)
/// ```
struct LevelComputer {

    // MARK: - Single-Level Lists

    /// Compute repetition and definition levels for a single-level list.
    ///
    /// For `[[T]]` where the outer array represents top-level records and the inner array
    /// is a repeated field (list).
    ///
    /// **Level Encoding Rules** (matching Arrow C++ `ArrayWriter::WriteValues`):
    /// - **NULL list `nil`**: Emits `(rep=0, def=nullListDefLevel, value=NONE)`
    /// - **Empty list `[]`**: Emits `(rep=0, def=repeatedAncestorDefLevel, value=NONE)`
    /// - **First value in list**: Emits `(rep=0, def=maxDef, value=T)` (new list boundary)
    /// - **Continuation values**: Emit `(rep=maxRep, def=maxDef, value=T)` (same list)
    ///
    /// **Critical**: At least one (rep, def) tuple is emitted per logical parent list,
    /// even for empty lists. This matches Arrow/Parquet's expectation.
    ///
    /// - Parameters:
    ///   - lists: Nested array structure `[[T]?]`
    ///   - maxDefinitionLevel: Maximum definition level for this column (leaf level, value present)
    ///   - maxRepetitionLevel: Maximum repetition level for this column
    ///   - repeatedAncestorDefLevel: Definition level when entering the repeated group (empty list level)
    ///   - nullListDefLevel: Definition level when the list itself is NULL.
    ///                       Typically `repeatedAncestorDefLevel - 1`, but depends on optional ancestors.
    ///
    /// - Returns: Tuple of (values, repetitionLevels, definitionLevels)
    ///
    /// - Note: Level encoding matches Arrow C++ `LevelInfo`:
    ///         - NULL list: `def = nullListDefLevel` (< repeatedAncestorDefLevel)
    ///         - Empty list: `def = repeatedAncestorDefLevel`
    ///         - List with content: `def > repeatedAncestorDefLevel`
    static func computeLevelsForList<T>(
        lists: [[T]?],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullListDefLevel: Int
    ) -> (values: [T], repetitionLevels: [UInt16], definitionLevels: [UInt16]) {

        var values: [T] = []
        var repLevels: [UInt16] = []
        var defLevels: [UInt16] = []

        for list in lists {
            if list == nil {
                // NULL list: def = nullListDefLevel
                // This level depends on which ancestor is optional/NULL
                repLevels.append(0)
                defLevels.append(UInt16(nullListDefLevel))
                continue
            }

            let unwrappedList = list!

            if unwrappedList.isEmpty {
                // Empty list: def = repeatedAncestorDefLevel
                // List is present (entered repeated group), but has zero elements
                repLevels.append(0)
                defLevels.append(UInt16(repeatedAncestorDefLevel))
                continue
            }

            // List with values: emit one (rep, def, value) tuple per element
            for (index, value) in unwrappedList.enumerated() {
                values.append(value)

                if index == 0 {
                    // First element: new list boundary
                    repLevels.append(0)
                } else {
                    // Continuation: same list, additional element
                    repLevels.append(UInt16(maxRepetitionLevel))
                }

                // Value is present
                defLevels.append(UInt16(maxDefinitionLevel))
            }
        }

        return (values, repLevels, defLevels)
    }

    /// Compute levels for a single-level list with nullable elements.
    ///
    /// Handles the case where list elements themselves can be NULL: `[[T?]]`.
    ///
    /// **Level Encoding** (matching reader's `DefRepLevelsToListInfoTests.testListWithNullElements`):
    /// - NULL list: `def = nullListDefLevel` (< repeatedAncestorDefLevel)
    /// - Empty list: `def = repeatedAncestorDefLevel` (list present, zero elements)
    /// - NULL element within list: `def = nullElementDefLevel` (list present, element NULL)
    /// - Present element: `def = maxDefinitionLevel` (list present, element present)
    ///
    /// - Parameters:
    ///   - lists: Nested array with nullable elements `[[T?]?]`
    ///   - maxDefinitionLevel: Maximum definition level (leaf present)
    ///   - maxRepetitionLevel: Maximum repetition level
    ///   - repeatedAncestorDefLevel: Definition level when entering repeated group
    ///   - nullListDefLevel: Definition level when the list itself is NULL.
    ///                       Typically `repeatedAncestorDefLevel - 1`, but depends on optional ancestors.
    ///   - nullElementDefLevel: Definition level when an element is NULL.
    ///                          For simple `list<optional T>`, this is `maxDefinitionLevel - 1`.
    ///                          But if T is wrapped in optional structs, this level must be computed from schema.
    ///
    /// - Returns: Tuple of (values, repetitionLevels, definitionLevels)
    static func computeLevelsForListWithNullableElements<T>(
        lists: [[T?]?],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullListDefLevel: Int,
        nullElementDefLevel: Int
    ) -> (values: [T], repetitionLevels: [UInt16], definitionLevels: [UInt16]) {

        var values: [T] = []
        var repLevels: [UInt16] = []
        var defLevels: [UInt16] = []

        for list in lists {
            if list == nil {
                // NULL list: def = nullListDefLevel
                repLevels.append(0)
                defLevels.append(UInt16(nullListDefLevel))
                continue
            }

            let unwrappedList = list!

            if unwrappedList.isEmpty {
                // Empty list: def = repeatedAncestorDefLevel
                repLevels.append(0)
                defLevels.append(UInt16(repeatedAncestorDefLevel))
                continue
            }

            // List with elements (some may be NULL)
            for (index, element) in unwrappedList.enumerated() {
                // Repetition level
                if index == 0 {
                    repLevels.append(0)  // New list
                } else {
                    repLevels.append(UInt16(maxRepetitionLevel))  // Continuation
                }

                // Definition level and value
                if let value = element {
                    // Element is present
                    defLevels.append(UInt16(maxDefinitionLevel))
                    values.append(value)
                } else {
                    // Element is NULL: def = nullElementDefLevel
                    // This level depends on schema structure (optional ancestors between repeated and leaf)
                    defLevels.append(UInt16(nullElementDefLevel))
                    // No value emitted for NULL element
                }
            }
        }

        return (values, repLevels, defLevels)
    }

    // MARK: - Multi-Level Lists (Phase 3)

    /// Stack-based traversal state for multi-level list flattening
    private struct TraversalState {
        let array: [Any?]  // Current array being traversed
        var index: Int     // Current position in array
        let level: Int     // Nesting level (0-indexed, 0 = outermost)
    }

    /// Compute repetition and definition levels for multi-level nested lists.
    ///
    /// Handles nested list structures with `maxRepetitionLevel > 1`, such as:
    /// - 2-level: `[[[T]?]?]` (list of optional lists)
    /// - 3-level: `[[[[T]?]?]?]` (list of optional lists of optional lists)
    /// - etc.
    ///
    /// This is the inverse of `ArrayReconstructor.reconstructNestedArrays()`.
    ///
    /// **Algorithm**:
    /// 1. Traverse nested arrays depth-first using a stack
    /// 2. Track current nesting level (0 = outermost, maxRep-1 = innermost)
    /// 3. Emit repetition level based on which level changed:
    ///    - `rep = levelIndex` when starting a new list at that level
    ///    - `rep = maxRep` when adding elements to the innermost list
    /// 4. Emit definition level based on NULL/empty/present state:
    ///    - NULL list at level L: `def = nullListDefLevels[L]`
    ///    - Empty list at level L: `def = repeatedAncestorDefLevels[L]`
    ///    - Present value: `def = maxDef`
    ///
    /// **Example (2-level list)**:
    /// ```swift
    /// let lists: [[[Int32]?]?] = [[[1, 2], [3]], [[4]]]
    ///
    /// // Traversal:
    /// // [0][0][0] = 1  → rep=0 (new outer), def=maxDef
    /// // [0][0][1] = 2  → rep=2 (continue inner), def=maxDef
    /// // [0][1][0] = 3  → rep=1 (continue outer, new inner), def=maxDef
    /// // [1][0][0] = 4  → rep=0 (new outer), def=maxDef
    ///
    /// // Result:
    /// values = [1, 2, 3, 4]
    /// repLevels = [0, 2, 1, 0]
    /// defLevels = [maxDef, maxDef, maxDef, maxDef]
    /// ```
    ///
    /// - Parameters:
    ///   - lists: Type-erased nested array (use `Any` for flexibility)
    ///   - maxDefinitionLevel: Maximum definition level (leaf value present)
    ///   - maxRepetitionLevel: Maximum repetition level (must be >= 2)
    ///   - repeatedAncestorDefLevels: Array of def levels for empty lists at each nesting level.
    ///                                Index 0 = outermost, index maxRep-1 = innermost.
    ///                                Length must equal maxRepetitionLevel.
    ///   - nullListDefLevels: Array of def levels for NULL lists at each nesting level.
    ///                        Index 0 = outermost, index maxRep-1 = innermost.
    ///                        Length must equal maxRepetitionLevel.
    ///
    /// - Returns: Tuple of (values, repetitionLevels, definitionLevels)
    ///
    /// - Throws: If input validation fails or nesting structure doesn't match maxRepetitionLevel
    static func computeLevelsForNestedList<T>(
        lists: Any,
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevels: [Int],
        nullListDefLevels: [Int]
    ) throws -> (values: [T], repetitionLevels: [UInt16], definitionLevels: [UInt16]) {

        guard maxRepetitionLevel >= 2 else {
            fatalError(
                "computeLevelsForNestedList requires maxRepetitionLevel >= 2, got \(maxRepetitionLevel). " +
                "Use computeLevelsForList for single-level (maxRepetitionLevel == 1)"
            )
        }

        guard repeatedAncestorDefLevels.count == maxRepetitionLevel else {
            fatalError(
                "repeatedAncestorDefLevels must have length \(maxRepetitionLevel), got \(repeatedAncestorDefLevels.count)"
            )
        }

        guard nullListDefLevels.count == maxRepetitionLevel else {
            fatalError(
                "nullListDefLevels must have length \(maxRepetitionLevel), got \(nullListDefLevels.count)"
            )
        }

        var values: [T] = []
        var repLevels: [UInt16] = []
        var defLevels: [UInt16] = []

        // Initialize with top-level lists
        guard let topLevelLists = lists as? [Any?] else {
            fatalError("lists must be an array of nested optional arrays")
        }

        var stack: [TraversalState] = [TraversalState(array: topLevelLists, index: 0, level: 0)]

        // Track previous element's path to compute repetition level
        // previousPath[i] = element index at nesting level i (0-based)
        var previousPath: [Int] = []

        while !stack.isEmpty {
            let currentStackLevel = stack.count - 1
            var current = stack[currentStackLevel]

            // Check if we've exhausted the current array
            if current.index >= current.array.count {
                // Pop this level and move to next element in parent
                stack.removeLast()
                if !stack.isEmpty {
                    stack[stack.count - 1].index += 1
                }
                continue
            }

            // Build current element path (which element we're processing at each level)
            // IMPORTANT: Use current.index (not incremented yet) for accurate path
            var currentPath: [Int] = []
            for i in 0...currentStackLevel {
                currentPath.append(stack[i].index)
            }

            let element = current.array[current.index]

            // Compute repetition level by finding deepest shared ancestor
            var repLevel: Int
            if previousPath.isEmpty {
                // First element
                repLevel = 0
            } else {
                // Find the deepest level where paths match
                var sharedDepth = 0
                while sharedDepth < min(previousPath.count, currentPath.count) &&
                      previousPath[sharedDepth] == currentPath[sharedDepth] {
                    sharedDepth += 1
                }
                // Repetition level = deepest level that's continuing
                repLevel = sharedDepth
            }

            // Handle the element based on its type
            if element == nil {
                // NULL list at this level
                repLevels.append(UInt16(repLevel))
                defLevels.append(UInt16(nullListDefLevels[currentStackLevel]))
                previousPath = currentPath

                // Move to next element at this level
                current.index += 1
                stack[currentStackLevel] = current
                continue
            }

            // Check if this is a nested array or a leaf value
            if let nestedArray = element as? [Any?] {
                if nestedArray.isEmpty {
                    // Empty list at this level - emit level entry
                    repLevels.append(UInt16(repLevel))
                    defLevels.append(UInt16(repeatedAncestorDefLevels[currentStackLevel]))
                    previousPath = currentPath

                    // Move to next element at this level
                    current.index += 1
                    stack[currentStackLevel] = current
                } else {
                    // Non-empty nested list - descend into it
                    // DON'T increment current.index yet - we'll do it when we pop back
                    stack.append(TraversalState(array: nestedArray, index: 0, level: currentStackLevel + 1))
                }
            } else {
                // Leaf value - emit it
                guard let value = element as? T else {
                    fatalError("Expected leaf value of type \(T.self), got \(type(of: element))")
                }

                values.append(value)
                repLevels.append(UInt16(repLevel))
                defLevels.append(UInt16(maxDefinitionLevel))
                previousPath = currentPath

                // Move to next element at this level
                current.index += 1
                stack[currentStackLevel] = current
            }
        }

        return (values, repLevels, defLevels)
    }
}
