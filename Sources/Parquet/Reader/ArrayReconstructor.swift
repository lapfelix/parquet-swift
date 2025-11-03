// ArrayReconstructor - Reconstructs arrays from flat values and def/rep levels
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Helper for reconstructing nested arrays from flat value sequences using
/// definition and repetition levels.
///
/// Based on Apache Arrow C++ `DefRepLevelsToListInfo` implementation.
///
/// # Background
///
/// Parquet stores repeated fields (arrays/lists) as flat value sequences with
/// definition and repetition levels that encode structure:
///
/// - **Repetition level = 0**: Start of new array
/// - **Repetition level = maxRepetitionLevel**: Continuation of current array
/// - **Definition level**: Distinguishes null elements, empty arrays, and present values
///
/// # Example
///
/// ```swift
/// // Data: [[1, 2], [], [3]]
/// let values = [1, 2, 3]  // Only non-null payloads
/// let defLevels: [UInt16] = [1, 1, 0, 1]  // 0=empty array, 1=present value
/// let repLevels: [UInt16] = [0, 1, 0, 0]  // 0=new array, 1=continuation
///
/// let result = try ArrayReconstructor.reconstructArrays(
///     values: values,
///     definitionLevels: defLevels,
///     repetitionLevels: repLevels,
///     maxDefinitionLevel: 1,
///     maxRepetitionLevel: 1,
///     repeatedAncestorDefLevel: 1
/// )
/// // Returns: [[1, 2], [], [3]]
/// ```
struct ArrayReconstructor {
    /// List state for multi-level reconstruction
    private enum ListState {
        case uninitialized  // List never appeared
        case null           // def < ancestorDef → NULL list
        case empty          // def == ancestorDef → EMPTY list (present, zero elements)
        case populated      // def > ancestorDef → list with elements/nulls
    }

    /// Reconstructs arrays from flat value sequence and def/rep levels.
    ///
    /// - Parameters:
    ///   - values: Non-null payloads only (no Optional wrapper)
    ///   - definitionLevels: One per logical value (including nulls and empty lists)
    ///   - repetitionLevels: One per logical value (including nulls and empty lists)
    ///   - maxDefinitionLevel: Maximum definition level for this column
    ///   - maxRepetitionLevel: Maximum repetition level for this column
    ///   - repeatedAncestorDefLevel: Definition level at which the repeated ancestor is present
    ///
    /// - Returns: Array of arrays where:
    ///   - Outer nil represents NULL list (list not present)
    ///   - Inner nil represents NULL element (element not present)
    ///   - Empty array [] represents empty list (list present, zero elements)
    ///
    /// - Throws: `ColumnReaderError` if levels are invalid
    static func reconstructArrays<T>(
        values: [T],
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int
    ) throws -> [[T?]?] {
        guard definitionLevels.count == repetitionLevels.count else {
            throw ColumnReaderError.internalError(
                "Definition and repetition level counts must match " +
                "(\(definitionLevels.count) vs \(repetitionLevels.count))"
            )
        }

        var result: [[T?]?] = []
        var currentList: [T?] = []
        var valueIndex = 0
        var needsAppend = false  // Track if currentList needs to be appended at end

        for i in 0..<definitionLevels.count {
            let defLevel = Int(definitionLevels[i])
            let repLevel = Int(repetitionLevels[i])

            // Invalid: rep level should never exceed max for this column
            if repLevel > maxRepetitionLevel {
                throw ColumnReaderError.internalError(
                    "Repetition level \(repLevel) exceeds column max \(maxRepetitionLevel)"
                )
            }

            if repLevel < maxRepetitionLevel {
                // Start of new list
                // Finish previous list if we have one pending
                if needsAppend {
                    result.append(currentList)
                }

                // Start new list
                currentList = []

                // Check if this is a NULL list (def < repeatedAncestorDefLevel)
                // Only applies when repeatedAncestorDefLevel > 0 (i.e., there are optional ancestors)
                // NULL list: The list itself is not present, represented as nil
                // Empty list: The list is present but contains zero elements, represented as []
                if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
                    // This is a NULL list (not empty) - append nil
                    result.append(nil)
                    needsAppend = false
                    continue
                }

                // Add first element based on definition level
                if defLevel >= maxDefinitionLevel {
                    // Non-null first element - list has value
                    guard valueIndex < values.count else {
                        throw ColumnReaderError.internalError(
                            "Value index \(valueIndex) exceeds array bounds \(values.count)"
                        )
                    }
                    currentList.append(values[valueIndex])
                    valueIndex += 1
                } else if defLevel > repeatedAncestorDefLevel {
                    // List is non-empty but first element is null
                    // defLevel between repeatedAncestorDefLevel and maxDefinitionLevel
                    currentList.append(nil)
                }
                // else: defLevel == repeatedAncestorDefLevel means EMPTY list (no elements)
                // currentList remains empty [], which is correct
                needsAppend = true
            } else {
                // Continuation of current list (repLevel == maxRepetitionLevel)
                // Skip values that belong to empty or null ancestor lists
                if defLevel < repeatedAncestorDefLevel {
                    throw ColumnReaderError.internalError(
                        "Continuation element has def level \(defLevel) < repeatedAncestorDefLevel \(repeatedAncestorDefLevel)"
                    )
                }

                if defLevel >= maxDefinitionLevel {
                    // Non-null element
                    guard valueIndex < values.count else {
                        throw ColumnReaderError.internalError(
                            "Value index \(valueIndex) exceeds array bounds \(values.count)"
                        )
                    }
                    currentList.append(values[valueIndex])
                    valueIndex += 1
                } else {
                    // Null element (def_level between ancestor and max)
                    currentList.append(nil)
                }
            }
        }

        // Add final list if needed
        if needsAppend {
            result.append(currentList)
        }

        // Verify all values were consumed
        if valueIndex != values.count {
            throw ColumnReaderError.internalError(
                "Not all values consumed: \(valueIndex) of \(values.count)"
            )
        }

        return result
    }

    // MARK: - Multi-level Nested Array Reconstruction

    /// Reconstructs multi-level nested arrays from flat value sequence and def/rep levels.
    ///
    /// This method handles arbitrary nesting depths (maxRepetitionLevel > 1) such as:
    /// - Lists of lists: `[[[1, 2], [3]], [[4]]]`
    /// - Lists of structs with repeated fields
    /// - Maps (list of key-value pairs)
    ///
    /// - Parameters:
    ///   - values: Non-null payloads only (no Optional wrapper)
    ///   - definitionLevels: One per logical value (including nulls and empty lists)
    ///   - repetitionLevels: One per logical value (including nulls and empty lists)
    ///   - maxDefinitionLevel: Maximum definition level for this column
    ///   - maxRepetitionLevel: Maximum repetition level for this column (must be > 1)
    ///   - repeatedAncestorDefLevels: Array of definition levels where each repeated ancestor becomes "present"
    ///                                 Length = maxRepetitionLevel, indexed by repetition level
    ///
    /// - Returns: Nested array structure as `Any`. Callers must cast based on maxRepetitionLevel:
    ///   - maxRepLevel=2: `[[[T?]?]?]` (list of optional lists of optional values)
    ///   - maxRepLevel=3: `[[[[T?]?]?]?]` (and so on)
    ///
    /// - Throws: `ColumnReaderError` if levels are invalid or maxRepetitionLevel < 2
    ///
    /// # Example
    ///
    /// ```swift
    /// // Data: [[[1, 2], [3]], [[4]]]  (maxRepLevel=2)
    /// let result = try ArrayReconstructor.reconstructNestedArrays(...)
    /// let typedResult = result as! [[[Int32?]?]?]
    /// // typedResult[0] = [[1, 2], [3]]
    /// // typedResult[1] = [[4]]
    /// ```
    static func reconstructNestedArrays<T>(
        values: [T],
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevels: [Int]
    ) throws -> Any {
        guard maxRepetitionLevel >= 2 else {
            throw ColumnReaderError.internalError(
                "reconstructNestedArrays requires maxRepetitionLevel >= 2, got \(maxRepetitionLevel). " +
                "Use reconstructArrays for single-level (maxRepetitionLevel == 1)"
            )
        }

        guard definitionLevels.count == repetitionLevels.count else {
            throw ColumnReaderError.internalError(
                "Definition and repetition level counts must match " +
                "(\(definitionLevels.count) vs \(repetitionLevels.count))"
            )
        }

        guard repeatedAncestorDefLevels.count == maxRepetitionLevel else {
            throw ColumnReaderError.internalError(
                "repeatedAncestorDefLevels must have length \(maxRepetitionLevel), got \(repeatedAncestorDefLevels.count)"
            )
        }

        // Stack of arrays at each nesting level (0-indexed)
        var listStack: [[Any?]] = Array(repeating: [], count: maxRepetitionLevel)
        var listState: [ListState] = Array(repeating: .uninitialized, count: maxRepetitionLevel)
        var result: [Any?] = []  // Top-level result accumulator
        var valueIndex = 0

        for i in 0..<definitionLevels.count {
            let defLevel = Int(definitionLevels[i])
            let repLevel = Int(repetitionLevels[i])

            guard repLevel <= maxRepetitionLevel else {
                throw ColumnReaderError.internalError(
                    "Repetition level \(repLevel) exceeds column max \(maxRepetitionLevel)"
                )
            }

            // When repLevel < maxRep AND we're not on the first element, collapse levels
            // Arrow approach: ALWAYS append on rep-level transitions
            // Use 0-based levelIndex: 0 = outermost, maxRepetitionLevel-1 = innermost
            if i > 0 && repLevel < maxRepetitionLevel {
                // Close levels from innermost down to repLevel (including repLevel)
                for levelIndex in stride(from: maxRepetitionLevel - 1, through: repLevel, by: -1) {
                    guard levelIndex >= 0 && levelIndex < maxRepetitionLevel else {
                        throw ColumnReaderError.internalError(
                            "levelIndex \(levelIndex) out of bounds [0, \(maxRepetitionLevel))"
                        )
                    }

                    let state = listState[levelIndex]
                    let parentIndex = levelIndex - 1

                    // Determine what to append based on state
                    switch state {
                    case .uninitialized:
                        // List never appeared - skip appending
                        break

                    case .null:
                        // NULL list - append nil
                        let itemToAppend: Any? = nil
                        if parentIndex >= 0 {
                            if listState[parentIndex] != .uninitialized {
                                listStack[parentIndex].append(itemToAppend)
                            }
                        } else {
                            result.append(itemToAppend)
                        }

                    case .empty:
                        // EMPTY list - append []
                        let itemToAppend: Any? = []
                        if parentIndex >= 0 {
                            if listState[parentIndex] != .uninitialized {
                                listStack[parentIndex].append(itemToAppend)
                            }
                        } else {
                            result.append(itemToAppend)
                        }

                    case .populated:
                        // List with content - append the array
                        let itemToAppend: Any? = listStack[levelIndex]
                        if parentIndex >= 0 {
                            if listState[parentIndex] != .uninitialized {
                                listStack[parentIndex].append(itemToAppend)
                            }
                        } else {
                            result.append(itemToAppend)
                        }
                    }

                    // Reset this level for next iteration
                    listStack[levelIndex] = []
                    listState[levelIndex] = .uninitialized
                }
            }

            // Add element at innermost level (0-based: maxRepetitionLevel - 1)
            let innermostIndex = maxRepetitionLevel - 1

            guard innermostIndex >= 0 && innermostIndex < maxRepetitionLevel else {
                throw ColumnReaderError.internalError(
                    "innermostIndex \(innermostIndex) out of bounds [0, \(maxRepetitionLevel))"
                )
            }

            // Update state for all levels based on current defLevel
            for levelIndex in 0..<maxRepetitionLevel {
                let ancestorDef = repeatedAncestorDefLevels[levelIndex]

                if defLevel < ancestorDef {
                    // This level and all deeper levels are NULL
                    if listState[levelIndex] == .uninitialized {
                        listState[levelIndex] = .null
                    }
                    break  // Stop here - deeper levels also NULL
                } else if defLevel == ancestorDef {
                    // List is present but EMPTY (no elements)
                    if listState[levelIndex] == .uninitialized {
                        listState[levelIndex] = .empty
                    }
                } else {
                    // List is present with elements (populated or will be)
                    if listState[levelIndex] == .uninitialized || listState[levelIndex] == .empty {
                        listState[levelIndex] = .populated
                    }
                }
            }

            // Add element to innermost list based on definition level
            if defLevel >= maxDefinitionLevel {
                // Non-null element - add value
                guard valueIndex < values.count else {
                    throw ColumnReaderError.internalError(
                        "Value index \(valueIndex) exceeds array bounds \(values.count)"
                    )
                }
                listStack[innermostIndex].append(values[valueIndex])
                valueIndex += 1
            } else if defLevel > repeatedAncestorDefLevels[innermostIndex] {
                // NULL element (element present but null) - add nil
                listStack[innermostIndex].append(nil)
            }
            // else: defLevel == ancestorDef means EMPTY list (no elements to add)
            // else: defLevel < ancestorDef means NULL list
        }

        // Final collapse: close any remaining open levels
        for levelIndex in stride(from: maxRepetitionLevel - 1, through: 0, by: -1) {
            let state = listState[levelIndex]

            // Determine what to append based on state
            switch state {
            case .uninitialized:
                // List never appeared - skip appending
                break

            case .null:
                // NULL list - append nil at top level only
                if levelIndex == 0 {
                    result.append(nil)
                } else if listState[levelIndex - 1] != .uninitialized {
                    listStack[levelIndex - 1].append(nil)
                }

            case .empty:
                // EMPTY list - append []
                if levelIndex == 0 {
                    result.append([])
                } else if listState[levelIndex - 1] != .uninitialized {
                    listStack[levelIndex - 1].append([])
                }

            case .populated:
                // List with content - append the array
                if levelIndex == 0 {
                    result.append(listStack[levelIndex])
                } else if listState[levelIndex - 1] != .uninitialized {
                    listStack[levelIndex - 1].append(listStack[levelIndex])
                }
            }
        }

        // Verify all values were consumed
        if valueIndex != values.count {
            throw ColumnReaderError.internalError(
                "Not all values consumed: \(valueIndex) of \(values.count)"
            )
        }

        return result
    }
}
