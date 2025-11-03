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
    /// - Returns: Array of arrays where inner nil represents NULL elements
    ///
    /// - Throws: `ColumnReaderError` if levels are invalid
    static func reconstructArrays<T>(
        values: [T],
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int
    ) throws -> [[T?]] {
        guard definitionLevels.count == repetitionLevels.count else {
            throw ColumnReaderError.internalError(
                "Definition and repetition level counts must match " +
                "(\(definitionLevels.count) vs \(repetitionLevels.count))"
            )
        }

        var result: [[T?]] = []
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

                // Check if this is an empty list (def < repeatedAncestorDefLevel)
                // Only applies when repeatedAncestorDefLevel > 0 (i.e., there are optional ancestors)
                if repeatedAncestorDefLevel > 0 && defLevel < repeatedAncestorDefLevel {
                    // Empty list - append it immediately
                    result.append([])
                    needsAppend = false
                    continue
                }

                // Add first element if present
                if defLevel >= maxDefinitionLevel {
                    // Non-null first element
                    guard valueIndex < values.count else {
                        throw ColumnReaderError.internalError(
                            "Value index \(valueIndex) exceeds array bounds \(values.count)"
                        )
                    }
                    currentList.append(values[valueIndex])
                    valueIndex += 1
                } else if defLevel >= repeatedAncestorDefLevel {
                    // List is non-empty but first element is null
                    currentList.append(nil)
                }
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
}
