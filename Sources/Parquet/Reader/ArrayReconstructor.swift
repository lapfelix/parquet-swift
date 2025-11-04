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

    /// Output structure for validity bitmap reconstruction, matching Arrow C++'s ValidityBitmapInputOutput
    ///
    /// Used by DefRepLevelsToListInfo to track:
    /// - Number of values/lists processed
    /// - Null count
    /// - Validity bitmap (which lists/values are present vs NULL)
    public struct ValidityBitmapOutput {
        /// Maximum number of values/lists expected (upper bound for safety)
        /// Set this to prevent unbounded allocation from malformed data
        public var valuesReadUpperBound: Int?

        /// Number of values/lists successfully read and added to output
        public var valuesRead: Int = 0

        /// Number of NULL values/lists encountered
        public var nullCount: Int = 0

        /// Validity bitmap: true = present (possibly empty), false = NULL
        /// For lists: each entry represents one list
        /// For structs: each entry represents one struct instance
        public var validBits: [Bool] = []

        public init(valuesReadUpperBound: Int? = nil) {
            self.valuesReadUpperBound = valuesReadUpperBound
        }
    }

    // MARK: - Core Algorithm (DefRepLevelsToListInfo)

    /// Reconstructs list offsets and validity bitmap from definition/repetition levels.
    ///
    /// This is the core algorithm ported from Apache Arrow C++'s `DefRepLevelsToListInfo`.
    /// It filters and processes level data to build:
    /// - Offsets array defining list boundaries (optional, nil for structs)
    /// - Validity bitmap tracking NULL vs present lists
    ///
    /// **Limitation**: Currently only supports Int32 offsets (up to ~2 billion elements per list).
    /// Arrow C++ templates on int32_t vs int64_t for large lists (LargeList/LargeMap).
    /// Future enhancement: add Int64 variant for large list support.
    ///
    /// **Key Features**:
    /// - **Filters by `rep > repLevel`**: Skips nested children (each reader processes only its level)
    /// - **Filters by `def < repeatedAncestorDefLevel`**: Skips continuation values from NULL ancestor lists
    ///   - Applied ONLY to continuation entries (rep == repLevel)
    ///   - For new lists (rep < repLevel), ALWAYS creates offset and validity entries
    ///   - Prevents incrementing offsets for values belonging to NULL ancestors
    /// - **Distinguishes NULL/empty/present lists**:
    ///   - `def < repeatedAncestorDefLevel`: NULL list (validity=false, offset not incremented)
    ///   - `def == repeatedAncestorDefLevel`: Empty list (validity=true, offset not incremented)
    ///   - `def > repeatedAncestorDefLevel`: List with content (validity=true, offset incremented)
    /// - Supports multi-level nesting through level filtering
    ///
    /// - Parameters:
    ///   - definitionLevels: Definition levels from Parquet column
    ///   - repetitionLevels: Repetition levels from Parquet column
    ///   - levelInfo: Level metadata (defLevel, repLevel, repeatedAncestorDefLevel)
    ///   - output: Output structure to populate with validity bitmap
    ///   - offsets: Optional offsets array. If provided, will be populated with list boundaries.
    ///              Pass nil for structs that don't need offset tracking.
    ///
    /// - Throws: `ColumnReaderError` if level data is invalid
    ///
    /// # Algorithm
    ///
    /// For each (def, rep) pair:
    /// 1. **Filter nested children**: Skip if `rep > repLevel` (values from child structures)
    /// 2. **If rep == repLevel** (continuation): Add element to current list
    ///    - Filter: Skip if `def < repeatedAncestorDefLevel` (belongs to NULL ancestor)
    ///    - Increment offset to add one more element
    /// 3. **If rep < repLevel** (new list): Start new list
    ///    - Always create offset and validity entries (even for NULL lists)
    ///    - Offset increment: Only if `def > repeatedAncestorDefLevel` (list has content)
    ///    - Validity: true if `def >= repeatedAncestorDefLevel`, false otherwise
    ///
    /// # Example
    ///
    /// ```swift
    /// // Data: [[1, 2], None, [3]]
    /// let defLevels: [UInt16] = [3, 3, 0, 3]  // 0=NULL list, 3=present value
    /// let repLevels: [UInt16] = [0, 1, 0, 0]  // 0=new list, 1=continuation
    /// let levelInfo = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 1)
    ///
    /// var output = ValidityBitmapOutput()
    /// var offsets: [Int32] = [0]
    /// try ArrayReconstructor.defRepLevelsToListInfo(
    ///     definitionLevels: defLevels,
    ///     repetitionLevels: repLevels,
    ///     levelInfo: levelInfo,
    ///     output: &output,
    ///     offsets: &offsets
    /// )
    /// // offsets = [0, 2, 2, 3]  (2 values in first list, 0 in second [NULL], 1 in third)
    /// // output.validBits = [true, false, true]  (first present, second NULL, third present)
    /// // output.valuesRead = 3  (three lists)
    /// // output.nullCount = 1  (one NULL list)
    /// ```
    public static func defRepLevelsToListInfo(
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        levelInfo: LevelInfo,
        output: inout ValidityBitmapOutput,
        offsets: inout [Int32]?
    ) throws {
        guard definitionLevels.count == repetitionLevels.count else {
            throw ColumnReaderError.internalError(
                "Definition and repetition level counts must match " +
                "(\(definitionLevels.count) vs \(repetitionLevels.count))"
            )
        }

        for i in 0..<definitionLevels.count {
            let defLevel = Int(definitionLevels[i])
            let repLevel = Int(repetitionLevels[i])

            // CRITICAL FILTER: Skip nested children (values with higher rep level)
            // For example, when reading list<map>, skip map entries (they have higher rep_level)
            // Each reader processes ONLY its own level
            if repLevel > levelInfo.repLevel {
                continue
            }

            if repLevel == levelInfo.repLevel {
                // Continuation of existing list at THIS level

                // Filter values belonging to ancestor NULL/empty lists
                // This check only applies to continuation entries, not new lists
                if defLevel < levelInfo.repeatedAncestorDefLevel {
                    // This value belongs to a NULL/empty ancestor list, skip it
                    continue
                }

                // Increment the current offset to add one more element
                if offsets != nil {
                    let lastIndex = offsets!.count - 1

                    // Guardrail: Check for Int32 overflow BEFORE incrementing
                    guard offsets![lastIndex] < Int32.max else {
                        throw ColumnReaderError.internalError(
                            "Offset overflow: list offsets would exceed Int32.max (\(Int32.max)). " +
                            "Consider using a file format with 64-bit offsets for large lists."
                        )
                    }

                    offsets![lastIndex] += 1
                }
            } else {
                // repLevel < levelInfo.repLevel
                // Start of NEW list at THIS level

                // Guardrail: Check upper bound before allocating
                if let upperBound = output.valuesReadUpperBound, output.valuesRead >= upperBound {
                    throw ColumnReaderError.internalError(
                        "Malformed data: attempting to read more lists (\(output.valuesRead + 1)) " +
                        "than upper bound (\(upperBound))"
                    )
                }

                // Append new offset
                if offsets != nil {
                    let previousOffset = offsets!.last!
                    var newOffset = previousOffset

                    // Check if this list has content (element present OR element NULL):
                    // - def > repeatedAncestorDefLevel: list has at least one element (present or NULL)
                    // - def == repeatedAncestorDefLevel: empty list (present, zero elements)
                    // - def < repeatedAncestorDefLevel: NULL list (no elements)
                    //
                    // This correctly handles lists starting with NULL:
                    // For list<int32?> where repeatedAncestorDefLevel=1, defLevel=3:
                    // - [[NULL, 1]]: first record has def=2, rep=0 → def > 1, so increment
                    // - [[1]]: first record has def=3, rep=0 → def > 1, so increment
                    // - [[]]: first record has def=1, rep=0 → def == 1, don't increment
                    // - [NULL]: first record has def=0, rep=0 → def < 1, don't increment
                    if defLevel > levelInfo.repeatedAncestorDefLevel {
                        // Guardrail: Check for Int32 overflow BEFORE incrementing
                        guard previousOffset < Int32.max else {
                            throw ColumnReaderError.internalError(
                                "Offset overflow: list offsets would exceed Int32.max (\(Int32.max)). " +
                                "Consider using a file format with 64-bit offsets for large lists."
                            )
                        }
                        newOffset += 1  // Add first element slot (present value or NULL element)
                    }

                    offsets!.append(newOffset)
                }

                // Update validity bitmap
                // Use repeatedAncestorDefLevel as the threshold:
                // - def >= repeatedAncestorDefLevel: List is present (possibly empty or with content)
                // - def < repeatedAncestorDefLevel: List is NULL
                //
                // For list<int> where repeatedAncestorDefLevel=1:
                // - def=0 < 1: NULL list
                // - def=1 >= 1: Empty list (present, zero elements)
                // - def=2+ >= 1: List with content (NULL or present elements)
                if defLevel >= levelInfo.repeatedAncestorDefLevel {
                    output.validBits.append(true)  // Present (possibly empty)
                } else {
                    output.validBits.append(false)  // NULL
                    output.nullCount += 1
                }

                output.valuesRead += 1
            }
        }
    }

    /// Reconstructs validity bitmap for structs containing repeated children (maps/lists).
    ///
    /// This is a wrapper around `defRepLevelsToListInfo` specifically for struct validity bitmaps
    /// when the struct contains repeated descendants (e.g., `struct { map }` or `struct { list }`).
    ///
    /// Ported from Apache Arrow C++ `DefRepLevelsToBitmap` (level_conversion.cc:168-177).
    ///
    /// ## Why This Function Exists
    ///
    /// When a struct contains lists or maps, we need **both** definition and repetition levels
    /// to reconstruct the struct's validity bitmap correctly. This is because:
    ///
    /// - Child lists/maps contribute repetition levels
    /// - We need to filter out values that belong to NULL ancestor structs
    /// - Simple definition-only approach (`defLevelsToBitmap`) doesn't work
    ///
    /// ## Key Difference from defRepLevelsToListInfo
    ///
    /// This function **bumps levels by 1** before calling `defRepLevelsToListInfo` because:
    /// - `defRepLevelsToListInfo` assumes it's processing the list/map itself
    /// - Here we're processing the **parent struct** that contains the list/map
    /// - Bumping levels adjusts the thresholds to the parent's perspective
    ///
    /// ## Example
    ///
    /// ```swift
    /// // Schema: struct { list<int32> items; }
    /// // Struct validity needs both def and rep levels from the child list column
    ///
    /// let levelInfo = LevelInfo(defLevel: 2, repLevel: 1, repeatedAncestorDefLevel: 1)
    /// var output = ArrayReconstructor.ValidityBitmapOutput()
    ///
    /// try ArrayReconstructor.defRepLevelsToBitmap(
    ///     definitionLevels: childDefLevels,
    ///     repetitionLevels: childRepLevels,
    ///     levelInfo: levelInfo,
    ///     output: &output
    /// )
    /// // output.validBits contains struct validity (not list validity)
    /// ```
    ///
    /// - Parameters:
    ///   - definitionLevels: Definition levels from child column
    ///   - repetitionLevels: Repetition levels from child column
    ///   - levelInfo: Level metadata for the **struct** (not the child)
    ///   - output: Output structure to populate with validity bitmap
    ///
    /// - Throws: `ColumnReaderError` if level data is invalid
    ///
    /// ## Implementation Note
    ///
    /// This function does NOT need an offsets parameter because structs don't have offsets,
    /// only validity bitmaps. We pass `offsets=nil` to `defRepLevelsToListInfo`.
    public static func defRepLevelsToBitmap(
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        levelInfo: LevelInfo,
        output: inout ValidityBitmapOutput
    ) throws {
        // CRITICAL: Bump levels by 1 because defRepLevelsToListInfo assumes it's
        // processing the list/map itself, but here we're processing the parent struct.
        //
        // Example: For struct { list<int32> }, the list column has:
        //   - defLevel=3, repLevel=1, repeatedAncestorDefLevel=1
        //
        // But for the struct validity, we need:
        //   - defLevel=2, repLevel=0, repeatedAncestorDefLevel=0
        //
        // So we bump the incoming levelInfo (which is for the struct) to match
        // what defRepLevelsToListInfo expects.
        let adjustedLevelInfo = LevelInfo(
            defLevel: levelInfo.defLevel + 1,
            repLevel: levelInfo.repLevel + 1,
            repeatedAncestorDefLevel: levelInfo.repeatedAncestorDefLevel + 1
        )

        // Call defRepLevelsToListInfo with offsets=nil (structs don't need offsets)
        var nilOffsets: [Int32]? = nil
        try defRepLevelsToListInfo(
            definitionLevels: definitionLevels,
            repetitionLevels: repetitionLevels,
            levelInfo: adjustedLevelInfo,
            output: &output,
            offsets: &nilOffsets
        )
    }

    // MARK: - LevelInfo-based API (Preferred)

    /// Reconstructs arrays from flat value sequence and def/rep levels using LevelInfo.
    ///
    /// **Preferred API**: Use this method for new code. It matches the Arrow C++ approach.
    ///
    /// This method validates that LevelInfo parameters match the actual level data and
    /// enforces single-source-of-truth for level thresholds.
    ///
    /// - Parameters:
    ///   - values: Non-null payloads only (no Optional wrapper)
    ///   - definitionLevels: One per logical value (including nulls and empty lists)
    ///   - repetitionLevels: One per logical value (including nulls and empty lists)
    ///   - levelInfo: Level metadata for reconstruction (MUST match the column's actual levels)
    ///
    /// - Returns: Array of arrays where:
    ///   - Outer nil represents NULL list (list not present)
    ///   - Inner nil represents NULL element (element not present)
    ///   - Empty array [] represents empty list (list present, zero elements)
    ///
    /// - Throws: `ColumnReaderError` if levels are invalid or don't match levelInfo
    static func reconstructArrays<T>(
        values: [T],
        definitionLevels: [UInt16],
        repetitionLevels: [UInt16],
        levelInfo: LevelInfo
    ) throws -> [[T?]?] {
        // Validate levelInfo consistency with actual level data
        // This catches mismatches early rather than silently using wrong thresholds
        let maxDefInData = definitionLevels.max() ?? 0
        let maxRepInData = repetitionLevels.max() ?? 0

        if Int(maxDefInData) > levelInfo.defLevel {
            throw ColumnReaderError.internalError(
                "Definition levels contain \(maxDefInData) which exceeds levelInfo.defLevel=\(levelInfo.defLevel)"
            )
        }

        if Int(maxRepInData) > levelInfo.repLevel {
            throw ColumnReaderError.internalError(
                "Repetition levels contain \(maxRepInData) which exceeds levelInfo.repLevel=\(levelInfo.repLevel)"
            )
        }

        // Use levelInfo as single source of truth for thresholds
        return try reconstructArrays(
            values: values,
            definitionLevels: definitionLevels,
            repetitionLevels: repetitionLevels,
            maxDefinitionLevel: levelInfo.defLevel,
            maxRepetitionLevel: levelInfo.repLevel,
            repeatedAncestorDefLevel: levelInfo.repeatedAncestorDefLevel
        )
    }

    // MARK: - Legacy API (Backwards Compatibility)

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
