// DoubleColumnReader - Read Double column values
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Reads Double values from a Parquet column chunk.
///
/// Supports both PLAIN and dictionary encoding, with nullable columns.
///
/// # Phase 3 Features
///
/// - ✅ Required columns (non-nullable)
/// - ✅ Nullable columns (definition levels)
/// - ✅ PLAIN and dictionary encoding
///
/// Returns `[Double?]` where `nil` represents NULL values in nullable columns.
/// For required columns, all values will be non-nil.
///
/// # Usage
///
/// ```swift
/// let reader = try DoubleColumnReader(
///     file: file,
///     columnMetadata: metadata,
///     codec: codec,
///     column: column
/// )
///
/// let values = try reader.readBatch(count: 1000)  // Returns [Double?]
/// ```
public final class DoubleColumnReader {
    /// Column schema (for level information)
    private let column: Column

    /// Page reader for this column
    private let pageReader: PageReader

    /// Dictionary (if column uses dictionary encoding)
    private let dictionary: Dictionary<Double>?

    /// Maximum definition level for this column (from schema)
    private let maxDefinitionLevel: Int

    /// Maximum repetition level for this column (from schema)
    private let maxRepetitionLevel: Int

    /// Level decoder for definition/repetition levels
    private let levelDecoder: LevelDecoder

    /// Current page being read
    private var currentPage: DataPage?

    /// Decoder for current page (PLAIN encoding)
    private var currentDecoder: PlainDecoder<Double>?

    /// Decoded indices for current page (dictionary encoding)
    private var currentIndices: [UInt32]?

    /// Definition levels for current page (nullable columns only)
    private var currentDefinitionLevels: [UInt16]?

    /// Repetition levels for current page (repeated columns only)
    private var currentRepetitionLevels: [UInt16]?

    /// Number of values read from current page (includes nulls)
    private var valuesReadFromPage: Int = 0

    /// Number of non-null values read from current page (offset into data stream)
    private var nonNullValuesRead: Int = 0

    /// Initialize an Double column reader
    ///
    /// - Parameters:
    ///   - file: The file to read from
    ///   - columnMetadata: Metadata for the column chunk
    ///   - codec: Compression codec
    ///   - column: Column schema (for level information)
    public init(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec,
        column: Column
    ) throws {
        self.column = column
        self.pageReader = try PageReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec
        )
        self.maxDefinitionLevel = column.maxDefinitionLevel
        self.maxRepetitionLevel = column.maxRepetitionLevel
        self.levelDecoder = LevelDecoder()

        // Read dictionary page if present
        if let dictPage = try pageReader.readDictionaryPage() {
            self.dictionary = try Dictionary.double(page: dictPage)
        } else {
            self.dictionary = nil
        }
    }

    // MARK: - Batch Reading

    /// Read a batch of values
    ///
    /// - Parameter count: Maximum number of values to read
    /// - Returns: Array of optional values (nil for NULL values in nullable columns)
    /// - Throws: `ColumnReaderError` if reading fails or column is repeated
    public func readBatch(count: Int) throws -> [Double?] {
        guard maxRepetitionLevel == 0 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is repeated (maxRepetitionLevel > 0). Use readAllRepeated() instead."
            )
        }

        var values: [Double?] = []
        values.reserveCapacity(count)

        var remaining = count

        while remaining > 0 {
            // Load next page if needed
            guard try loadPageIfNeeded() else {
                // No more pages
                break
            }

            // Read from current page
            let toRead = min(remaining, remainingInPage())

            for _ in 0..<toRead {
                // Check definition level to determine if value is null
                if let defLevels = currentDefinitionLevels {
                    let defLevel = defLevels[valuesReadFromPage]
                    if defLevel < maxDefinitionLevel {
                        // Value is null
                        values.append(nil)
                        valuesReadFromPage += 1
                        continue
                    }
                }

                // Value is present - read from data stream
                let value: Double
                if let decoder = currentDecoder {
                    // PLAIN encoding
                    value = try decoder.decodeOne()
                } else if let indices = currentIndices, let dict = dictionary {
                    // Dictionary encoding
                    value = try dict.value(at: indices[nonNullValuesRead])
                } else {
                    throw ColumnReaderError.internalError("No decoder or indices available")
                }

                values.append(value)
                valuesReadFromPage += 1
                nonNullValuesRead += 1
            }

            remaining -= toRead

            // Check if page is exhausted
            if valuesReadFromPage >= currentPage!.numValues {
                currentPage = nil
                currentDecoder = nil
                currentIndices = nil
                currentDefinitionLevels = nil
                currentRepetitionLevels = nil
                valuesReadFromPage = 0
                nonNullValuesRead = 0
            }
        }

        return values
    }

    /// Read a single value
    ///
    /// - Returns: Double optional: outer nil = no more values, inner nil = NULL value
    /// - Throws: `ColumnReaderError` if reading fails or column is repeated
    public func readOne() throws -> Double?? {
        guard maxRepetitionLevel == 0 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is repeated (maxRepetitionLevel > 0). Use readAllRepeated() instead."
            )
        }

        guard try loadPageIfNeeded() else {
            return nil  // No more values
        }

        // Check definition level to determine if value is null
        if let defLevels = currentDefinitionLevels {
            let defLevel = defLevels[valuesReadFromPage]
            if defLevel < maxDefinitionLevel {
                // Value is null
                valuesReadFromPage += 1

                // Check if page is exhausted
                if valuesReadFromPage >= currentPage!.numValues {
                    currentPage = nil
                    currentDecoder = nil
                    currentIndices = nil
                    currentDefinitionLevels = nil
                    currentRepetitionLevels = nil
                    valuesReadFromPage = 0
                    nonNullValuesRead = 0
                }

                return .some(nil)  // NULL value
            }
        }

        // Value is present - read from data stream
        let value: Double
        if let decoder = currentDecoder {
            // PLAIN encoding
            value = try decoder.decodeOne()
        } else if let indices = currentIndices, let dict = dictionary {
            // Dictionary encoding
            value = try dict.value(at: indices[nonNullValuesRead])
        } else {
            throw ColumnReaderError.internalError("No decoder or indices available")
        }

        valuesReadFromPage += 1
        nonNullValuesRead += 1

        // Check if page is exhausted
        if valuesReadFromPage >= currentPage!.numValues {
            currentPage = nil
            currentDecoder = nil
            currentIndices = nil
            currentDefinitionLevels = nil
            currentRepetitionLevels = nil
            valuesReadFromPage = 0
            nonNullValuesRead = 0
        }

        return .some(value)  // Present value
    }

    /// Read all remaining values
    ///
    /// - Returns: Array of all remaining optional values (nil for NULLs)
    /// - Throws: `ColumnReaderError` if reading fails or column is repeated
    public func readAll() throws -> [Double?] {
        guard maxRepetitionLevel == 0 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is repeated (maxRepetitionLevel > 0). Use readAllRepeated() instead."
            )
        }

        var values: [Double?] = []

        while let value = try readOne() {
            values.append(value)
        }

        return values
    }

    // MARK: - Internal API for Struct Reading

    /// Read all values and definition levels (internal API for struct reading)
    internal func readAllWithLevels() throws -> (values: [Double?], definitionLevels: [UInt16]) {
        var values: [Double?] = []
        var defLevels: [UInt16] = []

        // Read through all pages
        while try loadPageIfNeeded() {
            let numValuesInPage = currentPage!.numValues

            // Process each value in the page
            for _ in 0..<numValuesInPage {
                // Get definition level for this value position
                let defLevel: UInt16
                if let currentDefLevels = currentDefinitionLevels {
                    defLevel = currentDefLevels[valuesReadFromPage]
                } else {
                    defLevel = UInt16(maxDefinitionLevel)
                }

                defLevels.append(defLevel)

                if defLevel < maxDefinitionLevel {
                    values.append(nil)
                    valuesReadFromPage += 1
                } else {
                    let value: Double
                    if let decoder = currentDecoder {
                        value = try decoder.decodeOne()
                    } else if let indices = currentIndices, let dict = dictionary {
                        value = try dict.value(at: indices[nonNullValuesRead])
                    } else {
                        throw ColumnReaderError.internalError("No decoder or indices available")
                    }

                    values.append(value)
                    valuesReadFromPage += 1
                    nonNullValuesRead += 1
                }
            }

            // Page exhausted - reset for next page
            currentPage = nil
            currentDecoder = nil
            currentIndices = nil
            currentDefinitionLevels = nil
            currentRepetitionLevels = nil
            valuesReadFromPage = 0
            nonNullValuesRead = 0
        }

        return (values, defLevels)
    }

    /// Read all remaining values for a repeated column (returns nested arrays)
    ///
    /// This method is for columns with `maxRepetitionLevel > 0` (repeated fields).
    /// It reconstructs arrays from the flat value sequence using repetition levels.
    ///
    /// - Returns: Array of arrays where:
    ///   - Outer nil represents NULL list (list not present)
    ///   - Inner nil represents NULL element (element not present)
    ///   - Empty array [] represents empty list (list present, zero elements)
    /// - Throws: `ColumnReaderError` if column is not repeated or reading fails
    ///
    /// # Example
    ///
    /// For schema: `repeated double numbers;`
    /// Data: [[1.5, 2.5], None, [], [3.5]]
    /// Returns: [[1.5, 2.5], nil, [], [3.5]]
    public func readAllRepeated() throws -> [[Double?]?] {
        guard maxRepetitionLevel > 0 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is not repeated (maxRepetitionLevel = 0). Use readAll() instead."
            )
        }

        guard let repeatedAncestorDefLevel = column.repeatedAncestorDefLevel else {
            throw ColumnReaderError.internalError(
                "Cannot compute repeatedAncestorDefLevel for repeated column"
            )
        }

        // Collect all values and levels
        var allValues: [Double] = []  // Non-null values only
        var allDefLevels: [UInt16] = []
        var allRepLevels: [UInt16] = []

        // Read through all pages
        while try loadPageIfNeeded() {
            guard let defLevels = currentDefinitionLevels,
                  let repLevels = currentRepetitionLevels else {
                throw ColumnReaderError.internalError(
                    "Repeated column must have definition and repetition levels"
                )
            }

            let numValuesInPage = currentPage!.numValues

            // Collect levels for this page
            for i in 0..<numValuesInPage {
                let defLevel = defLevels[i]
                let repLevel = repLevels[i]

                allDefLevels.append(defLevel)
                allRepLevels.append(repLevel)

                // Collect value only if non-null
                if defLevel >= maxDefinitionLevel {
                    let value: Double
                    if let decoder = currentDecoder {
                        // PLAIN encoding
                        value = try decoder.decodeOne()
                    } else if let indices = currentIndices, let dict = dictionary {
                        // Dictionary encoding
                        value = try dict.value(at: indices[nonNullValuesRead])
                    } else {
                        throw ColumnReaderError.internalError("No decoder or indices available")
                    }
                    allValues.append(value)
                    nonNullValuesRead += 1
                }

                valuesReadFromPage += 1
            }

            // Mark page as exhausted
            currentPage = nil
            currentDecoder = nil
            currentIndices = nil
            currentDefinitionLevels = nil
            currentRepetitionLevels = nil
            valuesReadFromPage = 0
            nonNullValuesRead = 0
        }

        // Reconstruct arrays
        return try ArrayReconstructor.reconstructArrays(
            values: allValues,
            definitionLevels: allDefLevels,
            repetitionLevels: allRepLevels,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel
        )
    }

    /// Read all values from a multi-level repeated column and reconstruct nested arrays.
    ///
    /// This method handles columns with `maxRepetitionLevel > 1`, such as lists of lists.
    /// For single-level repeated columns (`maxRepetitionLevel == 1`), use `readAllRepeated()`.
    /// For flat columns (`maxRepetitionLevel == 0`), use `readAll()`.
    ///
    /// - Returns: Nested array structure as `Any`. Cast based on maxRepetitionLevel:
    ///   - maxRepLevel=2: `[[[Double?]?]?]` (list of optional lists of optional values)
    ///   - maxRepLevel=3: `[[[[Double?]?]?]?]` (and so on)
    ///
    /// - Throws: `ColumnReaderError` if the column is not multi-level repeated or if reading fails
    public func readAllNested() throws -> Any {
        let maxRep = maxRepetitionLevel
        let maxDef = maxDefinitionLevel

        guard maxRep > 1 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is not multi-level repeated (maxRepetitionLevel = \(maxRep)). " +
                "Use readAllRepeated() for single-level or readAll() for flat columns."
            )
        }

        guard let repeatedAncestorDefLevels = column.repeatedAncestorDefLevels else {
            throw ColumnReaderError.internalError(
                "Cannot compute repeatedAncestorDefLevels for multi-level repeated column"
            )
        }

        // Collect all values and levels
        var allValues: [Double] = []
        var allDefLevels: [UInt16] = []
        var allRepLevels: [UInt16] = []

        // Read through all pages
        while try loadPageIfNeeded() {
            guard let defLevels = currentDefinitionLevels,
                  let repLevels = currentRepetitionLevels else {
                throw ColumnReaderError.internalError(
                    "Multi-level repeated column must have definition and repetition levels"
                )
            }

            let numValuesInPage = currentPage!.numValues

            // Collect levels for this page
            for i in 0..<numValuesInPage {
                let defLevel = defLevels[i]
                let repLevel = repLevels[i]

                allDefLevels.append(defLevel)
                allRepLevels.append(repLevel)

                // Collect value only if non-null
                if defLevel >= maxDefinitionLevel {
                    let value: Double
                    if let decoder = currentDecoder {
                        value = try decoder.decodeOne()
                    } else if let indices = currentIndices, let dict = dictionary {
                        value = try dict.value(at: indices[nonNullValuesRead])
                    } else {
                        throw ColumnReaderError.internalError("No decoder or indices available")
                    }
                    allValues.append(value)
                    nonNullValuesRead += 1
                }

                valuesReadFromPage += 1
            }

            // Mark page as exhausted
            currentPage = nil
            currentDecoder = nil
            currentIndices = nil
            currentDefinitionLevels = nil
            currentRepetitionLevels = nil
            valuesReadFromPage = 0
            nonNullValuesRead = 0
        }

        // Reconstruct nested arrays
        return try ArrayReconstructor.reconstructNestedArrays(
            values: allValues,
            definitionLevels: allDefLevels,
            repetitionLevels: allRepLevels,
            maxDefinitionLevel: maxDef,
            maxRepetitionLevel: maxRep,
            repeatedAncestorDefLevels: repeatedAncestorDefLevels
        )
    }

    // MARK: - Private Helpers

    /// Load next page if current page is exhausted
    ///
    /// - Returns: true if a page is available, false if no more pages
    /// - Throws: `ColumnReaderError` if loading fails
    private func loadPageIfNeeded() throws -> Bool {
        // Return true if current page still has values
        if currentPage != nil && valuesReadFromPage < currentPage!.numValues {
            return true
        }

        // Load next page
        guard let page = try pageReader.readDataPage() else {
            return false // No more pages
        }

        // PHASE 3+: Support for nullable and repeated columns
        // Decode level streams from page data
        // Page format: [Repetition Levels] [Definition Levels] [Values]
        var dataOffset = 0
        var repetitionLevels: [UInt16]?
        var definitionLevels: [UInt16]?

        // 1. Decode repetition levels (if present)
        if maxRepetitionLevel > 0 {
            // Repetition levels come FIRST in the page
            // Format: <4-byte length> <RLE-encoded levels>
            guard page.data.count >= 4 else {
                throw ColumnReaderError.internalError("Page data too short for repetition levels")
            }

            // Read 4-byte length prefix (little-endian)
            let levelDataLength = Int(UInt32(page.data[dataOffset])
                | (UInt32(page.data[dataOffset + 1]) << 8)
                | (UInt32(page.data[dataOffset + 2]) << 16)
                | (UInt32(page.data[dataOffset + 3]) << 24))

            let levelStreamLength = 4 + levelDataLength
            guard page.data.count >= dataOffset + levelStreamLength else {
                throw ColumnReaderError.internalError(
                    "Page data too short: expected \(levelStreamLength) bytes for repetition levels at offset \(dataOffset)"
                )
            }

            // Extract level stream
            let levelData = page.data.subdata(in: dataOffset..<(dataOffset + levelStreamLength))

            // Decode repetition levels
            repetitionLevels = try levelDecoder.decodeLevels(
                from: levelData,
                numValues: page.numValues,
                maxLevel: maxRepetitionLevel
            )

            // Advance offset past repetition levels
            dataOffset += levelStreamLength
        }

        // 2. Decode definition levels (if present)
        if maxDefinitionLevel > 0 {
            // Definition levels come AFTER repetition levels
            // Format: <4-byte length> <RLE-encoded levels>
            guard page.data.count >= dataOffset + 4 else {
                throw ColumnReaderError.internalError("Page data too short for definition levels")
            }

            // Read 4-byte length prefix (little-endian)
            let levelDataLength = Int(UInt32(page.data[dataOffset])
                | (UInt32(page.data[dataOffset + 1]) << 8)
                | (UInt32(page.data[dataOffset + 2]) << 16)
                | (UInt32(page.data[dataOffset + 3]) << 24))

            let levelStreamLength = 4 + levelDataLength
            guard page.data.count >= dataOffset + levelStreamLength else {
                throw ColumnReaderError.internalError(
                    "Page data too short: expected \(levelStreamLength) bytes for definition levels at offset \(dataOffset)"
                )
            }

            // Extract level stream
            let levelData = page.data.subdata(in: dataOffset..<(dataOffset + levelStreamLength))

            // Decode definition levels
            definitionLevels = try levelDecoder.decodeLevels(
                from: levelData,
                numValues: page.numValues,
                maxLevel: maxDefinitionLevel
            )

            // Advance offset past definition levels
            dataOffset += levelStreamLength
        }

        // Extract data portion (after level streams)
        let dataSlice = page.data.subdata(in: dataOffset..<page.data.count)

        // Count non-null values (for sizing the data stream)
        let numNonNullValues: Int
        if let defLevels = definitionLevels {
            numNonNullValues = defLevels.filter { $0 == maxDefinitionLevel }.count
        } else {
            numNonNullValues = page.numValues
        }

        // Decode based on encoding type
        switch page.encoding {
        case .plain:
            // PLAIN encoding: use PlainDecoder on data portion
            let decoder = PlainDecoder<Double>(data: dataSlice)
            currentPage = page
            currentDecoder = decoder
            currentIndices = nil
            currentDefinitionLevels = definitionLevels
            currentRepetitionLevels = repetitionLevels
            valuesReadFromPage = 0
            nonNullValuesRead = 0

        case .rleDictionary, .plainDictionary:
            // Dictionary encoding: decode indices with RLE decoder
            guard dictionary != nil else {
                throw ColumnReaderError.missingDictionary(
                    "Page uses \(page.encoding) encoding but no dictionary was found"
                )
            }

            // Decode dictionary indices from data portion
            let rleDecoder = RLEDecoder()
            let indices = try rleDecoder.decodeIndices(from: dataSlice, numValues: numNonNullValues)

            currentPage = page
            currentDecoder = nil
            currentIndices = indices
            currentDefinitionLevels = definitionLevels
            currentRepetitionLevels = repetitionLevels
            valuesReadFromPage = 0
            nonNullValuesRead = 0

        default:
            throw ColumnReaderError.unsupportedEncoding(
                "Unsupported encoding \(page.encoding) for Double column"
            )
        }

        return true
    }

    /// Get remaining values in current page
    private func remainingInPage() -> Int {
        guard let page = currentPage else { return 0 }
        return page.numValues - valuesReadFromPage
    }
}
