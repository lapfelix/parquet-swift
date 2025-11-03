// StringColumnReader - Read String column values
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Reads String values from a Parquet column chunk.
///
/// Supports both PLAIN and dictionary encoding, with nullable columns.
///
/// # Phase 3 Features
///
/// - ✅ Required columns (non-nullable)
/// - ✅ Nullable columns (definition levels)
/// - ✅ PLAIN and dictionary encoding
///
/// Returns `[String?]` where `nil` represents NULL values in nullable columns.
/// For required columns, all values will be non-nil.
///
/// # Usage
///
/// ```swift
/// let reader = try StringColumnReader(
///     file: file,
///     columnMetadata: metadata,
///     codec: codec,
///     column: column
/// )
///
/// let values = try reader.readBatch(count: 1000)  // Returns [String?]
/// ```
public final class StringColumnReader {
    /// Column schema (for level information)
    private let column: Column

    /// Page reader for this column
    private let pageReader: PageReader

    /// Dictionary (if column uses dictionary encoding)
    private let dictionary: Dictionary<String>?

    /// Maximum definition level for this column (from schema)
    private let maxDefinitionLevel: Int

    /// Maximum repetition level for this column (from schema)
    private let maxRepetitionLevel: Int

    /// Level decoder for definition/repetition levels
    private let levelDecoder: LevelDecoder

    /// Current page being read
    private var currentPage: DataPage?

    /// Decoder for current page (PLAIN encoding)
    private var currentDecoder: PlainDecoder<String>?

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

    /// Initialize an String column reader
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
            self.dictionary = try Dictionary.string(page: dictPage)
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
    public func readBatch(count: Int) throws -> [String?] {
        guard maxRepetitionLevel == 0 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is repeated (maxRepetitionLevel > 0). Use readAllRepeated() instead."
            )
        }

        var values: [String?] = []
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
                let value: String
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
    public func readOne() throws -> String?? {
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
        let value: String
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
    public func readAll() throws -> [String?] {
        guard maxRepetitionLevel == 0 else {
            throw ColumnReaderError.unsupportedFeature(
                "Column is repeated (maxRepetitionLevel > 0). Use readAllRepeated() instead."
            )
        }

        var values: [String?] = []

        while let value = try readOne() {
            values.append(value)
        }

        return values
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
    /// For schema: `repeated string names;`
    /// Data: [["Alice", "Bob"], None, [], ["Charlie"]]
    /// Returns: [["Alice", "Bob"], nil, [], ["Charlie"]]
    public func readAllRepeated() throws -> [[String?]?] {
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
        var allValues: [String] = []  // Non-null values only
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
                    let value: String
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
    ///   - maxRepLevel=2: `[[[String?]?]?]` (list of optional lists of optional values)
    ///   - maxRepLevel=3: `[[[[String?]?]?]?]` (and so on)
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
        var allValues: [String] = []
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
                    let value: String
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

        // Decode repetition and definition levels
        var dataOffset = 0
        var repetitionLevels: [UInt16]?
        var definitionLevels: [UInt16]?

        // Decode repetition levels first (if column is repeated)
        if maxRepetitionLevel > 0 {
            guard page.data.count >= 4 else {
                throw ColumnReaderError.internalError("Page data too short for repetition levels")
            }

            let levelDataLength = Int(UInt32(page.data[0])
                | (UInt32(page.data[1]) << 8)
                | (UInt32(page.data[2]) << 16)
                | (UInt32(page.data[3]) << 24))

            let levelStreamLength = 4 + levelDataLength
            guard page.data.count >= levelStreamLength else {
                throw ColumnReaderError.internalError(
                    "Page data too short: expected \(levelStreamLength) bytes for repetition levels, got \(page.data.count)"
                )
            }

            let levelData = page.data.subdata(in: 0..<levelStreamLength)
            repetitionLevels = try levelDecoder.decodeLevels(
                from: levelData,
                numValues: page.numValues,
                maxLevel: maxRepetitionLevel
            )

            dataOffset = levelStreamLength
        }

        // Decode definition levels (if column is nullable)
        if maxDefinitionLevel > 0 {
            guard page.data.count >= dataOffset + 4 else {
                throw ColumnReaderError.internalError("Page data too short for definition levels")
            }

            let levelDataLength = Int(UInt32(page.data[dataOffset])
                | (UInt32(page.data[dataOffset + 1]) << 8)
                | (UInt32(page.data[dataOffset + 2]) << 16)
                | (UInt32(page.data[dataOffset + 3]) << 24))

            let levelStreamLength = 4 + levelDataLength
            guard page.data.count >= dataOffset + levelStreamLength else {
                throw ColumnReaderError.internalError(
                    "Page data too short: expected \(dataOffset + levelStreamLength) bytes for definition levels, got \(page.data.count)"
                )
            }

            let levelData = page.data.subdata(in: dataOffset..<(dataOffset + levelStreamLength))
            definitionLevels = try levelDecoder.decodeLevels(
                from: levelData,
                numValues: page.numValues,
                maxLevel: maxDefinitionLevel
            )

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
            let decoder = PlainDecoder<String>(data: dataSlice)
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
                "Unsupported encoding \(page.encoding) for String column"
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
