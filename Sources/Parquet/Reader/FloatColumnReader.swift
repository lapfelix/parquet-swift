// FloatColumnReader - Read Float column values
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Reads Float values from a Parquet column chunk.
///
/// Supports both PLAIN and dictionary encoding, with nullable columns.
///
/// # Phase 3 Features
///
/// - ✅ Required columns (non-nullable)
/// - ✅ Nullable columns (definition levels)
/// - ✅ PLAIN and dictionary encoding
///
/// Returns `[Float?]` where `nil` represents NULL values in nullable columns.
/// For required columns, all values will be non-nil.
///
/// # Usage
///
/// ```swift
/// let reader = try FloatColumnReader(
///     file: file,
///     columnMetadata: metadata,
///     codec: codec,
///     column: column
/// )
///
/// let values = try reader.readBatch(count: 1000)  // Returns [Float?]
/// ```
public final class FloatColumnReader {
    /// Page reader for this column
    private let pageReader: PageReader

    /// Dictionary (if column uses dictionary encoding)
    private let dictionary: Dictionary<Float>?

    /// Maximum definition level for this column (from schema)
    private let maxDefinitionLevel: Int

    /// Maximum repetition level for this column (from schema)
    private let maxRepetitionLevel: Int

    /// Level decoder for definition/repetition levels
    private let levelDecoder: LevelDecoder

    /// Current page being read
    private var currentPage: DataPage?

    /// Decoder for current page (PLAIN encoding)
    private var currentDecoder: PlainDecoder<Float>?

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

    /// Initialize an Float column reader
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
            self.dictionary = try Dictionary.float(page: dictPage)
        } else {
            self.dictionary = nil
        }
    }

    // MARK: - Batch Reading

    /// Read a batch of values
    ///
    /// - Parameter count: Maximum number of values to read
    /// - Returns: Array of optional values (nil for NULL values in nullable columns)
    /// - Throws: `ColumnReaderError` if reading fails
    public func readBatch(count: Int) throws -> [Float?] {
        var values: [Float?] = []
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
                let value: Float
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
    /// - Throws: `ColumnReaderError` if reading fails
    public func readOne() throws -> Float?? {
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
        let value: Float
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
    /// - Throws: `ColumnReaderError` if reading fails
    public func readAll() throws -> [Float?] {
        var values: [Float?] = []

        while let value = try readOne() {
            values.append(value)
        }

        return values
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

        // PHASE 3: Support for nullable and repeated columns
        // Decode repetition and definition levels
        var dataOffset = 0
        var repetitionLevels: [UInt16]?
        var definitionLevels: [UInt16]?

        // Decode repetition levels FIRST if maxRepetitionLevel > 0
        if maxRepetitionLevel > 0 {
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
                    "Page data too short: expected \(levelStreamLength) bytes for repetition levels, got \(page.data.count)"
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

            // Move offset past repetition level stream
            dataOffset += levelStreamLength
        }

        // Decode definition levels if maxDefinitionLevel > 0
        if maxDefinitionLevel > 0 {
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
                    "Page data too short: expected \(levelStreamLength) bytes for definition levels, got \(page.data.count)"
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

            // Move offset past definition level stream
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
            let decoder = PlainDecoder<Float>(data: dataSlice)
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
                "Unsupported encoding \(page.encoding) for Float column"
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
