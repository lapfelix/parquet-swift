// StringColumnReader - Read String column values
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Reads String values from a Parquet column chunk.
///
/// Supports both PLAIN and dictionary encoding.
///
/// # Phase 2.1 Limitations
///
/// **Dictionary encoding currently only works for required (non-nullable) columns.**
///
/// - ✅ Supported: Required columns with dictionary encoding
/// - ❌ Not yet supported: Optional/repeated columns (definition/repetition levels)
///
/// Nullable columns store definition levels before the data in each page.
/// Phase 3 will add level decoding to support nullable and repeated columns.
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
/// let values = try reader.readBatch(count: 1000)
/// ```
public final class StringColumnReader {
    /// Page reader for this column
    private let pageReader: PageReader

    /// Dictionary (if column uses dictionary encoding)
    private let dictionary: Dictionary<String>?

    /// Maximum definition level for this column (from schema)
    private let maxDefinitionLevel: Int

    /// Maximum repetition level for this column (from schema)
    private let maxRepetitionLevel: Int

    /// Current page being read
    private var currentPage: DataPage?

    /// Decoder for current page (PLAIN encoding)
    private var currentDecoder: PlainDecoder<String>?

    /// Decoded indices for current page (dictionary encoding)
    private var currentIndices: [UInt32]?

    /// Number of values read from current page
    private var valuesReadFromPage: Int = 0

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
        self.pageReader = try PageReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec
        )
        self.maxDefinitionLevel = column.maxDefinitionLevel
        self.maxRepetitionLevel = column.maxRepetitionLevel

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
    /// - Returns: Array of values (may be fewer than requested if end of column)
    /// - Throws: `ColumnReaderError` if reading fails
    public func readBatch(count: Int) throws -> [String] {
        var values: [String] = []
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

            if let decoder = currentDecoder {
                // PLAIN encoding
                let pageValues = try decoder.decode(count: toRead)
                values.append(contentsOf: pageValues)
            } else if let indices = currentIndices, let dict = dictionary {
                // Dictionary encoding
                let startIndex = valuesReadFromPage
                let endIndex = startIndex + toRead
                for i in startIndex..<endIndex {
                    let value = try dict.value(at: indices[i])
                    values.append(value)
                }
            } else {
                throw ColumnReaderError.internalError("No decoder or indices available")
            }

            valuesReadFromPage += toRead
            remaining -= toRead

            // Check if page is exhausted
            if valuesReadFromPage >= currentPage!.numValues {
                currentPage = nil
                currentDecoder = nil
                currentIndices = nil
                valuesReadFromPage = 0
            }
        }

        return values
    }

    /// Read a single value
    ///
    /// - Returns: The value, or nil if no more values
    /// - Throws: `ColumnReaderError` if reading fails
    public func readOne() throws -> String? {
        guard try loadPageIfNeeded() else {
            return nil
        }

        let value: String
        if let decoder = currentDecoder {
            // PLAIN encoding
            value = try decoder.decodeOne()
        } else if let indices = currentIndices, let dict = dictionary {
            // Dictionary encoding
            value = try dict.value(at: indices[valuesReadFromPage])
        } else {
            throw ColumnReaderError.internalError("No decoder or indices available")
        }

        valuesReadFromPage += 1

        // Check if page is exhausted
        if valuesReadFromPage >= currentPage!.numValues {
            currentPage = nil
            currentDecoder = nil
            currentIndices = nil
            valuesReadFromPage = 0
        }

        return value
    }

    /// Read all remaining values
    ///
    /// - Returns: Array of all remaining values
    /// - Throws: `ColumnReaderError` if reading fails
    public func readAll() throws -> [String] {
        var values: [String] = []

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

        // Decode based on encoding type
        switch page.encoding {
        case .plain:
            // PLAIN encoding: use PlainDecoder directly
            let decoder = PlainDecoder<String>(data: page.data)
            currentPage = page
            currentDecoder = decoder
            currentIndices = nil
            valuesReadFromPage = 0

        case .rleDictionary, .plainDictionary:
            // Dictionary encoding: decode indices with RLE decoder
            guard dictionary != nil else {
                throw ColumnReaderError.missingDictionary(
                    "Page uses \(page.encoding) encoding but no dictionary was found"
                )
            }

            // PHASE 2.1 LIMITATION: Only required (non-nullable, non-repeated) columns supported
            if maxDefinitionLevel > 0 || maxRepetitionLevel > 0 {
                var unsupportedFeatures: [String] = []
                if maxDefinitionLevel > 0 {
                    unsupportedFeatures.append("nullable columns (definition levels)")
                }
                if maxRepetitionLevel > 0 {
                    unsupportedFeatures.append("repeated columns (repetition levels)")
                }

                throw ColumnReaderError.unsupportedEncoding(
                    """
                    Dictionary encoding with \(unsupportedFeatures.joined(separator: " and ")) \
                    not yet supported in Phase 2.1. \
                    This column requires level stream decoding (Phase 3). \
                    Currently only required (non-nullable, non-repeated) dictionary columns are supported.
                    """
                )
            }

            // Safe to decode: maxDefinitionLevel = 0 and maxRepetitionLevel = 0
            let rleDecoder = RLEDecoder()
            let indices = try rleDecoder.decodeIndices(from: page.data, numValues: page.numValues)

            currentPage = page
            currentDecoder = nil
            currentIndices = indices
            valuesReadFromPage = 0

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
