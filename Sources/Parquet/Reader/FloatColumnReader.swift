// FloatColumnReader - Read Float column values
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Reads Float values from a Parquet column chunk.
///
/// Phase 1 implementation for PLAIN encoding only.
///
/// # Usage
///
/// ```swift
/// let reader = try FloatColumnReader(
///     file: file,
///     columnMetadata: metadata,
///     codec: codec
/// )
///
/// let values = try reader.readBatch(count: 1000)
/// ```
public final class FloatColumnReader {
    /// Page reader for this column
    private let pageReader: PageReader

    /// Current page being read
    private var currentPage: DataPage?

    /// Decoder for current page
    private var currentDecoder: PlainDecoder<Float>?

    /// Number of values read from current page
    private var valuesReadFromPage: Int = 0

    /// Initialize a Float column reader
    ///
    /// - Parameters:
    ///   - file: The file to read from
    ///   - columnMetadata: Metadata for the column chunk
    ///   - codec: Compression codec
    public init(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws {
        self.pageReader = try PageReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec
        )

        // Skip dictionary page if present (Phase 1: PLAIN encoding only, no dictionary support)
        _ = try pageReader.readDictionaryPage()
    }

    // MARK: - Batch Reading

    /// Read a batch of values
    ///
    /// - Parameter count: Maximum number of values to read
    /// - Returns: Array of values (may be fewer than requested if end of column)
    /// - Throws: `ColumnReaderError` if reading fails
    public func readBatch(count: Int) throws -> [Float] {
        var values: [Float] = []
        values.reserveCapacity(count)

        var remaining = count

        while remaining > 0 {
            // Get decoder for current page (or load next page)
            guard let decoder = try getCurrentDecoder() else {
                // No more pages
                break
            }

            // Read from current page
            let toRead = min(remaining, remainingInPage())
            let pageValues = try decoder.decode(count: toRead)
            values.append(contentsOf: pageValues)

            valuesReadFromPage += toRead
            remaining -= toRead

            // Check if page is exhausted
            if valuesReadFromPage >= currentPage!.numValues {
                currentPage = nil
                currentDecoder = nil
                valuesReadFromPage = 0
            }
        }

        return values
    }

    /// Read a single value
    ///
    /// - Returns: The value, or nil if no more values
    /// - Throws: `ColumnReaderError` if reading fails
    public func readOne() throws -> Float? {
        guard let decoder = try getCurrentDecoder() else {
            return nil
        }

        let value = try decoder.decodeOne()
        valuesReadFromPage += 1

        // Check if page is exhausted
        if valuesReadFromPage >= currentPage!.numValues {
            currentPage = nil
            currentDecoder = nil
            valuesReadFromPage = 0
        }

        return value
    }

    /// Read all remaining values
    ///
    /// - Returns: Array of all remaining values
    /// - Throws: `ColumnReaderError` if reading fails
    public func readAll() throws -> [Float] {
        var values: [Float] = []

        while let value = try readOne() {
            values.append(value)
        }

        return values
    }

    // MARK: - Private Helpers

    /// Get decoder for current page (loading new page if needed)
    private func getCurrentDecoder() throws -> PlainDecoder<Float>? {
        // Return existing decoder if page still has values
        if let decoder = currentDecoder, valuesReadFromPage < currentPage!.numValues {
            return decoder
        }

        // Load next page
        guard let page = try pageReader.readDataPage() else {
            return nil // No more pages
        }

        // Validate encoding
        guard page.encoding == .plain else {
            throw ColumnReaderError.unsupportedEncoding(
                "Only PLAIN encoding supported in M1.9c, got \(page.encoding)"
            )
        }

        // Create decoder for page data
        let decoder = PlainDecoder<Float>(data: page.data)

        currentPage = page
        currentDecoder = decoder
        valuesReadFromPage = 0

        return decoder
    }

    /// Get remaining values in current page
    private func remainingInPage() -> Int {
        guard let page = currentPage else { return 0 }
        return page.numValues - valuesReadFromPage
    }
}
