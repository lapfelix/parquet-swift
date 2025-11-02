// PageReader - Read and decompress Parquet pages
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Reads pages from a Parquet column chunk.
///
/// A column chunk contains a sequence of pages:
/// - Optional dictionary page (must be first)
/// - One or more data pages
///
/// Each page has:
/// - Page header (Thrift-encoded metadata)
/// - Page data (compressed or uncompressed)
///
/// # Usage
///
/// ```swift
/// let pageReader = try PageReader(
///     file: file,
///     columnMetadata: columnMeta,
///     codec: codec
/// )
///
/// // Read dictionary page (if present)
/// if let dictPage = try pageReader.readDictionaryPage() {
///     // Process dictionary
/// }
///
/// // Read data pages
/// while let dataPage = try pageReader.readDataPage() {
///     // Process data
/// }
/// ```
public final class PageReader {
    /// The file to read from
    private let reader: BufferedReader

    /// Column metadata
    private let columnMetadata: ColumnMetadata

    /// Compression codec
    private let codec: Codec

    /// Current offset in the file
    private var offset: Int64

    /// Whether we've read the dictionary page
    private var dictionaryPageRead: Bool = false

    /// Total bytes read so far
    private var bytesRead: Int64 = 0

    /// Initialize a page reader
    ///
    /// - Parameters:
    ///   - file: The file to read from
    ///   - columnMetadata: Metadata for the column chunk
    ///   - codec: Compression codec for decompression
    public init(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws {
        self.reader = BufferedReader(file: file)
        self.columnMetadata = columnMetadata
        self.codec = codec

        // Start reading from the first page
        // Dictionary page comes first (if present), otherwise first data page
        // Note: Page offsets in ColumnMetaData are absolute file offsets per Parquet spec
        if let dictOffset = columnMetadata.dictionaryPageOffset {
            self.offset = dictOffset
        } else {
            self.offset = columnMetadata.dataPageOffset
        }
    }

    // MARK: - Dictionary Page Reading

    /// Read the dictionary page (if present).
    ///
    /// Must be called before reading data pages.
    /// Returns nil if no dictionary page exists.
    ///
    /// - Returns: The dictionary page, or nil
    /// - Throws: `PageReaderError` if reading fails
    public func readDictionaryPage() throws -> DictionaryPage? {
        guard !dictionaryPageRead else {
            return nil // Already read
        }

        dictionaryPageRead = true

        guard columnMetadata.dictionaryPageOffset != nil else {
            return nil // No dictionary page
        }

        // Read page header
        let (header, headerSize) = try readPageHeader()

        guard header.type == .dictionaryPage else {
            // Not a dictionary page, reset offset
            offset -= Int64(headerSize)
            return nil
        }

        guard let dictHeader = header.dictionaryPageHeader else {
            throw PageReaderError.invalidPageHeader("Dictionary page header missing")
        }

        // Read and decompress page data
        let pageData = try readAndDecompressPage(header: header)

        bytesRead += Int64(headerSize) + Int64(header.compressedPageSize)

        return DictionaryPage(
            data: pageData,
            numValues: Int(dictHeader.numValues),
            encoding: dictHeader.encoding
        )
    }

    // MARK: - Data Page Reading

    /// Read the next data page.
    ///
    /// Returns nil when all pages have been read.
    ///
    /// - Returns: The data page, or nil if no more pages
    /// - Throws: `PageReaderError` if reading fails
    public func readDataPage() throws -> DataPage? {
        // Check if we've read all data
        let totalCompressedSize = columnMetadata.totalCompressedSize
        if bytesRead >= totalCompressedSize {
            return nil
        }

        // Read page header
        let (header, headerSize) = try readPageHeader()

        // Handle different page types
        switch header.type {
        case .dataPage:
            guard let dataHeader = header.dataPageHeader else {
                throw PageReaderError.invalidPageHeader("Data page header missing")
            }

            // Read and decompress page data
            let pageData = try readAndDecompressPage(header: header)

            bytesRead += Int64(headerSize) + Int64(header.compressedPageSize)

            return DataPage(
                data: pageData,
                numValues: Int(dataHeader.numValues),
                encoding: dataHeader.encoding,
                definitionLevelEncoding: dataHeader.definitionLevelEncoding,
                repetitionLevelEncoding: dataHeader.repetitionLevelEncoding
            )

        case .dataPageV2:
            // Phase 1: Skip v2 pages
            throw PageReaderError.unsupportedPageType("DATA_PAGE_V2 not supported in Phase 1")

        case .indexPage:
            // Skip index pages
            offset += Int64(header.compressedPageSize)
            bytesRead += Int64(headerSize) + Int64(header.compressedPageSize)
            return try readDataPage() // Try next page

        case .dictionaryPage:
            // Dictionary page after data pages is invalid
            throw PageReaderError.invalidPageHeader("Dictionary page must come first")
        }
    }

    // MARK: - Private Helpers

    /// Read a page header from the current offset
    private func readPageHeader() throws -> (ThriftPageHeader, Int) {
        // Read enough bytes for the header (headers are typically small, < 1KB)
        // We'll read generously and ThriftReader will use what it needs
        // But clamp to file size to avoid reading past end
        let fileSize = Int64(try reader.fileSize)
        let maxRead = min(4096, Int(fileSize - offset))

        guard maxRead > 0 else {
            throw PageReaderError.ioError("No data available at offset \(offset)")
        }

        let headerData = try reader.read(at: Int(offset), count: maxRead)

        let thriftReader = ThriftReader(data: headerData)
        let header = try thriftReader.readPageHeader()

        // Calculate actual header size from position
        let headerSize = thriftReader.currentPosition

        // Advance offset past the header
        offset += Int64(headerSize)

        return (header, headerSize)
    }

    /// Read and decompress page data
    private func readAndDecompressPage(header: ThriftPageHeader) throws -> Data {
        let compressedSize = Int(header.compressedPageSize)
        let uncompressedSize = Int(header.uncompressedPageSize)

        // Read compressed page data
        let compressedData = try reader.read(at: Int(offset), count: compressedSize)

        // Advance offset past the page data
        offset += Int64(compressedSize)

        // Decompress if needed
        let pageData: Data
        if codec.compressionType == .uncompressed {
            pageData = compressedData
        } else {
            pageData = try codec.decompress(compressedData, uncompressedSize: uncompressedSize)
        }

        return pageData
    }
}

// MARK: - Page Types

/// A dictionary page containing dictionary values.
public struct DictionaryPage {
    /// Uncompressed page data
    public let data: Data

    /// Number of values in the dictionary
    public let numValues: Int

    /// Encoding used for dictionary values
    public let encoding: ThriftEncoding
}

/// A data page containing column values.
public struct DataPage {
    /// Uncompressed page data
    public let data: Data

    /// Number of values in this page (including nulls)
    public let numValues: Int

    /// Encoding used for values
    public let encoding: ThriftEncoding

    /// Encoding used for definition levels
    public let definitionLevelEncoding: ThriftEncoding

    /// Encoding used for repetition levels
    public let repetitionLevelEncoding: ThriftEncoding
}

// MARK: - Errors

/// Errors that can occur during page reading
public enum PageReaderError: Error, Equatable {
    /// Invalid page header
    case invalidPageHeader(String)

    /// Unsupported page type
    case unsupportedPageType(String)

    /// Decompression failed
    case decompressionFailed(String)

    /// I/O error
    case ioError(String)
}

extension PageReaderError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .invalidPageHeader(let msg):
            return "Invalid page header: \(msg)"
        case .unsupportedPageType(let msg):
            return "Unsupported page type: \(msg)"
        case .decompressionFailed(let msg):
            return "Decompression failed: \(msg)"
        case .ioError(let msg):
            return "I/O error: \(msg)"
        }
    }
}
