// PageWriter.swift - Writes data pages for column chunks
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Writes individual data pages for a column chunk
///
/// Handles page header serialization, compression, and writing to sink
final class PageWriter {
    private let sink: OutputSink
    private let codec: Compression
    private let properties: WriterProperties

    /// Initialize page writer
    /// - Parameters:
    ///   - sink: Output sink to write pages to
    ///   - codec: Compression codec to use
    ///   - properties: Writer properties
    init(sink: OutputSink, codec: Compression, properties: WriterProperties) {
        self.sink = sink
        self.codec = codec
        self.properties = properties
    }

    /// Result of writing a page
    struct PageWriteResult {
        let startOffset: Int64
        let bytesWritten: Int
        let uncompressedSize: Int
        let compressedSize: Int
    }

    /// Write a data page (V1)
    /// - Parameters:
    ///   - values: Encoded values data
    ///   - numValues: Number of values in page
    ///   - encoding: Encoding used for values
    ///   - definitionLevels: Optional definition levels for nullable columns
    /// - Returns: Page write result with offsets and sizes
    /// - Throws: WriterError if write fails
    func writeDataPage(
        values: Data,
        numValues: Int32,
        encoding: Encoding,
        definitionLevels: Data? = nil
    ) throws -> PageWriteResult {
        // Capture starting offset before writing
        let startOffset = try sink.tell()

        // Build page data: [definition levels] + values
        var pageData = Data()
        if let defLevels = definitionLevels {
            pageData.append(defLevels)
        }
        pageData.append(values)

        let uncompressedSize = pageData.count

        // Compress the page data
        let (compressedData, compressedSize) = try compressPage(pageData)

        // Build page header
        let dataPageHeader = ThriftDataPageHeader(
            numValues: numValues,
            encoding: encoding.toThrift(),
            definitionLevelEncoding: .rle,  // Always RLE for definition levels
            repetitionLevelEncoding: .rle,  // Not used in W4 (no repeated columns yet)
            statistics: nil  // Statistics not implemented yet
        )

        let pageHeader = ThriftPageHeader(
            type: .dataPage,
            uncompressedPageSize: Int32(uncompressedSize),
            compressedPageSize: Int32(compressedSize),
            crc: nil,  // CRC not implemented
            dataPageHeader: dataPageHeader,
            dictionaryPageHeader: nil,
            dataPageHeaderV2: nil
        )

        // Serialize page header
        let headerData = try serializePageHeader(pageHeader)

        // Write header + compressed data to sink
        try sink.write(headerData)
        try sink.write(compressedData)

        let bytesWritten = headerData.count + compressedSize

        return PageWriteResult(
            startOffset: startOffset,
            bytesWritten: bytesWritten,
            uncompressedSize: uncompressedSize,
            compressedSize: compressedSize
        )
    }

    /// Write a dictionary page
    /// - Parameters:
    ///   - dictionary: Encoded dictionary data
    ///   - numValues: Number of values in dictionary
    ///   - encoding: Encoding used for dictionary
    /// - Returns: Page write result with offsets and sizes
    /// - Throws: WriterError if write fails
    func writeDictionaryPage(
        dictionary: Data,
        numValues: Int32,
        encoding: Encoding
    ) throws -> PageWriteResult {
        // Capture starting offset before writing
        let startOffset = try sink.tell()

        let uncompressedSize = dictionary.count

        // Compress the dictionary data
        let (compressedData, compressedSize) = try compressPage(dictionary)

        // Build page header
        let dictionaryPageHeader = ThriftDictionaryPageHeader(
            numValues: numValues,
            encoding: encoding.toThrift(),
            isSorted: nil  // Not tracking sort order in W2
        )

        let pageHeader = ThriftPageHeader(
            type: .dictionaryPage,
            uncompressedPageSize: Int32(uncompressedSize),
            compressedPageSize: Int32(compressedSize),
            crc: nil,
            dataPageHeader: nil,
            dictionaryPageHeader: dictionaryPageHeader,
            dataPageHeaderV2: nil
        )

        // Serialize page header
        let headerData = try serializePageHeader(pageHeader)

        // Write header + compressed data to sink
        try sink.write(headerData)
        try sink.write(compressedData)

        let bytesWritten = headerData.count + compressedSize

        return PageWriteResult(
            startOffset: startOffset,
            bytesWritten: bytesWritten,
            uncompressedSize: uncompressedSize,
            compressedSize: compressedSize
        )
    }

    // MARK: - Private Methods

    private func compressPage(_ data: Data) throws -> (Data, Int) {
        guard codec != .uncompressed else {
            return (data, data.count)
        }

        do {
            let compressor = try CodecFactory.codec(for: codec)
            let compressed = try compressor.compress(data)
            return (compressed, compressed.count)
        } catch {
            throw WriterError.compressionFailed(codec, underlying: error)
        }
    }

    private func serializePageHeader(_ header: ThriftPageHeader) throws -> Data {
        let writer = ThriftWriter()
        try writer.writePageHeader(header)
        return writer.data
    }
}
