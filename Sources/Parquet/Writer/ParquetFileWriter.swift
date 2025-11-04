// ParquetFileWriter.swift - Main entry point for writing Parquet files
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Main class for writing Parquet files
///
/// Usage:
/// ```swift
/// let writer = try ParquetFileWriter(url: fileURL)
/// defer { try? writer.close() }
///
/// writer.setSchema(schema)
/// writer.setProperties(properties)
///
/// let rowGroup = try writer.createRowGroup()
/// // Write columns...
/// try rowGroup.close()
///
/// try writer.close()
/// ```
public final class ParquetFileWriter {
    // MARK: - State Machine

    private enum State {
        case created
        case schemaSet
        case rowGroupOpen
        case closed
    }

    private var state: State = .created

    // MARK: - Properties

    private let sink: OutputSink
    private let url: URL?

    private var schema: Schema?
    private var properties: WriterProperties = .default

    private var rowGroups: [WriterRowGroupMetadata] = []
    private var currentRowGroup: RowGroupWriter?

    private var startPosition: Int64 = 0

    // MARK: - Initialization

    /// Create a file writer for the given URL
    /// - Parameter url: File URL to write to
    /// - Throws: I/O errors if file cannot be created
    public init(url: URL) throws {
        self.url = url
        self.sink = try FileOutputSink(url: url)

        // Write magic number at start
        try writeStartMagic()
    }

    /// Create a file writer with a custom output sink
    /// - Parameter sink: Custom output sink
    /// - Throws: I/O errors if initialization fails
    init(sink: OutputSink) throws {
        self.url = nil
        self.sink = sink

        // Write magic number at start
        try writeStartMagic()
    }

    // MARK: - Configuration

    /// Set the schema for this file
    /// - Parameter schema: The Parquet schema
    /// - Throws: WriterError if schema is already set or state is invalid
    public func setSchema(_ schema: Schema) throws {
        guard state == .created else {
            throw WriterError.invalidState("Schema can only be set once, before creating row groups")
        }

        self.schema = schema
        state = .schemaSet
    }

    /// Set writer properties
    /// - Parameter properties: Configuration properties
    public func setProperties(_ properties: WriterProperties) {
        self.properties = properties
    }

    // MARK: - Row Group Management

    /// Create a new row group
    /// - Returns: A RowGroupWriter for writing columns
    /// - Throws: WriterError if schema not set or state is invalid
    public func createRowGroup() throws -> RowGroupWriter {
        guard let schema = schema else {
            throw WriterError.schemaNotSet
        }

        guard state == .schemaSet || state == .rowGroupOpen else {
            throw WriterError.invalidState("Cannot create row group in current state")
        }

        // Close previous row group if open
        if let previousRowGroup = currentRowGroup {
            let metadata = try previousRowGroup.close()
            rowGroups.append(metadata)
        }

        // Get current position for row group start
        let rowGroupStartPosition = try sink.tell()

        // Create new row group
        let rowGroup = RowGroupWriter(
            schema: schema,
            properties: properties,
            sink: sink,
            startOffset: rowGroupStartPosition,
            ordinal: rowGroups.count
        )

        currentRowGroup = rowGroup
        state = .rowGroupOpen

        return rowGroup
    }

    // MARK: - File Finalization

    /// Close the file and write the footer
    /// - Throws: WriterError or I/O errors if close fails
    public func close() throws {
        guard state != .closed else {
            return  // Already closed
        }

        guard let schema = schema else {
            throw WriterError.schemaNotSet
        }

        // Close current row group if open
        if let rowGroup = currentRowGroup {
            // Only include row group if it has columns written
            // In W1, we may have empty row groups since column writers aren't implemented
            if rowGroup.hasColumnsWritten {
                let metadata = try rowGroup.close()
                rowGroups.append(metadata)
            }
            currentRowGroup = nil
        }

        // Write footer
        try writeFooter(schema: schema, rowGroups: rowGroups)

        // Close sink
        try sink.close()

        state = .closed
    }

    // MARK: - Private Methods

    private func writeStartMagic() throws {
        startPosition = try sink.tell()

        // Write "PAR1" magic number
        let magic = Data("PAR1".utf8)
        try sink.write(magic)
    }

    private func writeFooter(schema: Schema, rowGroups: [WriterRowGroupMetadata]) throws {
        // Build FileMetaData
        let fileMetadata = try buildFileMetadata(
            schema: schema,
            rowGroups: rowGroups,
            createdBy: "parquet-swift version 0.9"
        )

        // Serialize FileMetaData to Thrift
        let metadataBytes = try serializeFileMetadata(fileMetadata)

        // Write metadata
        try sink.write(metadataBytes)

        // Write metadata length (4 bytes, little-endian)
        let metadataLength = UInt32(metadataBytes.count)
        var lengthBytes = Data(count: 4)
        lengthBytes.withUnsafeMutableBytes { ptr in
            ptr.storeBytes(of: metadataLength.littleEndian, as: UInt32.self)
        }
        try sink.write(lengthBytes)

        // Write end magic "PAR1"
        let magic = Data("PAR1".utf8)
        try sink.write(magic)

        // Flush to ensure all data is written
        try sink.flush()
    }

    private func buildFileMetadata(
        schema: Schema,
        rowGroups: [WriterRowGroupMetadata],
        createdBy: String
    ) throws -> ThriftFileMetaData {
        // Convert schema to Thrift format
        let thriftSchema = try schema.toThrift()

        // Convert row groups to Thrift format
        let thriftRowGroups = rowGroups.map { $0.toThrift() }

        // Calculate total number of rows
        let numRows = rowGroups.reduce(0) { $0 + $1.numRows }

        return ThriftFileMetaData(
            version: 1,
            schema: thriftSchema,
            numRows: numRows,
            rowGroups: thriftRowGroups,
            keyValueMetadata: nil,
            createdBy: createdBy,
            columnOrders: nil
        )
    }

    private func serializeFileMetadata(_ metadata: ThriftFileMetaData) throws -> Data {
        // Use ThriftWriter to serialize to Compact Binary Protocol
        let writer = ThriftWriter()

        do {
            try writer.writeFileMetaData(metadata)
            return writer.data
        } catch {
            throw WriterError.thriftSerializationError(
                "Failed to serialize FileMetaData: \(error.localizedDescription)"
            )
        }
    }
}

// MARK: - Row Group Metadata

/// Metadata for a completed row group
struct WriterRowGroupMetadata {
    let numRows: Int64
    let totalByteSize: Int64
    let columns: [WriterColumnChunkMetadata]
    let ordinal: Int

    func toThrift() -> ThriftRowGroup {
        return ThriftRowGroup(
            columns: columns.map { $0.toThrift() },
            totalByteSize: totalByteSize,
            numRows: numRows,
            sortingColumns: nil,
            fileOffset: nil,
            totalCompressedSize: nil,
            ordinal: Int16(exactly: ordinal)
        )
    }
}

// MARK: - Column Chunk Metadata

/// Metadata for a completed column chunk
struct WriterColumnChunkMetadata {
    let column: Column
    let fileOffset: Int64
    let dataPageOffset: Int64
    let dictionaryPageOffset: Int64?
    let numValues: Int64
    let totalCompressedSize: Int64
    let totalUncompressedSize: Int64
    let encodings: [Encoding]
    let codec: Compression

    func toThrift() -> ThriftColumnChunk {
        let metaData = ThriftColumnMetaData(
            type: column.physicalType.toThrift(),
            encodings: encodings.map { $0.toThrift() },
            pathInSchema: column.path,
            codec: codec.toThrift(),
            numValues: numValues,
            totalUncompressedSize: totalUncompressedSize,
            totalCompressedSize: totalCompressedSize,
            keyValueMetadata: nil,
            dataPageOffset: dataPageOffset,
            indexPageOffset: nil,
            dictionaryPageOffset: dictionaryPageOffset,
            statistics: nil,
            encodingStats: nil,
            bloomFilterOffset: nil,
            bloomFilterLength: nil
        )

        return ThriftColumnChunk(
            filePath: nil,
            fileOffset: fileOffset,
            metaData: metaData,
            offsetIndexOffset: nil,
            offsetIndexLength: nil,
            columnIndexOffset: nil,
            columnIndexLength: nil
        )
    }
}
