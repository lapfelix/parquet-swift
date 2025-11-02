// Parquet file metadata wrappers
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// File-level metadata for a Parquet file.
///
/// Contains schema information, row groups, and file-level key-value metadata.
public final class FileMetadata {
    /// The underlying Thrift metadata.
    internal let thrift: ThriftFileMetaData

    /// The schema for this file.
    public let schema: Schema

    /// Initialize from Thrift metadata.
    internal init(thrift: ThriftFileMetaData) throws {
        self.thrift = thrift
        self.schema = try SchemaBuilder.buildSchema(from: thrift.schema)
    }

    /// The Parquet format version (1 or 2).
    public var version: Int {
        return Int(thrift.version)
    }

    /// The total number of rows in the file.
    public var numRows: Int64 {
        return thrift.numRows
    }

    /// The row groups in this file.
    public var rowGroups: [RowGroupMetadata] {
        return thrift.rowGroups.map { RowGroupMetadata(thrift: $0) }
    }

    /// The number of row groups in this file.
    public var numRowGroups: Int {
        return thrift.rowGroups.count
    }

    /// File-level key-value metadata.
    public var keyValueMetadata: [String: String] {
        guard let kvList = thrift.keyValueMetadata else {
            return [:]
        }
        var dict: [String: String] = [:]
        for kv in kvList {
            dict[kv.key] = kv.value
        }
        return dict
    }

    /// String identifying the writer that created this file.
    public var createdBy: String? {
        return thrift.createdBy
    }

    /// Column ordering specifications.
    public var columnOrders: [ThriftColumnOrder]? {
        return thrift.columnOrders
    }
}

// MARK: - RowGroupMetadata

/// Metadata for a single row group.
///
/// A row group is a horizontal partition of the data, containing a subset of rows.
public final class RowGroupMetadata {
    /// The underlying Thrift metadata.
    internal let thrift: ThriftRowGroup

    /// Initialize from Thrift metadata.
    internal init(thrift: ThriftRowGroup) {
        self.thrift = thrift
    }

    /// The number of rows in this row group.
    public var numRows: Int64 {
        return thrift.numRows
    }

    /// The total byte size of all compressed column chunks in this row group.
    public var totalByteSize: Int64 {
        return thrift.totalByteSize
    }

    /// The column chunks in this row group.
    public var columns: [ColumnChunkMetadata] {
        return thrift.columns.map { ColumnChunkMetadata(thrift: $0) }
    }

    /// The number of columns in this row group.
    public var numColumns: Int {
        return thrift.columns.count
    }

    /// The file offset where this row group starts.
    public var fileOffset: Int64? {
        return thrift.fileOffset
    }

    /// Total uncompressed byte size of all column chunks.
    public var totalCompressedSize: Int64? {
        return thrift.totalCompressedSize
    }

    /// Ordinal position of this row group (if available).
    public var ordinal: Int16? {
        return thrift.ordinal
    }
}

// MARK: - ColumnChunkMetadata

/// Metadata for a single column chunk within a row group.
///
/// Contains information about data location, encoding, compression, and statistics.
public final class ColumnChunkMetadata {
    /// The underlying Thrift metadata.
    internal let thrift: ThriftColumnChunk

    /// Initialize from Thrift metadata.
    internal init(thrift: ThriftColumnChunk) {
        self.thrift = thrift
    }

    /// The file path (for external columns, rarely used).
    public var filePath: String? {
        return thrift.filePath
    }

    /// The byte offset in the file where this column chunk starts.
    public var fileOffset: Int64 {
        return thrift.fileOffset
    }

    /// Detailed column metadata.
    public var metadata: ColumnMetadata? {
        guard let meta = thrift.metaData else {
            return nil
        }
        return ColumnMetadata(thrift: meta)
    }

    /// The starting position in the file for offset index pages.
    public var offsetIndexOffset: Int64? {
        return thrift.offsetIndexOffset
    }

    /// The length of the offset index.
    public var offsetIndexLength: Int32? {
        return thrift.offsetIndexLength
    }

    /// The starting position in the file for column index pages.
    public var columnIndexOffset: Int64? {
        return thrift.columnIndexOffset
    }

    /// The length of the column index.
    public var columnIndexLength: Int32? {
        return thrift.columnIndexLength
    }
}

// MARK: - ColumnMetadata

/// Detailed metadata for a column chunk.
///
/// Contains type information, encoding, compression, statistics, and size information.
public final class ColumnMetadata {
    /// The underlying Thrift metadata.
    internal let thrift: ThriftColumnMetaData

    /// Initialize from Thrift metadata.
    internal init(thrift: ThriftColumnMetaData) {
        self.thrift = thrift
    }

    /// The physical type of this column.
    ///
    /// Note: For fixedLenByteArray types, the length is not stored in ColumnMetaData.
    /// It must be obtained from the schema. This accessor returns length 0 for such types.
    public var physicalType: PhysicalType {
        return convertPhysicalType(thrift.type, typeLength: nil)
    }

    /// The encodings used in this column chunk.
    public var encodings: [Encoding] {
        return thrift.encodings.compactMap { convertEncoding($0) }
    }

    /// The path in the schema to this column.
    public var path: [String] {
        return thrift.pathInSchema
    }

    /// The compression codec used for this column chunk.
    public var codec: Compression {
        return convertCompression(thrift.codec)
    }

    /// The number of values in this column chunk.
    public var numValues: Int64 {
        return thrift.numValues
    }

    /// The total uncompressed size in bytes.
    public var totalUncompressedSize: Int64 {
        return thrift.totalUncompressedSize
    }

    /// The total compressed size in bytes.
    public var totalCompressedSize: Int64 {
        return thrift.totalCompressedSize
    }

    /// The file offset of the first data page.
    public var dataPageOffset: Int64 {
        return thrift.dataPageOffset
    }

    /// The file offset of the dictionary page (if present).
    public var dictionaryPageOffset: Int64? {
        return thrift.dictionaryPageOffset
    }

    /// Statistics for this column chunk.
    public var statistics: Statistics? {
        guard let stats = thrift.statistics else {
            return nil
        }
        return Statistics(thrift: stats)
    }

    /// The file offset of the index page (rarely used).
    public var indexPageOffset: Int64? {
        return thrift.indexPageOffset
    }

    /// Key-value metadata specific to this column chunk.
    public var keyValueMetadata: [String: String] {
        guard let kvList = thrift.keyValueMetadata else {
            return [:]
        }
        var dict: [String: String] = [:]
        for kv in kvList {
            dict[kv.key] = kv.value
        }
        return dict
    }

    /// The encoding statistics for this column chunk.
    public var encodingStats: [EncodingStat]? {
        return thrift.encodingStats?.map { EncodingStat(thrift: $0) }
    }

    /// The offset of the Bloom filter in the file.
    public var bloomFilterOffset: Int64? {
        return thrift.bloomFilterOffset
    }

    /// The length of the Bloom filter in bytes.
    public var bloomFilterLength: Int32? {
        return thrift.bloomFilterLength
    }
}

// MARK: - Statistics

/// Statistics for a column chunk.
///
/// Contains min/max values, null count, and distinct count.
public final class Statistics {
    /// The underlying Thrift statistics.
    internal let thrift: ThriftStatistics

    /// Initialize from Thrift statistics.
    internal init(thrift: ThriftStatistics) {
        self.thrift = thrift
    }

    /// The maximum value (encoded as bytes).
    public var max: Data? {
        return thrift.max
    }

    /// The minimum value (encoded as bytes).
    public var min: Data? {
        return thrift.min
    }

    /// The number of null values.
    public var nullCount: Int64? {
        return thrift.nullCount
    }

    /// The number of distinct values.
    public var distinctCount: Int64? {
        return thrift.distinctCount
    }

    /// The maximum value (encoded as bytes, newer format).
    public var maxValue: Data? {
        return thrift.maxValue
    }

    /// The minimum value (encoded as bytes, newer format).
    public var minValue: Data? {
        return thrift.minValue
    }

    /// Whether min and max values are present.
    public var hasMinMax: Bool {
        return (min != nil && max != nil) || (minValue != nil && maxValue != nil)
    }
}

// MARK: - EncodingStat

/// Statistics about encoding usage.
public struct EncodingStat {
    /// The page type.
    public let pageType: ThriftPageType

    /// The encoding type.
    public let encoding: Encoding

    /// The number of pages using this encoding.
    public let pageCount: Int

    /// Initialize from Thrift encoding stats.
    internal init(thrift: ThriftPageEncodingStats) {
        self.pageType = thrift.pageType
        self.encoding = convertEncoding(thrift.encoding) ?? .plain
        self.pageCount = Int(thrift.count)
    }
}

// MARK: - Conversion Functions

/// Convert Thrift physical type to Swift PhysicalType.
private func convertPhysicalType(_ thrift: ThriftType, typeLength: Int32?) -> PhysicalType {
    switch thrift {
    case .boolean:
        return .boolean
    case .int32:
        return .int32
    case .int64:
        return .int64
    case .int96:
        return .int96
    case .float:
        return .float
    case .double:
        return .double
    case .byteArray:
        return .byteArray
    case .fixedLenByteArray:
        return .fixedLenByteArray(length: Int(typeLength ?? 0))
    }
}

/// Convert Thrift encoding to Swift Encoding.
private func convertEncoding(_ thrift: ThriftEncoding) -> Encoding? {
    switch thrift {
    case .plain:
        return .plain
    case .plainDictionary:
        return .plainDictionary
    case .rle:
        return .rle
    case .bitPacked:
        return .bitPacked
    case .rleDictionary:
        return .rleDictionary
    case .deltaBinaryPacked:
        return .deltaBinaryPacked
    case .deltaLengthByteArray:
        return .deltaLengthByteArray
    case .deltaByteArray:
        return .deltaByteArray
    case .byteStreamSplit:
        return .byteStreamSplit
    }
}

/// Convert Thrift compression codec to Swift Compression.
private func convertCompression(_ thrift: ThriftCompressionCodec) -> Compression {
    switch thrift {
    case .uncompressed:
        return .uncompressed
    case .snappy:
        return .snappy
    case .gzip:
        return .gzip
    case .lzo:
        return .lzo
    case .brotli:
        return .brotli
    case .lz4:
        return .lz4
    case .zstd:
        return .zstd
    case .lz4Raw:
        return .lz4Raw
    }
}

// MARK: - CustomStringConvertible

extension FileMetadata: CustomStringConvertible {
    public var description: String {
        var lines: [String] = []
        lines.append("Parquet File Metadata:")
        lines.append("  Version: \(version)")
        lines.append("  Rows: \(numRows)")
        lines.append("  Row Groups: \(numRowGroups)")
        lines.append("  Columns: \(schema.columnCount)")
        if let createdBy = createdBy {
            lines.append("  Created By: \(createdBy)")
        }
        if !keyValueMetadata.isEmpty {
            lines.append("  Metadata: \(keyValueMetadata.count) key-value pairs")
        }
        return lines.joined(separator: "\n")
    }
}

extension RowGroupMetadata: CustomStringConvertible {
    public var description: String {
        var lines: [String] = []
        lines.append("Row Group:")
        lines.append("  Rows: \(numRows)")
        lines.append("  Total Byte Size: \(totalByteSize)")
        lines.append("  Columns: \(numColumns)")
        if let offset = fileOffset {
            lines.append("  File Offset: \(offset)")
        }
        return lines.joined(separator: "\n")
    }
}

extension ColumnChunkMetadata: CustomStringConvertible {
    public var description: String {
        var lines: [String] = []
        lines.append("Column Chunk:")
        lines.append("  File Offset: \(fileOffset)")
        if let meta = metadata {
            lines.append("  Type: \(meta.physicalType.description)")
            lines.append("  Codec: \(meta.codec.description)")
            lines.append("  Encodings: \(meta.encodings.map { $0.description }.joined(separator: ", "))")
            lines.append("  Values: \(meta.numValues)")
            lines.append("  Compressed Size: \(meta.totalCompressedSize)")
            lines.append("  Uncompressed Size: \(meta.totalUncompressedSize)")
        }
        return lines.joined(separator: "\n")
    }
}
