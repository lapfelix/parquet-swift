// Thrift Column Metadata - Column chunk metadata in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Statistics of a given page type and encoding.
///
/// Maps to Thrift `PageEncodingStats` struct.
public struct ThriftPageEncodingStats: Sendable {
    /// The page type (data/dictionary/etc.)
    public let pageType: ThriftPageType

    /// Encoding of the page
    public let encoding: ThriftEncoding

    /// Number of pages of this type with this encoding
    public let count: Int32

    public init(pageType: ThriftPageType, encoding: ThriftEncoding, count: Int32) {
        self.pageType = pageType
        self.encoding = encoding
        self.count = count
    }
}

/// Description for column metadata.
///
/// Maps to Thrift `ColumnMetaData` struct.
public struct ThriftColumnMetaData: Sendable {
    /// Type of this column
    public let type: ThriftType

    /// Set of all encodings used for this column
    /// Used to validate whether we can decode those pages
    public let encodings: [ThriftEncoding]

    /// Path in schema
    public let pathInSchema: [String]

    /// Compression codec
    public let codec: ThriftCompressionCodec

    /// Number of values in this column
    public let numValues: Int64

    /// Total byte size of all uncompressed pages in this column chunk (including headers)
    public let totalUncompressedSize: Int64

    /// Total byte size of all compressed pages in this column chunk (including headers)
    public let totalCompressedSize: Int64

    /// Optional key/value metadata
    public let keyValueMetadata: [ThriftKeyValue]?

    /// Byte offset from beginning of file to first data page
    public let dataPageOffset: Int64

    /// Byte offset from beginning of file to root index page
    public let indexPageOffset: Int64?

    /// Byte offset from beginning of file to first dictionary page
    public let dictionaryPageOffset: Int64?

    /// Optional statistics for this column chunk
    public let statistics: ThriftStatistics?

    /// Set of all encodings used for pages in this column chunk
    public let encodingStats: [ThriftPageEncodingStats]?

    /// Byte offset from beginning of file to Bloom filter data
    public let bloomFilterOffset: Int64?

    /// Size of Bloom filter data including the serialized header, in bytes
    public let bloomFilterLength: Int32?

    public init(
        type: ThriftType,
        encodings: [ThriftEncoding],
        pathInSchema: [String],
        codec: ThriftCompressionCodec,
        numValues: Int64,
        totalUncompressedSize: Int64,
        totalCompressedSize: Int64,
        keyValueMetadata: [ThriftKeyValue]? = nil,
        dataPageOffset: Int64,
        indexPageOffset: Int64? = nil,
        dictionaryPageOffset: Int64? = nil,
        statistics: ThriftStatistics? = nil,
        encodingStats: [ThriftPageEncodingStats]? = nil,
        bloomFilterOffset: Int64? = nil,
        bloomFilterLength: Int32? = nil
    ) {
        self.type = type
        self.encodings = encodings
        self.pathInSchema = pathInSchema
        self.codec = codec
        self.numValues = numValues
        self.totalUncompressedSize = totalUncompressedSize
        self.totalCompressedSize = totalCompressedSize
        self.keyValueMetadata = keyValueMetadata
        self.dataPageOffset = dataPageOffset
        self.indexPageOffset = indexPageOffset
        self.dictionaryPageOffset = dictionaryPageOffset
        self.statistics = statistics
        self.encodingStats = encodingStats
        self.bloomFilterOffset = bloomFilterOffset
        self.bloomFilterLength = bloomFilterLength
    }
}

/// Column chunk metadata.
///
/// Maps to Thrift `ColumnChunk` struct.
public struct ThriftColumnChunk: Sendable {
    /// File where column data is stored
    /// If not set, assumed to be same file as metadata
    public let filePath: String?

    /// DEPRECATED: Byte offset in file_path to the ColumnMetaData
    /// This field should be set to 0 if no ColumnMetaData has been written outside the footer
    public let fileOffset: Int64

    /// Column metadata for this chunk
    /// Note: while marked as optional, this field is required by most implementations
    public let metaData: ThriftColumnMetaData?

    /// File offset of ColumnChunk's OffsetIndex
    public let offsetIndexOffset: Int64?

    /// Size of ColumnChunk's OffsetIndex, in bytes
    public let offsetIndexLength: Int32?

    /// File offset of ColumnChunk's ColumnIndex
    public let columnIndexOffset: Int64?

    /// Size of ColumnChunk's ColumnIndex, in bytes
    public let columnIndexLength: Int32?

    public init(
        filePath: String? = nil,
        fileOffset: Int64 = 0,
        metaData: ThriftColumnMetaData? = nil,
        offsetIndexOffset: Int64? = nil,
        offsetIndexLength: Int32? = nil,
        columnIndexOffset: Int64? = nil,
        columnIndexLength: Int32? = nil
    ) {
        self.filePath = filePath
        self.fileOffset = fileOffset
        self.metaData = metaData
        self.offsetIndexOffset = offsetIndexOffset
        self.offsetIndexLength = offsetIndexLength
        self.columnIndexOffset = columnIndexOffset
        self.columnIndexLength = columnIndexLength
    }
}
