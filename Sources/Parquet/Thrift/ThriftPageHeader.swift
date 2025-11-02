// Thrift Page Headers - Page metadata in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Data page header (v1).
///
/// Maps to Thrift `DataPageHeader` struct.
public struct ThriftDataPageHeader: Sendable {
    /// Number of values, including NULLs, in this data page
    public let numValues: Int32

    /// Encoding used for this data page
    public let encoding: ThriftEncoding

    /// Encoding used for definition levels
    public let definitionLevelEncoding: ThriftEncoding

    /// Encoding used for repetition levels
    public let repetitionLevelEncoding: ThriftEncoding

    /// Optional statistics for the data in this page
    public let statistics: ThriftStatistics?

    public init(
        numValues: Int32,
        encoding: ThriftEncoding,
        definitionLevelEncoding: ThriftEncoding,
        repetitionLevelEncoding: ThriftEncoding,
        statistics: ThriftStatistics? = nil
    ) {
        self.numValues = numValues
        self.encoding = encoding
        self.definitionLevelEncoding = definitionLevelEncoding
        self.repetitionLevelEncoding = repetitionLevelEncoding
        self.statistics = statistics
    }
}

/// Dictionary page header.
///
/// The dictionary page must be placed at the first position of the column chunk
/// if it is partly or completely dictionary encoded. At most one dictionary page
/// can be placed in a column chunk.
///
/// Maps to Thrift `DictionaryPageHeader` struct.
public struct ThriftDictionaryPageHeader: Sendable {
    /// Number of values in the dictionary
    public let numValues: Int32

    /// Encoding used for this dictionary page
    public let encoding: ThriftEncoding

    /// If true, the entries in the dictionary are sorted in ascending order
    public let isSorted: Bool?

    public init(
        numValues: Int32,
        encoding: ThriftEncoding,
        isSorted: Bool? = nil
    ) {
        self.numValues = numValues
        self.encoding = encoding
        self.isSorted = isSorted
    }
}

/// Data page header (v2).
///
/// New page format allowing reading levels without decompressing the data.
/// Repetition and definition levels are uncompressed.
/// The remaining section containing the data is compressed if is_compressed is true.
///
/// Maps to Thrift `DataPageHeaderV2` struct.
public struct ThriftDataPageHeaderV2: Sendable {
    /// Number of values, including NULLs, in this data page
    public let numValues: Int32

    /// Number of NULL values in this data page
    /// Number of non-null = num_values - num_nulls
    public let numNulls: Int32

    /// Number of rows in this data page
    /// Every page must begin at a row boundary (repetition_level = 0)
    public let numRows: Int32

    /// Encoding used for data in this page
    public let encoding: ThriftEncoding

    /// Length of the definition levels
    public let definitionLevelsByteLength: Int32

    /// Length of the repetition levels
    public let repetitionLevelsByteLength: Int32

    /// Whether the values are compressed
    /// If missing, it is considered compressed
    public let isCompressed: Bool

    /// Optional statistics for the data in this page
    public let statistics: ThriftStatistics?

    public init(
        numValues: Int32,
        numNulls: Int32,
        numRows: Int32,
        encoding: ThriftEncoding,
        definitionLevelsByteLength: Int32,
        repetitionLevelsByteLength: Int32,
        isCompressed: Bool = true,
        statistics: ThriftStatistics? = nil
    ) {
        self.numValues = numValues
        self.numNulls = numNulls
        self.numRows = numRows
        self.encoding = encoding
        self.definitionLevelsByteLength = definitionLevelsByteLength
        self.repetitionLevelsByteLength = repetitionLevelsByteLength
        self.isCompressed = isCompressed
        self.statistics = statistics
    }
}

/// Page header containing type and metadata about a page.
///
/// Maps to Thrift `PageHeader` struct.
public struct ThriftPageHeader: Sendable {
    /// The type of the page (indicates which header field is set)
    public let type: ThriftPageType

    /// Uncompressed page size in bytes (not including this header)
    public let uncompressedPageSize: Int32

    /// Compressed page size in bytes (not including this header)
    public let compressedPageSize: Int32

    /// The 32-bit CRC checksum for the page
    public let crc: Int32?

    /// Headers for page-specific data (only one will be set)
    public let dataPageHeader: ThriftDataPageHeader?
    public let dictionaryPageHeader: ThriftDictionaryPageHeader?
    public let dataPageHeaderV2: ThriftDataPageHeaderV2?

    public init(
        type: ThriftPageType,
        uncompressedPageSize: Int32,
        compressedPageSize: Int32,
        crc: Int32? = nil,
        dataPageHeader: ThriftDataPageHeader? = nil,
        dictionaryPageHeader: ThriftDictionaryPageHeader? = nil,
        dataPageHeaderV2: ThriftDataPageHeaderV2? = nil
    ) {
        self.type = type
        self.uncompressedPageSize = uncompressedPageSize
        self.compressedPageSize = compressedPageSize
        self.crc = crc
        self.dataPageHeader = dataPageHeader
        self.dictionaryPageHeader = dictionaryPageHeader
        self.dataPageHeaderV2 = dataPageHeaderV2
    }
}
