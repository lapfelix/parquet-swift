// Thrift RowGroup - Row group metadata in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Sort order within a RowGroup of a leaf column.
///
/// Maps to Thrift `SortingColumn` struct.
public struct ThriftSortingColumn: Sendable {
    /// The ordinal position of the column (in this row group)
    public let columnIdx: Int32

    /// If true, indicates this column is sorted in descending order
    public let descending: Bool

    /// If true, nulls will come before non-null values, otherwise nulls go at the end
    public let nullsFirst: Bool

    public init(columnIdx: Int32, descending: Bool, nullsFirst: Bool) {
        self.columnIdx = columnIdx
        self.descending = descending
        self.nullsFirst = nullsFirst
    }
}

/// Row group metadata.
///
/// Maps to Thrift `RowGroup` struct.
public struct ThriftRowGroup: Sendable {
    /// Metadata for each column chunk in this row group
    /// This list must have the same order as the SchemaElement list in FileMetaData
    public let columns: [ThriftColumnChunk]

    /// Total byte size of all the uncompressed column data in this row group
    public let totalByteSize: Int64

    /// Number of rows in this row group
    public let numRows: Int64

    /// If set, specifies a sort ordering of the rows in this RowGroup
    /// The sorting columns can be a subset of all the columns
    public let sortingColumns: [ThriftSortingColumn]?

    /// Byte offset from beginning of file to first page (data or dictionary) in this row group
    public let fileOffset: Int64?

    /// Total byte size of all compressed column data in this row group
    public let totalCompressedSize: Int64?

    /// Row group ordinal in the file
    public let ordinal: Int16?

    public init(
        columns: [ThriftColumnChunk],
        totalByteSize: Int64,
        numRows: Int64,
        sortingColumns: [ThriftSortingColumn]? = nil,
        fileOffset: Int64? = nil,
        totalCompressedSize: Int64? = nil,
        ordinal: Int16? = nil
    ) {
        self.columns = columns
        self.totalByteSize = totalByteSize
        self.numRows = numRows
        self.sortingColumns = sortingColumns
        self.fileOffset = fileOffset
        self.totalCompressedSize = totalCompressedSize
        self.ordinal = ordinal
    }
}
