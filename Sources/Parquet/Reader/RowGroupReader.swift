// RowGroupReader - Read columns from a row group
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Errors that can occur during row group reading.
public enum RowGroupReaderError: Error {
    case columnIndexOutOfBounds(Int, available: Int)
    case typeMismatch(expected: String, actual: String)
    case unsupportedType(String)
}

/// Reader for a single row group in a Parquet file.
///
/// Provides access to typed column readers for reading columnar data.
///
/// Example:
/// ```swift
/// let rowGroup = try reader.rowGroup(at: 0)
///
/// // Type-safe column access
/// let idColumn = try rowGroup.int32Column(at: 0)
/// let ids = try idColumn.readAll()
///
/// let nameColumn = try rowGroup.stringColumn(at: 4)
/// let names = try nameColumn.readAll()
/// ```
///
/// **Phase 1 Limitations:**
/// - PLAIN encoding only (no dictionary support)
/// - GZIP/UNCOMPRESSED only (no Snappy)
/// - Required columns only (no nulls/definition levels)
/// - Primitive types only (no nested types)
///
/// **Phase 2 Work:**
/// - Dictionary encoding support
/// - Snappy compression
/// - Nullable columns (definition levels)
/// - Nested types (lists, maps, structs)
public final class RowGroupReader {
    /// The file being read.
    private let file: RandomAccessFile

    /// The row group metadata.
    public let metadata: RowGroupMetadata

    /// The file schema.
    private let schema: Schema

    /// Initialize a row group reader.
    ///
    /// - Parameters:
    ///   - file: The file to read from
    ///   - metadata: The row group metadata
    ///   - schema: The file schema
    internal init(file: RandomAccessFile, metadata: RowGroupMetadata, schema: Schema) {
        self.file = file
        self.metadata = metadata
        self.schema = schema
    }

    // MARK: - Typed Column Access

    /// Returns an Int32 column reader for the specified column.
    ///
    /// - Parameter index: The column index (0-based)
    /// - Returns: An Int32 column reader
    /// - Throws: RowGroupReaderError if the index is invalid or type doesn't match
    public func int32Column(at index: Int) throws -> Int32ColumnReader {
        try validateColumnIndex(index)
        try validateColumnType(index, expected: .int32)
        return try createInt32ColumnReader(at: index)
    }

    /// Returns an Int64 column reader for the specified column.
    ///
    /// - Parameter index: The column index (0-based)
    /// - Returns: An Int64 column reader
    /// - Throws: RowGroupReaderError if the index is invalid or type doesn't match
    public func int64Column(at index: Int) throws -> Int64ColumnReader {
        try validateColumnIndex(index)
        try validateColumnType(index, expected: .int64)
        return try createInt64ColumnReader(at: index)
    }

    /// Returns a Float column reader for the specified column.
    ///
    /// - Parameter index: The column index (0-based)
    /// - Returns: A Float column reader
    /// - Throws: RowGroupReaderError if the index is invalid or type doesn't match
    public func floatColumn(at index: Int) throws -> FloatColumnReader {
        try validateColumnIndex(index)
        try validateColumnType(index, expected: .float)
        return try createFloatColumnReader(at: index)
    }

    /// Returns a Double column reader for the specified column.
    ///
    /// - Parameter index: The column index (0-based)
    /// - Returns: A Double column reader
    /// - Throws: RowGroupReaderError if the index is invalid or type doesn't match
    public func doubleColumn(at index: Int) throws -> DoubleColumnReader {
        try validateColumnIndex(index)
        try validateColumnType(index, expected: .double)
        return try createDoubleColumnReader(at: index)
    }

    /// Returns a String column reader for the specified column.
    ///
    /// - Parameter index: The column index (0-based)
    /// - Returns: A String column reader
    /// - Throws: RowGroupReaderError if the index is invalid or type doesn't match
    public func stringColumn(at index: Int) throws -> StringColumnReader {
        try validateColumnIndex(index)
        try validateColumnType(index, expected: .byteArray)
        return try createStringColumnReader(at: index)
    }

    // MARK: - Validation

    private func validateColumnIndex(_ index: Int) throws {
        guard index >= 0 && index < metadata.columns.count else {
            throw RowGroupReaderError.columnIndexOutOfBounds(index, available: metadata.columns.count)
        }
    }

    private func validateColumnType(_ index: Int, expected: PhysicalType) throws {
        let column = schema.columns[index]
        guard column.physicalType == expected else {
            throw RowGroupReaderError.typeMismatch(
                expected: expected.description,
                actual: column.physicalType.description
            )
        }
    }

    // MARK: - Column Reader Creation

    private func createInt32ColumnReader(at index: Int) throws -> Int32ColumnReader {
        let columnChunk = metadata.columns[index]
        guard let columnMetadata = columnChunk.metadata else {
            throw RowGroupReaderError.unsupportedType("Column metadata missing")
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        return try Int32ColumnReader(file: file, columnMetadata: columnMetadata, codec: codec)
    }

    private func createInt64ColumnReader(at index: Int) throws -> Int64ColumnReader {
        let columnChunk = metadata.columns[index]
        guard let columnMetadata = columnChunk.metadata else {
            throw RowGroupReaderError.unsupportedType("Column metadata missing")
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        return try Int64ColumnReader(file: file, columnMetadata: columnMetadata, codec: codec)
    }

    private func createFloatColumnReader(at index: Int) throws -> FloatColumnReader {
        let columnChunk = metadata.columns[index]
        guard let columnMetadata = columnChunk.metadata else {
            throw RowGroupReaderError.unsupportedType("Column metadata missing")
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        return try FloatColumnReader(file: file, columnMetadata: columnMetadata, codec: codec)
    }

    private func createDoubleColumnReader(at index: Int) throws -> DoubleColumnReader {
        let columnChunk = metadata.columns[index]
        guard let columnMetadata = columnChunk.metadata else {
            throw RowGroupReaderError.unsupportedType("Column metadata missing")
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        return try DoubleColumnReader(file: file, columnMetadata: columnMetadata, codec: codec)
    }

    private func createStringColumnReader(at index: Int) throws -> StringColumnReader {
        let columnChunk = metadata.columns[index]
        guard let columnMetadata = columnChunk.metadata else {
            throw RowGroupReaderError.unsupportedType("Column metadata missing")
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        return try StringColumnReader(file: file, columnMetadata: columnMetadata, codec: codec)
    }
}
