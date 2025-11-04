// RowGroupWriter.swift - Row group writer for managing column writers
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Writer for a single row group
///
/// Manages column writers and tracks row group metadata
public final class RowGroupWriter {
    // MARK: - Properties

    private let schema: Schema
    private let properties: WriterProperties
    private let sink: OutputSink
    private let startOffset: Int64
    private let ordinal: Int

    private var columnWriters: [Int: Any] = [:]
    private var columnMetadata: [WriterColumnChunkMetadata] = []
    private var currentColumn: Int = 0
    private var numRows: Int64 = 0
    private var isClosed: Bool = false

    /// Check if any columns have been written (internal for ParquetFileWriter)
    var hasColumnsWritten: Bool {
        return currentColumn > 0
    }

    // MARK: - Initialization

    init(
        schema: Schema,
        properties: WriterProperties,
        sink: OutputSink,
        startOffset: Int64,
        ordinal: Int
    ) {
        self.schema = schema
        self.properties = properties
        self.sink = sink
        self.startOffset = startOffset
        self.ordinal = ordinal
    }

    // MARK: - Column Writers

    /// Get an Int32 column writer
    /// - Parameter index: Column index
    /// - Returns: An Int32 column writer
    /// - Throws: WriterError if column index is invalid or already written
    public func int32ColumnWriter(at index: Int) throws -> Int32ColumnWriter {
        try validateColumnAccess(at: index, expectedType: .int32)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()
        let writer = Int32ColumnWriter(column: column, properties: properties, pageWriter: pageWriter, startOffset: startOffset)

        columnWriters[index] = writer
        return writer
    }

    /// Get an Int64 column writer
    /// - Parameter index: Column index
    /// - Returns: An Int64 column writer
    /// - Throws: WriterError if column index is invalid or already written
    public func int64ColumnWriter(at index: Int) throws -> Int64ColumnWriter {
        try validateColumnAccess(at: index, expectedType: .int64)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()
        let writer = Int64ColumnWriter(column: column, properties: properties, pageWriter: pageWriter, startOffset: startOffset)

        columnWriters[index] = writer
        return writer
    }

    /// Get a Float column writer
    /// - Parameter index: Column index
    /// - Returns: A Float column writer
    /// - Throws: WriterError if column index is invalid or already written
    public func floatColumnWriter(at index: Int) throws -> FloatColumnWriter {
        try validateColumnAccess(at: index, expectedType: .float)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()
        let writer = FloatColumnWriter(column: column, properties: properties, pageWriter: pageWriter, startOffset: startOffset)

        columnWriters[index] = writer
        return writer
    }

    /// Get a Double column writer
    /// - Parameter index: Column index
    /// - Returns: A Double column writer
    /// - Throws: WriterError if column index is invalid or already written
    public func doubleColumnWriter(at index: Int) throws -> DoubleColumnWriter {
        try validateColumnAccess(at: index, expectedType: .double)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()
        let writer = DoubleColumnWriter(column: column, properties: properties, pageWriter: pageWriter, startOffset: startOffset)

        columnWriters[index] = writer
        return writer
    }

    /// Get a String column writer
    /// - Parameter index: Column index
    /// - Returns: A String column writer
    /// - Throws: WriterError if column index is invalid or already written
    public func stringColumnWriter(at index: Int) throws -> StringColumnWriter {
        try validateColumnAccess(at: index, expectedType: .byteArray)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()
        let writer = StringColumnWriter(column: column, properties: properties, pageWriter: pageWriter, startOffset: startOffset)

        columnWriters[index] = writer
        return writer
    }

    // MARK: - List Column Writers (W7)

    /// Get an Int32 list column writer
    /// - Parameter index: Column index
    /// - Returns: An Int32 list column writer
    /// - Throws: WriterError if column index is invalid or not a repeated field
    public func int32ListColumnWriter(at index: Int) throws -> Int32ListColumnWriter {
        try validateListColumnAccess(at: index, expectedType: .int32)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()

        // Compute null list def level based on whether list is optional
        // repeatedAncestorDefLevel is the def level when the list is present but empty
        // NULL list has def level < repeatedAncestorDefLevel
        let repeatedAncestorDefLevel = column.repeatedAncestorDefLevel ?? 0

        // For optional lists: NULL list def = repeatedAncestorDefLevel - 1
        // For required lists (repeatedAncestorDefLevel == 0): use -1 as sentinel
        // The list writer will reject nil lists when nullListDefLevel < 0
        let nullListDefLevel = repeatedAncestorDefLevel - 1

        let writer = Int32ListColumnWriter(
            column: column,
            properties: properties,
            pageWriter: pageWriter,
            startOffset: startOffset,
            maxDefinitionLevel: column.maxDefinitionLevel,
            maxRepetitionLevel: column.maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel
        )

        columnWriters[index] = writer
        return writer
    }

    /// Get an Int64 list column writer
    /// - Parameter index: Column index
    /// - Returns: An Int64 list column writer
    /// - Throws: WriterError if column index is invalid or not a repeated field
    public func int64ListColumnWriter(at index: Int) throws -> Int64ListColumnWriter {
        try validateListColumnAccess(at: index, expectedType: .int64)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()

        let repeatedAncestorDefLevel = column.repeatedAncestorDefLevel ?? 0
        let nullListDefLevel = repeatedAncestorDefLevel - 1

        let writer = Int64ListColumnWriter(
            column: column,
            properties: properties,
            pageWriter: pageWriter,
            startOffset: startOffset,
            maxDefinitionLevel: column.maxDefinitionLevel,
            maxRepetitionLevel: column.maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel
        )

        columnWriters[index] = writer
        return writer
    }

    /// Get a String list column writer
    /// - Parameter index: Column index
    /// - Returns: A String list column writer
    /// - Throws: WriterError if column index is invalid or not a repeated field
    public func stringListColumnWriter(at index: Int) throws -> StringListColumnWriter {
        try validateListColumnAccess(at: index, expectedType: .byteArray)

        let column = schema.columns[index]
        let codec = properties.compression(for: column.name)
        let pageWriter = PageWriter(sink: sink, codec: codec, properties: properties)
        let startOffset = try sink.tell()

        let repeatedAncestorDefLevel = column.repeatedAncestorDefLevel ?? 0
        let nullListDefLevel = repeatedAncestorDefLevel - 1

        let writer = StringListColumnWriter(
            column: column,
            properties: properties,
            pageWriter: pageWriter,
            startOffset: startOffset,
            maxDefinitionLevel: column.maxDefinitionLevel,
            maxRepetitionLevel: column.maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel
        )

        columnWriters[index] = writer
        return writer
    }

    /// Finalize a column writer and store its metadata
    /// - Parameter index: Column index to finalize
    /// - Throws: WriterError if column not found or finalization fails
    func finalizeColumn(at index: Int) throws {
        guard let writerAny = columnWriters[index] else {
            throw WriterError.invalidState("No writer found for column \(index)")
        }

        let metadata: WriterColumnChunkMetadata
        let columnRowCount: Int64

        // Type-switch to call the appropriate close() method and get row count
        if let writer = writerAny as? Int32ColumnWriter {
            metadata = try writer.close()
            columnRowCount = metadata.numValues  // For primitives: numValues == numRows
        } else if let writer = writerAny as? Int64ColumnWriter {
            metadata = try writer.close()
            columnRowCount = metadata.numValues
        } else if let writer = writerAny as? FloatColumnWriter {
            metadata = try writer.close()
            columnRowCount = metadata.numValues
        } else if let writer = writerAny as? DoubleColumnWriter {
            metadata = try writer.close()
            columnRowCount = metadata.numValues
        } else if let writer = writerAny as? StringColumnWriter {
            metadata = try writer.close()
            columnRowCount = metadata.numValues
        } else if let writer = writerAny as? Int32ListColumnWriter {
            metadata = try writer.close()
            columnRowCount = writer.numRows  // For lists: use tracked row count, NOT numValues
        } else if let writer = writerAny as? Int64ListColumnWriter {
            metadata = try writer.close()
            columnRowCount = writer.numRows
        } else if let writer = writerAny as? StringListColumnWriter {
            metadata = try writer.close()
            columnRowCount = writer.numRows
        } else {
            throw WriterError.invalidState("Unknown column writer type")
        }

        // Row count validation: all columns must have same row count
        if currentColumn == 0 {
            numRows = columnRowCount
        } else {
            guard numRows == columnRowCount else {
                throw WriterError.invalidState(
                    "Column \(index) has \(columnRowCount) rows, expected \(numRows)"
                )
            }
        }

        columnMetadata.append(metadata)
        currentColumn += 1
    }

    // MARK: - Row Group Finalization

    /// Close the row group and return metadata
    /// - Returns: Metadata for this row group
    /// - Throws: WriterError if row group is already closed or columns incomplete
    func close() throws -> WriterRowGroupMetadata {
        guard !isClosed else {
            throw WriterError.invalidState("Row group already closed")
        }

        // Validate all columns written
        guard currentColumn == schema.columnCount else {
            throw WriterError.invalidState(
                "Not all columns written: \(currentColumn)/\(schema.columnCount)"
            )
        }

        // Per Parquet spec: totalByteSize is "Total byte size of all the uncompressed column data"
        let totalByteSize = columnMetadata.reduce(Int64(0)) { $0 + $1.totalUncompressedSize }

        isClosed = true

        return WriterRowGroupMetadata(
            numRows: numRows,
            totalByteSize: totalByteSize,
            columns: columnMetadata,
            ordinal: ordinal
        )
    }

    // MARK: - Private Methods

    private func validateColumnAccess(at index: Int, expectedType: PhysicalType) throws {
        guard !isClosed else {
            throw WriterError.invalidState("Row group is closed")
        }

        guard index < schema.columnCount else {
            throw WriterError.columnIndexOutOfBounds(index)
        }

        guard index == currentColumn else {
            throw WriterError.invalidState(
                "Columns must be written sequentially. Expected column \(currentColumn), got \(index)"
            )
        }

        guard !columnWriters.keys.contains(index) else {
            throw WriterError.columnAlreadyWritten(index)
        }

        // Validate type matches
        let column = schema.columns[index]
        guard column.physicalType == expectedType else {
            throw WriterError.incompatibleType(
                expected: expectedType,
                actual: "\(column.physicalType)"
            )
        }
    }

    private func validateListColumnAccess(at index: Int, expectedType: PhysicalType) throws {
        guard !isClosed else {
            throw WriterError.invalidState("Row group is closed")
        }

        guard index < schema.columnCount else {
            throw WriterError.columnIndexOutOfBounds(index)
        }

        guard index == currentColumn else {
            throw WriterError.invalidState(
                "Columns must be written sequentially. Expected column \(currentColumn), got \(index)"
            )
        }

        guard !columnWriters.keys.contains(index) else {
            throw WriterError.columnAlreadyWritten(index)
        }

        let column = schema.columns[index]

        // Validate this is a repeated field (list)
        // In a 3-level list structure, the leaf element itself is optional,
        // but the parent group is repeated, so we check maxRepetitionLevel > 0
        guard column.maxRepetitionLevel > 0 else {
            throw WriterError.invalidState(
                "Column \(index) is not a repeated field (maxRepetitionLevel=0, use primitive column writer instead)"
            )
        }

        // Validate element type matches
        guard column.physicalType == expectedType else {
            throw WriterError.incompatibleType(
                expected: expectedType,
                actual: "\(column.physicalType)"
            )
        }
    }
}
