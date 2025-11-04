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

    // MARK: - Map Column Writers (W7 Phase 5)

    /// Get a map<string, int32> column writer
    /// - Parameter index: Column index (of the key column)
    /// - Returns: A map writer for string keys and int32 values
    /// - Throws: WriterError if column is not part of a map structure
    public func stringInt32MapColumnWriter(at index: Int) throws -> StringInt32MapColumnWriter {
        let (keyColumn, valueColumn) = try validateMapColumnAccess(
            at: index,
            expectedKeyType: .byteArray,
            expectedValueType: .int32
        )

        let startOffset = try sink.tell()

        // Map wrapper column for levels computation
        // The key column's parent is the key_value group, parent's parent is the map wrapper
        guard let mapElement = keyColumn.element.parent?.parent else {
            throw WriterError.invalidState("Cannot find map wrapper for column \(index)")
        }

        // Keys and values have different max definition levels
        // (keys are required, values are optional)
        let keyMaxDefinitionLevel = keyColumn.maxDefinitionLevel
        let valueMaxDefinitionLevel = valueColumn.maxDefinitionLevel
        let maxRepetitionLevel = keyColumn.maxRepetitionLevel

        // repeatedAncestorDefLevel: def level when map is present but empty
        let repeatedAncestorDefLevel = keyColumn.repeatedAncestorDefLevel ?? 0

        // nullMapDefLevel: def level when map is NULL (one less than empty map)
        let nullMapDefLevel = repeatedAncestorDefLevel - 1

        let writer = StringInt32MapColumnWriter(
            column: keyColumn,
            properties: properties,
            sink: sink,
            startOffset: startOffset,
            keyMaxDefinitionLevel: keyMaxDefinitionLevel,
            valueMaxDefinitionLevel: valueMaxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullMapDefLevel: nullMapDefLevel,
            keyColumn: keyColumn,
            valueColumn: valueColumn
        )

        // Store writer for both key and value column indices
        columnWriters[index] = writer
        columnWriters[index + 1] = writer

        return writer
    }

    /// Get a map<string, int64> column writer
    /// - Parameter index: Column index (of the key column)
    /// - Returns: A map writer for string keys and int64 values
    /// - Throws: WriterError if column is not part of a map structure
    public func stringInt64MapColumnWriter(at index: Int) throws -> StringInt64MapColumnWriter {
        let (keyColumn, valueColumn) = try validateMapColumnAccess(
            at: index,
            expectedKeyType: .byteArray,
            expectedValueType: .int64
        )

        let startOffset = try sink.tell()

        // Keys and values have different max definition levels
        let keyMaxDefinitionLevel = keyColumn.maxDefinitionLevel
        let valueMaxDefinitionLevel = valueColumn.maxDefinitionLevel
        let maxRepetitionLevel = keyColumn.maxRepetitionLevel
        let repeatedAncestorDefLevel = keyColumn.repeatedAncestorDefLevel ?? 0
        let nullMapDefLevel = repeatedAncestorDefLevel - 1

        let writer = StringInt64MapColumnWriter(
            column: keyColumn,
            properties: properties,
            sink: sink,
            startOffset: startOffset,
            keyMaxDefinitionLevel: keyMaxDefinitionLevel,
            valueMaxDefinitionLevel: valueMaxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullMapDefLevel: nullMapDefLevel,
            keyColumn: keyColumn,
            valueColumn: valueColumn
        )

        columnWriters[index] = writer
        columnWriters[index + 1] = writer

        return writer
    }

    /// Get a map<string, string> column writer
    /// - Parameter index: Column index (of the key column)
    /// - Returns: A map writer for string keys and string values
    /// - Throws: WriterError if column is not part of a map structure
    public func stringStringMapColumnWriter(at index: Int) throws -> StringStringMapColumnWriter {
        let (keyColumn, valueColumn) = try validateMapColumnAccess(
            at: index,
            expectedKeyType: .byteArray,
            expectedValueType: .byteArray
        )

        let startOffset = try sink.tell()

        // Keys and values have different max definition levels
        let keyMaxDefinitionLevel = keyColumn.maxDefinitionLevel
        let valueMaxDefinitionLevel = valueColumn.maxDefinitionLevel
        let maxRepetitionLevel = keyColumn.maxRepetitionLevel
        let repeatedAncestorDefLevel = keyColumn.repeatedAncestorDefLevel ?? 0
        let nullMapDefLevel = repeatedAncestorDefLevel - 1

        let writer = StringStringMapColumnWriter(
            column: keyColumn,
            properties: properties,
            sink: sink,
            startOffset: startOffset,
            keyMaxDefinitionLevel: keyMaxDefinitionLevel,
            valueMaxDefinitionLevel: valueMaxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullMapDefLevel: nullMapDefLevel,
            keyColumn: keyColumn,
            valueColumn: valueColumn
        )

        columnWriters[index] = writer
        columnWriters[index + 1] = writer

        return writer
    }

    /// Finalize a column writer and store its metadata
    /// - Parameter index: Column index to finalize
    /// - Throws: WriterError if column not found or finalization fails
    func finalizeColumn(at index: Int) throws {
        guard let writerAny = columnWriters[index] else {
            throw WriterError.invalidState("No writer found for column \(index)")
        }

        // Check if this is a map writer (spans 2 columns)
        if let mapWriter = writerAny as? StringInt32MapColumnWriter {
            try finalizeMapWriter(mapWriter, startIndex: index)
            return
        } else if let mapWriter = writerAny as? StringInt64MapColumnWriter {
            try finalizeMapWriter(mapWriter, startIndex: index)
            return
        } else if let mapWriter = writerAny as? StringStringMapColumnWriter {
            try finalizeMapWriter(mapWriter, startIndex: index)
            return
        }

        // Handle primitive and list writers
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

    /// Finalize a map writer (which spans 2 columns: key and value)
    private func finalizeMapWriter<T>(
        _ mapWriter: T,
        startIndex: Int
    ) throws where T: AnyObject {
        // Extract metadata based on map writer type
        let keyMetadata: WriterColumnChunkMetadata
        let valueMetadata: WriterColumnChunkMetadata
        let rowCount: Int64

        if let writer = mapWriter as? StringInt32MapColumnWriter {
            let metadata = try writer.close()
            keyMetadata = metadata.key
            valueMetadata = metadata.value
            rowCount = writer.numRows
        } else if let writer = mapWriter as? StringInt64MapColumnWriter {
            let metadata = try writer.close()
            keyMetadata = metadata.key
            valueMetadata = metadata.value
            rowCount = writer.numRows
        } else if let writer = mapWriter as? StringStringMapColumnWriter {
            let metadata = try writer.close()
            keyMetadata = metadata.key
            valueMetadata = metadata.value
            rowCount = writer.numRows
        } else {
            throw WriterError.invalidState("Unknown map writer type")
        }

        // Row count validation
        if currentColumn == 0 {
            numRows = rowCount
        } else {
            guard numRows == rowCount else {
                throw WriterError.invalidState(
                    "Map columns \(startIndex)-\(startIndex + 1) have \(rowCount) rows, expected \(numRows)"
                )
            }
        }

        // Add metadata for both key and value columns
        columnMetadata.append(keyMetadata)
        columnMetadata.append(valueMetadata)

        // Increment by 2 since map spans 2 columns
        currentColumn += 2
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

    private func validateMapColumnAccess(
        at index: Int,
        expectedKeyType: PhysicalType,
        expectedValueType: PhysicalType
    ) throws -> (keyColumn: Column, valueColumn: Column) {
        guard !isClosed else {
            throw WriterError.invalidState("Row group is closed")
        }

        guard index < schema.columnCount - 1 else {
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

        let keyColumn = schema.columns[index]
        let valueColumn = schema.columns[index + 1]

        // Validate this is a map structure:
        // - Key column's parent should be a repeated group (key_value)
        // - Key column's grandparent should have MAP logical type
        guard let kvGroup = keyColumn.element.parent,
              kvGroup.repetitionType == .repeated,
              let mapWrapper = kvGroup.parent,
              mapWrapper.logicalType == .map else {
            throw WriterError.invalidState(
                "Column \(index) is not part of a map structure (use primitive column writer instead)"
            )
        }

        // Validate key and value are siblings in the key_value group
        guard valueColumn.element.parent === kvGroup else {
            throw WriterError.invalidState(
                "Columns \(index) and \(index + 1) are not key-value siblings in a map"
            )
        }

        // Validate key and value names
        guard keyColumn.name == "key" else {
            throw WriterError.invalidState(
                "Column \(index) is not named 'key' (found '\(keyColumn.name)')"
            )
        }

        guard valueColumn.name == "value" else {
            throw WriterError.invalidState(
                "Column \(index + 1) is not named 'value' (found '\(valueColumn.name)')"
            )
        }

        // Validate types
        guard keyColumn.physicalType == expectedKeyType else {
            throw WriterError.incompatibleType(
                expected: expectedKeyType,
                actual: "\(keyColumn.physicalType)"
            )
        }

        guard valueColumn.physicalType == expectedValueType else {
            throw WriterError.incompatibleType(
                expected: expectedValueType,
                actual: "\(valueColumn.physicalType)"
            )
        }

        return (keyColumn, valueColumn)
    }
}
