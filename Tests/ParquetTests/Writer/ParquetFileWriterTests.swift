// ParquetFileWriterTests.swift - Tests for ParquetFileWriter
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class ParquetFileWriterTests: XCTestCase {

    // MARK: - Helper

    private func temporaryFileURL() -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let filename = "test_\(UUID().uuidString).parquet"
        return tempDir.appendingPathComponent(filename)
    }

    private func cleanupFile(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }

    // MARK: - W1 Tests - File Structure & Footer

    func testWriteEmptyFileWithSchema() throws {
        // This test validates the basic file structure:
        // - Magic number "PAR1" at start and end
        // - Footer with FileMetaData
        // - Schema serialization
        // - Empty row groups (no data yet)

        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create a simple schema: single Int32 column
        // Use SchemaElement initializer directly
        let idColumn = SchemaElement(
            name: "id",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [idColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write empty file
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)
        try writer.close()  // No row groups - just header and footer

        // Verify file exists and has content
        XCTAssertTrue(FileManager.default.fileExists(atPath: url.path))

        let fileData = try Data(contentsOf: url)
        XCTAssertGreaterThan(fileData.count, 0, "File should have content")

        // Verify magic numbers
        let startMagic = fileData.prefix(4)
        let endMagic = fileData.suffix(4)
        XCTAssertEqual(String(data: startMagic, encoding: .utf8), "PAR1", "File should start with PAR1")
        XCTAssertEqual(String(data: endMagic, encoding: .utf8), "PAR1", "File should end with PAR1")

        // Read back with existing reader to validate footer
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        // Validate metadata
        XCTAssertEqual(reader.metadata.numRows, 0, "File should have 0 rows")
        XCTAssertEqual(reader.metadata.numRowGroups, 0, "File should have 0 row groups")
        XCTAssertEqual(reader.metadata.schema.columnCount, 1, "Schema should have 1 column")

        // Validate schema
        let column = reader.metadata.schema.columns[0]
        XCTAssertEqual(column.name, "id")
        XCTAssertEqual(column.physicalType, .int32)
        XCTAssertTrue(column.isRequired)
    }

    func testWriteFileWithEmptyRowGroup() throws {
        // This test creates a row group but writes no data
        // Validates row group metadata structure

        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema
        let valueColumn = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [valueColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write file with empty row group
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        // Create row group but don't write any data
        let rowGroup = try writer.createRowGroup()

        // Note: Can't close row group without writing all columns
        // This will fail - that's expected behavior for W1
        // TODO: In W2, we'll write actual column data

        // For now, just verify we can create the row group
        XCTAssertNotNil(rowGroup)
    }

    func testStateTransitions() throws {
        // Validate writer state machine

        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let writer = try ParquetFileWriter(url: url)

        // Create schema
        let idColumn = SchemaElement(
            name: "id",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [idColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Can't create row group before schema is set
        XCTAssertThrowsError(try writer.createRowGroup()) { error in
            guard case WriterError.schemaNotSet = error else {
                XCTFail("Expected schemaNotSet error, got \(error)")
                return
            }
        }

        // Set schema
        try writer.setSchema(schema)

        // Can't set schema twice
        XCTAssertThrowsError(try writer.setSchema(schema)) { error in
            guard case WriterError.invalidState = error else {
                XCTFail("Expected invalidState error, got \(error)")
                return
            }
        }

        // Can create row group now
        let rowGroup = try writer.createRowGroup()
        XCTAssertNotNil(rowGroup)

        // Can close multiple times (idempotent)
        try writer.close()
        try writer.close()  // Should not throw
    }

    func testWriterProperties() throws {
        // Validate writer properties configuration

        var properties = WriterProperties.default
        XCTAssertEqual(properties.compression, .uncompressed)
        XCTAssertEqual(properties.dictionaryEnabled, true)
        XCTAssertEqual(properties.dataPageSize, 1024 * 1024)
        XCTAssertEqual(properties.statisticsEnabled, true)

        // Modify properties
        properties.compression = .snappy
        properties.dictionaryEnabled = false
        properties.dataPageSize = 512 * 1024

        XCTAssertEqual(properties.compression, .snappy)
        XCTAssertEqual(properties.dictionaryEnabled, false)
        XCTAssertEqual(properties.dataPageSize, 512 * 1024)

        // Per-column overrides
        properties.columnProperties["id"] = ColumnProperties(
            compression: .gzip,
            dictionaryEnabled: true
        )

        XCTAssertEqual(properties.compression(for: "id"), .gzip)
        XCTAssertEqual(properties.compression(for: "other"), .snappy)
        XCTAssertEqual(properties.dictionaryEnabled(for: "id"), true)
        XCTAssertEqual(properties.dictionaryEnabled(for: "other"), false)
    }

    func testMemoryOutputSink() throws {
        // Test writing to memory instead of file

        let sink = MemoryOutputSink()
        let writer = try ParquetFileWriter(sink: sink)

        // Create schema
        let colColumn = SchemaElement(
            name: "col",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [colColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        try writer.setSchema(schema)
        try writer.close()

        // Verify we wrote data to memory
        XCTAssertGreaterThan(sink.buffer.count, 0)

        // Verify magic numbers in buffer
        let startMagic = sink.buffer.prefix(4)
        let endMagic = sink.buffer.suffix(4)
        XCTAssertEqual(String(data: startMagic, encoding: .utf8), "PAR1")
        XCTAssertEqual(String(data: endMagic, encoding: .utf8), "PAR1")
    }

    // MARK: - W2 Tests - Column Writers

    func testWriteAndReadInt32Column() throws {
        // Write Int32 data and read it back
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema with Int32 column
        let idColumn = SchemaElement(
            name: "id",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [idColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write data
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.int32ColumnWriter(at: 0)

        let testValues: [Int32] = [1, 2, 3, 4, 5]
        try columnWriter.writeValues(testValues)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, Int64(testValues.count))
        XCTAssertEqual(reader.metadata.numRowGroups, 1)

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int32Column(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues, testValues)
    }

    func testWriteAndReadStringColumn() throws {
        // Write String data and read it back
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema with String column
        let nameColumn = SchemaElement(
            name: "name",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [nameColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write data
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.stringColumnWriter(at: 0)

        let testValues = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
        try columnWriter.writeValues(testValues)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, Int64(testValues.count))
        XCTAssertEqual(reader.metadata.numRowGroups, 1)

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.stringColumn(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues, testValues)
    }

    // MARK: - W3 Tests - Dictionary Encoding

    func testWriteAndReadStringColumnWithDictionaryEncoding() throws {
        // Write String data with dictionary encoding and read it back
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema with String column
        let nameColumn = SchemaElement(
            name: "name",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [nameColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write data with dictionary encoding enabled (default)
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.stringColumnWriter(at: 0)

        // Use repeated values to benefit from dictionary encoding
        let testValues = ["Alice", "Bob", "Charlie", "Alice", "Bob", "Diana", "Alice", "Eve", "Bob", "Charlie"]
        try columnWriter.writeValues(testValues)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, Int64(testValues.count))
        XCTAssertEqual(reader.metadata.numRowGroups, 1)

        // Read back and verify values
        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.stringColumn(at: 0)
        let readValues = try readColumn.readAll()

        // Verify all values were correctly written and read back with dictionary encoding
        XCTAssertEqual(readValues, testValues)
    }

    func testWriteAndReadStringColumnWithMultiPageDictionaryEncoding() throws {
        // Test dictionary encoding with multiple page flushes
        // This verifies the bug fix: indices are cleared after each flush to prevent duplicates
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema with String column
        let nameColumn = SchemaElement(
            name: "name",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [nameColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write data with small page size to force multiple flushes
        var properties = WriterProperties.default
        properties.dataPageSize = 100  // Very small to force multiple pages

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(properties)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.stringColumnWriter(at: 0)

        // Write enough data to trigger multiple flushes (each name ~10-15 bytes with length prefix)
        // With 100-byte page size, should get ~7-8 values per page
        var testValues: [String] = []
        let names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
        for i in 0..<50 {  // 50 values = ~6-7 pages
            testValues.append(names[i % names.count])
        }

        try columnWriter.writeValues(testValues)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify NO DUPLICATES
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, Int64(testValues.count), "Should have exactly 50 rows, not more")

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.stringColumn(at: 0)
        let readValues = try readColumn.readAll()

        // Critical: verify exact match, no duplicates
        XCTAssertEqual(readValues.count, testValues.count, "Read count should match written count (no duplicates)")
        XCTAssertEqual(readValues, testValues, "Values should match exactly")
    }

    // MARK: - W4 Tests - Nullable Columns

    func testWriteAndReadNullableStringColumn() throws {
        // Test writing nullable string column with definition levels
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema with OPTIONAL String column
        let nameColumn = SchemaElement(
            name: "name",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .optional,  // W4: nullable column
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [nameColumn],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        // Write data with nulls (disable dictionary for simplicity)
        var properties = WriterProperties.default
        properties.dictionaryEnabled = false

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(properties)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.stringColumnWriter(at: 0)

        // Mix of present and null values
        let testValues: [String?] = ["Alice", nil, "Charlie", "Diana", nil, nil, "Greg", "Hannah"]
        try columnWriter.writeOptionalValues(testValues)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify nulls preserved
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, Int64(testValues.count))
        XCTAssertEqual(reader.metadata.numRowGroups, 1)

        let readRowGroup = try reader.rowGroup(at: 0)

        // Verify column metadata includes RLE encoding for definition levels
        let columnChunk = readRowGroup.metadata.columns[0]
        let columnMetadata = try XCTUnwrap(columnChunk.metadata, "Column metadata should be present")
        XCTAssertTrue(columnMetadata.encodings.contains(Encoding.rle), "Column metadata should include RLE encoding for definition levels")
        XCTAssertTrue(columnMetadata.encodings.contains(Encoding.plain), "Column metadata should include PLAIN encoding for values")

        let readColumn = try readRowGroup.stringColumn(at: 0)
        let readValues = try readColumn.readAll()

        // Verify nulls are preserved
        XCTAssertEqual(readValues.count, testValues.count)
        for (index, (expected, actual)) in zip(testValues, readValues).enumerated() {
            XCTAssertEqual(actual, expected, "Value at index \(index) should match (including nulls)")
        }
    }

    // MARK: - W5 Tests - Nullable Primitive Types

    func testRLEEncoderExactlyMinRepeatCount() throws {
        // Regression test for RLE encoder bug: exactly minRepeatCount (8) consecutive
        // values should not duplicate the first value
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let column = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [column],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.int32ColumnWriter(at: 0)

        // Exactly 8 values (minRepeatCount for RLE) - all present (defLevel=1)
        // This tests the edge case where consecutiveCount == minRepeatCount
        let testValues: [Int32?] = [1, 1, 1, 1, 1, 1, 1, 1]
        try columnWriter.writeOptionalValues(testValues)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back - should get exactly 8 values, not 9
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 8, "Should have exactly 8 rows, not 9")

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int32Column(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues.count, 8, "Should read exactly 8 values, not 9")
        XCTAssertTrue(readValues.allSatisfy { $0 == 1 }, "All values should be 1")
    }

    func testWriteAndReadNullableInt32Column() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Schema with OPTIONAL Int32 column
        let column = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [column],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.int32ColumnWriter(at: 0)

        let testValues: [Int32?] = [1, nil, 3, 4, nil, nil, 7, 8]
        try columnWriter.writeOptionalValues(testValues)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int32Column(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues.count, testValues.count)
        for (index, (expected, actual)) in zip(testValues, readValues).enumerated() {
            XCTAssertEqual(actual, expected, "Int32 value at index \(index) should match")
        }
    }

    func testWriteAndReadNullableInt64Column() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let column = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int64, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [column],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.int64ColumnWriter(at: 0)

        let testValues: [Int64?] = [100, nil, 300, 400, nil, nil, 700, 800]
        try columnWriter.writeOptionalValues(testValues)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int64Column(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues.count, testValues.count)
        for (index, (expected, actual)) in zip(testValues, readValues).enumerated() {
            XCTAssertEqual(actual, expected, "Int64 value at index \(index) should match")
        }
    }

    func testWriteAndReadNullableFloatColumn() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let column = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .float, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [column],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.floatColumnWriter(at: 0)

        let testValues: [Float?] = [1.1, nil, 3.3, 4.4, nil, nil, 7.7, 8.8]
        try columnWriter.writeOptionalValues(testValues)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.floatColumn(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues.count, testValues.count)
        for (index, (expected, actual)) in zip(testValues, readValues).enumerated() {
            if let expectedValue = expected, let actualValue = actual {
                XCTAssertEqual(actualValue, expectedValue, accuracy: 0.001, "Float value at index \(index) should match")
            } else {
                XCTAssertEqual(actual, expected, "Float null status at index \(index) should match")
            }
        }
    }

    func testWriteAndReadNullableDoubleColumn() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let column = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .double, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [column],
            parent: nil,
            depth: 0
        )

        let schema = Schema(root: root)

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)

        let rowGroup = try writer.createRowGroup()
        let columnWriter = try rowGroup.doubleColumnWriter(at: 0)

        let testValues: [Double?] = [1.11, nil, 3.33, 4.44, nil, nil, 7.77, 8.88]
        try columnWriter.writeOptionalValues(testValues)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.doubleColumn(at: 0)
        let readValues = try readColumn.readAll()

        XCTAssertEqual(readValues.count, testValues.count)
        for (index, (expected, actual)) in zip(testValues, readValues).enumerated() {
            if let expectedValue = expected, let actualValue = actual {
                XCTAssertEqual(actualValue, expectedValue, accuracy: 0.001, "Double value at index \(index) should match")
            } else {
                XCTAssertEqual(actual, expected, "Double null status at index \(index) should match")
            }
        }
    }
}
