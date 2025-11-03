// Integration tests with real Parquet files
//
// Licensed under the Apache License, Version 2.0
//
// Tests reading metadata and schema from real Parquet files from apache/parquet-testing.
// These tests verify that the Thrift Compact Binary reader correctly handles
// real-world file metadata.

import XCTest
@testable import Parquet

final class IntegrationTests: XCTestCase {
    /// Path to the fixtures directory
    var fixturesURL: URL {
        // Tests/ParquetTests/Fixtures/
        let sourceFile = URL(fileURLWithPath: #file)
        let testsDir = sourceFile.deletingLastPathComponent().deletingLastPathComponent()
        return testsDir.appendingPathComponent("Fixtures")
    }

    // MARK: - Real File Tests

    func testReadDataPageV1Metadata() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        // Verify file exists
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "Test fixture not found: \(fileURL.path)")

        // Read metadata
        let metadata = try ParquetFileReader.readMetadata(from: fileURL)

        // Verify basic metadata
        XCTAssertEqual(metadata.version, 1, "Expected Parquet format version 1")
        XCTAssertGreaterThan(metadata.numRows, 0, "File should have rows")
        XCTAssertGreaterThan(metadata.schema.columnCount, 0, "File should have schema columns")
        XCTAssertGreaterThan(metadata.numRowGroups, 0, "File should have row groups")

        print("File metadata:")
        print("  Version: \(metadata.version)")
        print("  Rows: \(metadata.numRows)")
        print("  Schema columns: \(metadata.schema.columnCount)")
        print("  Row groups: \(metadata.numRowGroups)")
        if let createdBy = metadata.createdBy {
            print("  Created by: \(createdBy)")
        }
    }

    func testBuildSchemaFromRealFile() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        // Read schema
        let schema = try ParquetFileReader.readSchema(from: fileURL)

        // Verify schema structure
        XCTAssertEqual(schema.root.name, "m") // Real parquet files may use any root name
        XCTAssertTrue(schema.root.isRoot)
        XCTAssertGreaterThan(schema.columnCount, 0, "Schema should have columns")

        print("\nSchema structure:")
        print(schema.description)

        // Verify we can access columns
        for (index, column) in schema.columns.enumerated() {
            let pathStr = column.path.joined(separator: ".")
            print("  Column \(index): \(pathStr) (\(column.physicalType.name))", terminator: "")
            if let logical = column.logicalType {
                print(" - \(logical.name)", terminator: "")
            }
            print(" [\(column.repetitionType.rawValue)]")

            // Verify column has required properties
            XCTAssertFalse(column.name.isEmpty, "Column name should not be empty")
            XCTAssertNotNil(column.physicalType, "Column should have physical type")
            XCTAssertNotNil(column.repetitionType, "Column should have repetition type")
        }
    }

    func testSchemaColumnNavigation() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let schema = try ParquetFileReader.readSchema(from: fileURL)

        // Test column access by index
        guard let firstColumn = schema.column(at: 0) else {
            XCTFail("Should have at least one column")
            return
        }
        XCTAssertFalse(firstColumn.name.isEmpty)

        // Test column access by path
        let column = schema.column(at: [firstColumn.name])
        XCTAssertNotNil(column, "Should find column by path")
        XCTAssertEqual(column?.name, firstColumn.name)

        // Test invalid index
        let invalidColumn = schema.column(at: 9999)
        XCTAssertNil(invalidColumn, "Should return nil for invalid index")
    }

    func testRowGroupMetadata() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let metadata = try ParquetFileReader.readMetadata(from: fileURL)

        guard let firstRowGroup = metadata.rowGroups.first else {
            XCTFail("File should have at least one row group")
            return
        }

        print("\nRow group 0:")
        print("  Rows: \(firstRowGroup.numRows)")
        print("  Total byte size: \(firstRowGroup.totalByteSize)")
        print("  Columns: \(firstRowGroup.columns.count)")

        // Verify row group structure
        XCTAssertGreaterThan(firstRowGroup.numRows, 0)
        XCTAssertGreaterThan(firstRowGroup.totalByteSize, 0)
        XCTAssertGreaterThan(firstRowGroup.columns.count, 0)

        // Check first column chunk
        if let firstChunk = firstRowGroup.columns.first,
           let colMetadata = firstChunk.metadata {
            print("\n  First column:")
            print("    Type: \(colMetadata.physicalType.description)")
            print("    Codec: \(colMetadata.codec.description)")
            print("    Encodings: \(colMetadata.encodings.map { $0.description }.joined(separator: ", "))")
            print("    Num values: \(colMetadata.numValues)")
            print("    Compressed size: \(colMetadata.totalCompressedSize)")

            XCTAssertGreaterThan(colMetadata.numValues, 0)
            XCTAssertGreaterThan(colMetadata.totalCompressedSize, 0)
        }
    }

    // MARK: - Error Handling

    func testInvalidFile() {
        let invalidData = Data([0x00, 0x01, 0x02, 0x03])

        XCTAssertThrowsError(try ParquetFileReader.readMetadata(from: invalidData)) { error in
            guard case ParquetFileError.invalidFile(let msg) = error else {
                XCTFail("Expected invalidFile error")
                return
            }
            XCTAssertTrue(msg.contains("magic") || msg.contains("small"))
        }
    }

    func testNonExistentFile() {
        let nonExistentURL = URL(fileURLWithPath: "/tmp/nonexistent.parquet")

        XCTAssertThrowsError(try ParquetFileReader.readMetadata(from: nonExistentURL)) { error in
            // Should throw an error (file not found)
            XCTAssertNotNil(error)
        }
    }

    // MARK: - Instance-Based API Tests (M1.10)

    func testInstanceBasedFileReader() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        // Open file with instance-based API
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        // Verify metadata access
        XCTAssertEqual(reader.metadata.version, 1)
        XCTAssertGreaterThan(reader.metadata.numRows, 0)
        XCTAssertGreaterThan(reader.metadata.numRowGroups, 0)

        print("\nInstance-based reader:")
        print("  Rows: \(reader.metadata.numRows)")
        print("  Row groups: \(reader.metadata.numRowGroups)")
        print("  Columns: \(reader.metadata.schema.columnCount)")
    }

    func testRowGroupAccess() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        // Access first row group
        let rowGroup = try reader.rowGroup(at: 0)
        XCTAssertNotNil(rowGroup)
        XCTAssertGreaterThan(rowGroup.metadata.numRows, 0)
        XCTAssertGreaterThan(rowGroup.metadata.columns.count, 0)

        print("\nRow group access:")
        print("  Rows in group 0: \(rowGroup.metadata.numRows)")
        print("  Columns in group 0: \(rowGroup.metadata.columns.count)")
    }

    func testRowGroupOutOfBounds() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        // Try to access invalid row group
        XCTAssertThrowsError(try reader.rowGroup(at: 9999)) { error in
            guard case ParquetFileError.invalidFile(let msg) = error else {
                XCTFail("Expected invalidFile error")
                return
            }
            XCTAssertTrue(msg.contains("out of bounds"))
        }
    }

    func testTypedColumnAccess() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        print("\nTesting typed column access:")

        // Find and test each column type
        for (index, column) in schema.columns.enumerated() {
            let pathStr = column.path.joined(separator: ".")
            print("  Column \(index): \(pathStr) (\(column.physicalType.name))")

            // Only test if column uses PLAIN encoding (Phase 1 limitation)
            let columnChunk = rowGroup.metadata.columns[index]
            guard let colMetadata = columnChunk.metadata else { continue }

            let hasPlainEncoding = colMetadata.encodings.contains(.plain)
            let hasDictEncoding = colMetadata.encodings.contains(.rleDictionary)

            // Skip if not PLAIN-only
            if !hasPlainEncoding || hasDictEncoding {
                print("    Skipped: Requires dictionary or non-PLAIN encoding")
                continue
            }

            // Skip if Snappy compressed (Phase 1 limitation)
            if colMetadata.codec == .snappy {
                print("    Skipped: Snappy compression not supported")
                continue
            }

            // Try to access with correct type
            switch column.physicalType {
            case .int32:
                let columnReader = try rowGroup.int32Column(at: index)
                XCTAssertNotNil(columnReader)
                print("    ✓ Int32 column reader created")

            case .int64:
                let columnReader = try rowGroup.int64Column(at: index)
                XCTAssertNotNil(columnReader)
                print("    ✓ Int64 column reader created")

            case .float:
                let columnReader = try rowGroup.floatColumn(at: index)
                XCTAssertNotNil(columnReader)
                print("    ✓ Float column reader created")

            case .double:
                let columnReader = try rowGroup.doubleColumn(at: index)
                XCTAssertNotNil(columnReader)
                print("    ✓ Double column reader created")

            case .byteArray:
                let columnReader = try rowGroup.stringColumn(at: index)
                XCTAssertNotNil(columnReader)
                print("    ✓ String column reader created")

            default:
                print("    Skipped: Type \(column.physicalType.name) not supported in Phase 1")
            }
        }
    }

    func testColumnTypeMismatch() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        // Find an Int32 column
        if let int32Index = schema.columns.firstIndex(where: { $0.physicalType == .int32 }) {
            // Try to read it as Int64 (type mismatch)
            XCTAssertThrowsError(try rowGroup.int64Column(at: int32Index)) { error in
                guard case RowGroupReaderError.typeMismatch = error else {
                    XCTFail("Expected typeMismatch error, got \(error)")
                    return
                }
            }
        }
    }

    func testColumnIndexOutOfBounds() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Try to access invalid column index
        XCTAssertThrowsError(try rowGroup.int32Column(at: 9999)) { error in
            guard case RowGroupReaderError.columnIndexOutOfBounds = error else {
                XCTFail("Expected columnIndexOutOfBounds error, got \(error)")
                return
            }
        }
    }

    func testResourceCleanup() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        // Test explicit close
        do {
            let reader = try ParquetFileReader(url: fileURL)
            XCTAssertNotNil(reader.metadata)
            try reader.close()
            // File should be closed now
        }

        // Test defer cleanup
        do {
            let reader = try ParquetFileReader(url: fileURL)
            defer { try? reader.close() }
            XCTAssertNotNil(reader.metadata)
            // File should be closed when scope exits
        }

        // Test deinit cleanup (file closed automatically)
        do {
            let reader = try ParquetFileReader(url: fileURL)
            XCTAssertNotNil(reader.metadata)
            // File should be closed when reader is deallocated
        }
    }
}
