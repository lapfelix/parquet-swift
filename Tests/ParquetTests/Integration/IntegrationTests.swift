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
        XCTAssertGreaterThan(metadata.schema.count, 0, "File should have schema elements")
        XCTAssertGreaterThan(metadata.rowGroups.count, 0, "File should have row groups")

        print("File metadata:")
        print("  Version: \(metadata.version)")
        print("  Rows: \(metadata.numRows)")
        print("  Schema elements: \(metadata.schema.count)")
        print("  Row groups: \(metadata.rowGroups.count)")
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
           let colMetadata = firstChunk.metaData {
            print("\n  First column:")
            print("    Type: \(colMetadata.type.name)")
            print("    Codec: \(colMetadata.codec.name)")
            print("    Encodings: \(colMetadata.encodings.map { $0.name }.joined(separator: ", "))")
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
}
