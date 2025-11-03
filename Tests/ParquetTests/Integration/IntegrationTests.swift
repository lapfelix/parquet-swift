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

            // Snappy is now supported! (Phase 2)
            // No need to skip Snappy-compressed columns

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

    func testSnappyCompressedFile() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        // Open Snappy-compressed file
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        print("\nSnappy-compressed file:")
        print("  Rows: \(reader.metadata.numRows)")
        print("  Columns: \(reader.metadata.schema.columnCount)")
        print("  Row groups: \(reader.metadata.numRowGroups)")

        // Verify metadata
        XCTAssertGreaterThan(reader.metadata.numRows, 0)
        XCTAssertEqual(reader.metadata.numRows, 5120)
        XCTAssertGreaterThan(reader.metadata.numRowGroups, 0)

        // Access row group
        let rowGroup = try reader.rowGroup(at: 0)
        XCTAssertGreaterThan(rowGroup.metadata.numRows, 0)

        // Check that columns use Snappy compression
        var foundSnappy = false
        for (index, column) in rowGroup.metadata.columns.enumerated() {
            if let metadata = column.metadata {
                print("  Column \(index) codec: \(metadata.codec)")
                if metadata.codec == .snappy {
                    foundSnappy = true
                }
            }
        }

        XCTAssertTrue(foundSnappy, "File should have at least one column with Snappy compression")
        print("  ✓ Successfully opened and read metadata from Snappy-compressed file!")
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

    // MARK: - PyArrow Compatibility Tests

    func testPyArrowGeneratedFile() throws {
        let fileURL = fixturesURL.appendingPathComponent("pyarrow_test.parquet")

        print("\nTesting PyArrow-generated file compatibility:")
        print("  File: \(fileURL.lastPathComponent)")

        // Verify file exists
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "PyArrow test fixture not found: \(fileURL.path)")

        do {
            // Try to read metadata
            let metadata = try ParquetFileReader.readMetadata(from: fileURL)

            print("  ✅ Metadata parsed successfully!")
            print("     Version: \(metadata.version)")
            print("     Rows: \(metadata.numRows)")
            print("     Columns: \(metadata.schema.columnCount)")
            print("     Row groups: \(metadata.numRowGroups)")
            if let createdBy = metadata.createdBy {
                print("     Created by: \(createdBy)")
            }

            // Verify we got all the data
            XCTAssertEqual(metadata.numRows, 5, "Should have 5 rows")
            XCTAssertEqual(metadata.numRowGroups, 1, "Should have 1 row group")
            XCTAssertEqual(metadata.schema.columnCount, 3, "Should have 3 columns")

        } catch {
            print("  ❌ ERROR: \(error)")
            XCTFail("Failed to read PyArrow file: \(error)")
        }
    }

    // MARK: - Phase 3: Nullable Column Tests

    func testReadDataFromWorkingFile() throws {
        let fileURL = fixturesURL.appendingPathComponent("datapage_v1-snappy-compressed-checksum.parquet")

        print("\nTesting data reading from datapage_v1-snappy file:")

        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        print("  Row group rows: \(rowGroup.metadata.numRows)")

        // Try to read from column 0 (INT32, PLAIN encoding, no dictionary)
        let columnReader = try rowGroup.int32Column(at: 0)
        print("  Column reader created: ✓")

        // Actually try to READ data
        print("  Attempting to read first value...")
        if let firstValue = try columnReader.readOne() {
            print("  First value: \(String(describing: firstValue)) ✓")
        } else {
            print("  No values")
        }

        // Read a few more
        let batch = try columnReader.readBatch(count: 5)
        print("  Read batch of \(batch.count) values ✓")
    }

    func testAlltypesPlainFileAccess() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")

        print("\nTesting basic access to alltypes_plain.parquet:")

        // Verify file exists
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "Test fixture not found: \(fileURL.path)")

        // Open file
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        print("  File opened: ✓")
        print("  Rows: \(reader.metadata.numRows)")
        print("  Columns: \(reader.metadata.schema.columnCount)")

        // Access row group
        let rowGroup = try reader.rowGroup(at: 0)
        print("  Row group accessed: ✓")

        // Check column 0 metadata
        let col0 = rowGroup.metadata.columns[0]
        print("\n  Column 0 metadata:")
        if let meta = col0.metadata {
            print("    Physical type: \(meta.physicalType)")
            print("    Codec: \(meta.codec)")
            print("    Encodings: \(meta.encodings.map { $0.description }.joined(separator: ", "))")
            print("    Total compressed size: \(meta.totalCompressedSize)")
            print("    Data page offset: \(meta.dataPageOffset)")
            if let dictOffset = meta.dictionaryPageOffset {
                print("    Dictionary page offset: \(dictOffset)")
            }
        }

        // Try to create column reader (this is where it might fail)
        print("\n  Creating column reader...")
        let columnReader = try rowGroup.int32Column(at: 0)
        print("  Column reader created: ✓")

        // Try to read one value (this is where it actually fails)
        print("\n  Reading first value...")
        if let firstValue = try columnReader.readOne() {
            print("  First value read: \(String(describing: firstValue))")
        } else {
            print("  No values available")
        }
    }

    func testNullableColumnsMetadata() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")

        // Verify file exists
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "Test fixture not found: \(fileURL.path)")

        // Read metadata
        let metadata = try ParquetFileReader.readMetadata(from: fileURL)
        let schema = metadata.schema

        print("\nNullable columns file metadata:")
        print("  Rows: \(metadata.numRows)")
        print("  Columns: \(schema.columnCount)")
        if let createdBy = metadata.createdBy {
            print("  Created by: \(createdBy)")
        }

        // Verify all columns are nullable (maxDefinitionLevel > 0)
        var nullableCount = 0
        print("\nColumn nullability:")
        for (index, column) in schema.columns.enumerated() {
            let pathStr = column.path.joined(separator: ".")
            print("  Column \(index): \(pathStr) (\(column.physicalType.name))", terminator: "")
            print(" - maxDefLevel: \(column.maxDefinitionLevel), maxRepLevel: \(column.maxRepetitionLevel)")

            if column.maxDefinitionLevel > 0 {
                nullableCount += 1
            }
        }

        print("\nNullable columns: \(nullableCount) of \(schema.columnCount)")
        XCTAssertGreaterThan(nullableCount, 0, "File should have nullable columns for testing")
    }

    func testReadNullableInt32Column() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        // Find the 'id' column (INT32, nullable)
        guard let idColumn = schema.columns.first(where: { $0.name == "id" }) else {
            XCTFail("Could not find 'id' column")
            return
        }

        guard let columnIndex = schema.columns.firstIndex(where: { $0.name == "id" }) else {
            XCTFail("Could not find column index")
            return
        }

        print("\nReading nullable Int32 column 'id':")
        print("  Physical type: \(idColumn.physicalType.name)")
        print("  Max definition level: \(idColumn.maxDefinitionLevel)")
        print("  Max repetition level: \(idColumn.maxRepetitionLevel)")

        // Read the column
        let columnReader = try rowGroup.int32Column(at: columnIndex)
        let values = try columnReader.readAll()

        print("  Values read: \(values.count)")
        let nullCount = values.filter { $0 == nil }.count
        let nonNullCount = values.filter { $0 != nil }.count
        print("  NULL values: \(nullCount)")
        print("  Non-NULL values: \(nonNullCount)")

        // Print first few values
        print("  First values: \(values.prefix(10))")

        // Verify results
        XCTAssertGreaterThan(values.count, 0, "Should read some values")
        XCTAssertEqual(values.count, Int(rowGroup.metadata.numRows), "Should read all rows")

        // For this test file, we expect some non-null values
        XCTAssertGreaterThan(nonNullCount, 0, "Should have some non-NULL values")
    }

    func testReadNullableInt64Column() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        // Find the 'bigint_col' column (INT64, nullable)
        guard let columnIndex = schema.columns.firstIndex(where: { $0.name == "bigint_col" }) else {
            XCTFail("Could not find 'bigint_col' column")
            return
        }

        print("\nReading nullable Int64 column 'bigint_col':")

        // Read the column
        let columnReader = try rowGroup.int64Column(at: columnIndex)
        let values = try columnReader.readAll()

        let nullCount = values.filter { $0 == nil }.count
        let nonNullCount = values.filter { $0 != nil }.count
        print("  Total: \(values.count), NULL: \(nullCount), Non-NULL: \(nonNullCount)")
        print("  First values: \(values.prefix(10))")

        XCTAssertEqual(values.count, Int(rowGroup.metadata.numRows))
    }

    func testReadNullableFloatColumn() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        // Find the 'float_col' column (FLOAT, nullable)
        guard let columnIndex = schema.columns.firstIndex(where: { $0.name == "float_col" }) else {
            XCTFail("Could not find 'float_col' column")
            return
        }

        print("\nReading nullable Float column 'float_col':")

        // Read the column
        let columnReader = try rowGroup.floatColumn(at: columnIndex)
        let values = try columnReader.readAll()

        let nullCount = values.filter { $0 == nil }.count
        let nonNullCount = values.filter { $0 != nil }.count
        print("  Total: \(values.count), NULL: \(nullCount), Non-NULL: \(nonNullCount)")
        print("  First values: \(values.prefix(10))")

        XCTAssertEqual(values.count, Int(rowGroup.metadata.numRows))
    }

    func testReadNullableDoubleColumn() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        // Find the 'double_col' column (DOUBLE, nullable)
        guard let columnIndex = schema.columns.firstIndex(where: { $0.name == "double_col" }) else {
            XCTFail("Could not find 'double_col' column")
            return
        }

        print("\nReading nullable Double column 'double_col':")

        // Read the column
        let columnReader = try rowGroup.doubleColumn(at: columnIndex)
        let values = try columnReader.readAll()

        let nullCount = values.filter { $0 == nil }.count
        let nonNullCount = values.filter { $0 != nil }.count
        print("  Total: \(values.count), NULL: \(nullCount), Non-NULL: \(nonNullCount)")
        print("  First values: \(values.prefix(10))")

        XCTAssertEqual(values.count, Int(rowGroup.metadata.numRows))
    }

    func testReadNullableStringColumn() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        // Find the 'string_col' column (BYTE_ARRAY, nullable)
        guard let columnIndex = schema.columns.firstIndex(where: { $0.name == "string_col" }) else {
            XCTFail("Could not find 'string_col' column")
            return
        }

        print("\nReading nullable String column 'string_col':")

        // Read the column
        let columnReader = try rowGroup.stringColumn(at: columnIndex)
        let values = try columnReader.readAll()

        let nullCount = values.filter { $0 == nil }.count
        let nonNullCount = values.filter { $0 != nil }.count
        print("  Total: \(values.count), NULL: \(nullCount), Non-NULL: \(nonNullCount)")
        print("  First values: \(values.prefix(10))")

        XCTAssertEqual(values.count, Int(rowGroup.metadata.numRows))
    }

    func testReadAllNullableColumnTypes() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let reader = try ParquetFileReader(url: fileURL)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let schema = reader.metadata.schema

        print("\nReading all nullable columns:")
        print(String(repeating: "=", count: 60))

        // Test each supported column type
        let testColumns = [
            ("id", "Int32"),
            ("int_col", "Int32"),
            ("bigint_col", "Int64"),
            ("float_col", "Float"),
            ("double_col", "Double"),
            ("string_col", "String")
        ]

        for (columnName, typeName) in testColumns {
            guard let columnIndex = schema.columns.firstIndex(where: { $0.name == columnName }) else {
                print("\n⚠️  Column '\(columnName)' not found - skipping")
                continue
            }

            let column = schema.columns[columnIndex]
            print("\n📊 Column: \(columnName) (\(typeName))")
            print("   Max def level: \(column.maxDefinitionLevel), Max rep level: \(column.maxRepetitionLevel)")

            do {
                switch typeName {
                case "Int32":
                    let reader = try rowGroup.int32Column(at: columnIndex)
                    let values = try reader.readAll()
                    let nulls = values.filter { $0 == nil }.count
                    print("   ✅ Read \(values.count) values (\(nulls) NULLs)")

                case "Int64":
                    let reader = try rowGroup.int64Column(at: columnIndex)
                    let values = try reader.readAll()
                    let nulls = values.filter { $0 == nil }.count
                    print("   ✅ Read \(values.count) values (\(nulls) NULLs)")

                case "Float":
                    let reader = try rowGroup.floatColumn(at: columnIndex)
                    let values = try reader.readAll()
                    let nulls = values.filter { $0 == nil }.count
                    print("   ✅ Read \(values.count) values (\(nulls) NULLs)")

                case "Double":
                    let reader = try rowGroup.doubleColumn(at: columnIndex)
                    let values = try reader.readAll()
                    let nulls = values.filter { $0 == nil }.count
                    print("   ✅ Read \(values.count) values (\(nulls) NULLs)")

                case "String":
                    let reader = try rowGroup.stringColumn(at: columnIndex)
                    let values = try reader.readAll()
                    let nulls = values.filter { $0 == nil }.count
                    print("   ✅ Read \(values.count) values (\(nulls) NULLs)")

                default:
                    print("   ⚠️  Type \(typeName) not tested")
                }
            } catch {
                XCTFail("Failed to read column '\(columnName)': \(error)")
                print("   ❌ Error: \(error)")
            }
        }

        print("\n" + String(repeating: "=", count: 60))
        print("✅ Nullable column testing complete!")
    }
}
