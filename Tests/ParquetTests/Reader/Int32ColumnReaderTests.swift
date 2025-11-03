// Tests for Int32ColumnReader
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class Int32ColumnReaderTests: XCTestCase {
    /// Path to the fixtures directory
    var fixturesURL: URL {
        // Tests/ParquetTests/Fixtures/
        let sourceFile = URL(fileURLWithPath: #file)
        let testsDir = sourceFile.deletingLastPathComponent().deletingLastPathComponent()
        return testsDir.appendingPathComponent("Fixtures")
    }

    // MARK: - Basic Reading Tests

    // BLOCKED: No suitable test fixture for Phase 1
    //
    // Column reader tests require a Parquet file with:
    // - PLAIN encoding only (no dictionary)
    // - UNCOMPRESSED or GZIP compression (SNAPPY not supported in Phase 1)
    // - parquet-mr generated (PyArrow metadata incompatible - see docs/limitations.md)
    //
    // Existing fixtures:
    // - alltypes_plain.parquet: Uses dictionary encoding ❌
    // - datapage_v1-snappy: Uses SNAPPY compression ❌
    // - plain_types.parquet: PyArrow-generated, metadata parse fails ❌
    //
    // TODO: Generate test fixture using parquet-mr tools with:
    //   - WriterVersion.PARQUET_1_0
    //   - Disable dictionary encoding
    //   - Use UNCOMPRESSED or GZIP
    //   - Simple Int32/Int64/Float/Double/String columns
    //
    // Once fixture exists, update these tests to remove XCTSkip.

    func testReadInt32Column() throws {
        throw XCTSkip("Blocked: No PLAIN-only, parquet-mr generated test fixture (see TODO above)")
    }

    func _testReadInt32ColumnDisabled() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")

        // Verify file exists
        XCTAssertTrue(FileManager.default.fileExists(atPath: fileURL.path),
                      "Test fixture not found: \(fileURL.path)")

        // Read metadata
        let metadata = try ParquetFileReader.readMetadata(from: fileURL)

        print("\nFile metadata:")
        print("  Rows: \(metadata.numRows)")
        print("  Row groups: \(metadata.numRowGroups)")
        print("  Schema:")
        for (idx, column) in metadata.schema.columns.enumerated() {
            print("    [\(idx)] \(column.path.joined(separator: ".")) - \(column.physicalType.description)")
        }

        // Find an Int32 column that uses PLAIN encoding only (no dictionary)
        var int32ColumnIndex: Int?
        for (index, column) in metadata.schema.columns.enumerated() {
            guard column.physicalType == .int32 else { continue }

            // Check the column's encoding in the first row group
            if let rowGroup = metadata.rowGroups.first,
               index < rowGroup.columns.count,
               let colMetadata = rowGroup.columns[index].metadata {
                // Skip if column uses dictionary encoding
                let hasDictEncoding = colMetadata.encodings.contains(where: {
                    $0 == .plainDictionary || $0 == .rleDictionary
                })
                if !hasDictEncoding {
                    int32ColumnIndex = index
                    break
                }
            }
        }

        guard let int32ColumnIndex = int32ColumnIndex else {
            XCTFail("No Int32 column with PLAIN-only encoding found in test file")
            return
        }

        let column = metadata.schema.columns[int32ColumnIndex]
        print("\nReading Int32 column: \(column.path.joined(separator: "."))")

        // Get first row group
        guard let firstRowGroup = metadata.rowGroups.first else {
            XCTFail("No row groups in file")
            return
        }

        // Get column chunk for the Int32 column
        guard int32ColumnIndex < firstRowGroup.columns.count else {
            XCTFail("Column index out of bounds")
            return
        }

        let columnChunk = firstRowGroup.columns[int32ColumnIndex]
        guard let columnMetadata = columnChunk.metadata else {
            XCTFail("Column metadata missing")
            return
        }

        print("Column chunk file offset: \(columnChunk.fileOffset)")
        print("Column metadata:")
        print("  Type: \(columnMetadata.physicalType.description)")
        print("  Codec: \(columnMetadata.codec.description)")
        print("  Encodings: \(columnMetadata.encodings.map { $0.description }.joined(separator: ", "))")
        print("  Num values: \(columnMetadata.numValues)")
        print("  Data page offset: \(columnMetadata.dataPageOffset)")
        if let dictOffset = columnMetadata.dictionaryPageOffset {
            print("  Dictionary page offset: \(dictOffset)")
        }

        // Create codec
        let codec = try CodecFactory.codec(for: columnMetadata.codec)

        // Open file
        let file = try FileRandomAccessFile(url: fileURL)
        defer { try? file.close() }

        // Create reader
        let reader = try Int32ColumnReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            column: column
        )

        // Read values
        let values = try reader.readAll()

        print("\nRead \(values.count) values")
        print("First 10 values: \(values.prefix(10))")

        // Verify we read the expected number of values
        XCTAssertEqual(values.count, Int(columnMetadata.numValues))
        XCTAssertGreaterThan(values.count, 0)
    }

    func testReadBatch() throws {
        throw XCTSkip("Blocked: No PLAIN-only, parquet-mr generated test fixture (see TODO above)")
    }

    func _testReadBatchDisabled() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let metadata = try ParquetFileReader.readMetadata(from: fileURL)

        // Find an Int32 column that uses PLAIN encoding only (no dictionary)
        var int32ColumnIndex: Int?
        for (index, column) in metadata.schema.columns.enumerated() {
            guard column.physicalType == .int32 else { continue }

            // Check the column's encoding in the first row group
            if let rowGroup = metadata.rowGroups.first,
               index < rowGroup.columns.count,
               let colMetadata = rowGroup.columns[index].metadata {
                // Skip if column uses dictionary encoding
                let hasDictEncoding = colMetadata.encodings.contains(where: {
                    $0 == .plainDictionary || $0 == .rleDictionary
                })
                if !hasDictEncoding {
                    int32ColumnIndex = index
                    break
                }
            }
        }

        guard let int32ColumnIndex = int32ColumnIndex else {
            XCTFail("No Int32 column with PLAIN-only encoding found in test file")
            return
        }

        guard let firstRowGroup = metadata.rowGroups.first else {
            XCTFail("No row groups in file")
            return
        }

        let columnChunk = firstRowGroup.columns[int32ColumnIndex]
        guard let columnMetadata = columnChunk.metadata else {
            XCTFail("Column metadata missing")
            return
        }

        guard let column = metadata.schema.column(at: int32ColumnIndex) else {
            XCTFail("Column schema missing for index \(int32ColumnIndex)")
            return
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        let file = try FileRandomAccessFile(url: fileURL)
        defer { try? file.close() }

        let reader = try Int32ColumnReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            column: column
        )

        // Read in batches
        var allValues: [Int32?] = []
        let batchSize = 100

        while true {
            let batch = try reader.readBatch(count: batchSize)
            if batch.isEmpty {
                break
            }
            allValues.append(contentsOf: batch)
        }

        print("\nRead \(allValues.count) values in batches of \(batchSize)")
        XCTAssertEqual(allValues.count, Int(columnMetadata.numValues))

        // For required columns, all values should be non-nil
        XCTAssertTrue(allValues.allSatisfy { $0 != nil }, "Required column should have no nil values")
    }

    func testReadOne() throws {
        throw XCTSkip("Blocked: No PLAIN-only, parquet-mr generated test fixture (see TODO above)")
    }

    func _testReadOneDisabled() throws {
        let fileURL = fixturesURL.appendingPathComponent("alltypes_plain.parquet")
        let metadata = try ParquetFileReader.readMetadata(from: fileURL)

        // Find an Int32 column that uses PLAIN encoding only (no dictionary)
        var int32ColumnIndex: Int?
        for (index, column) in metadata.schema.columns.enumerated() {
            guard column.physicalType == .int32 else { continue }

            // Check the column's encoding in the first row group
            if let rowGroup = metadata.rowGroups.first,
               index < rowGroup.columns.count,
               let colMetadata = rowGroup.columns[index].metadata {
                // Skip if column uses dictionary encoding
                let hasDictEncoding = colMetadata.encodings.contains(where: {
                    $0 == .plainDictionary || $0 == .rleDictionary
                })
                if !hasDictEncoding {
                    int32ColumnIndex = index
                    break
                }
            }
        }

        guard let int32ColumnIndex = int32ColumnIndex else {
            XCTFail("No Int32 column with PLAIN-only encoding found in test file")
            return
        }

        guard let firstRowGroup = metadata.rowGroups.first else {
            XCTFail("No row groups in file")
            return
        }

        let columnChunk = firstRowGroup.columns[int32ColumnIndex]
        guard let columnMetadata = columnChunk.metadata else {
            XCTFail("Column metadata missing")
            return
        }

        guard let column = metadata.schema.column(at: int32ColumnIndex) else {
            XCTFail("Column schema missing for index \(int32ColumnIndex)")
            return
        }

        let codec = try CodecFactory.codec(for: columnMetadata.codec)
        let file = try FileRandomAccessFile(url: fileURL)
        defer { try? file.close() }

        let reader = try Int32ColumnReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            column: column
        )

        // Read one by one
        var count = 0
        while let _ = try reader.readOne() {
            count += 1

            // Stop after reading a few to keep test fast
            if count >= 10 {
                break
            }
        }

        print("\nRead \(count) values one by one")
        XCTAssertGreaterThan(count, 0)
    }

    // MARK: - Error Handling Tests

    func testUnsupportedEncoding() throws {
        // This test would need a file with non-PLAIN encoding
        // For now, we'll skip since we only support PLAIN in Phase 1
    }
}
