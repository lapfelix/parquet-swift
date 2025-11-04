// MapWritingTests.swift - Integration tests for writing map columns
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

/// Integration tests for writing map columns (Phase 5).
///
/// Tests cover the three supported map types:
/// - map<string, int32>
/// - map<string, int64>
/// - map<string, string>
final class MapWritingTests: XCTestCase {

    // MARK: - Helper Methods

    private func temporaryFileURL() -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let filename = "test_map_\(UUID().uuidString).parquet"
        return tempDir.appendingPathComponent(filename)
    }

    private func cleanupFile(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }

    /// Create a map<string, int32> schema
    private func createStringInt32MapSchema() -> Schema {
        // Level 5: optional int32 value
        let valueElement = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 4
        )

        // Level 4: required string key
        let keyElement = SchemaElement(
            name: "key",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 4
        )

        // Level 3: repeated group key_value
        let keyValueGroup = SchemaElement(
            name: "key_value",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [keyElement, valueElement],
            parent: nil,
            depth: 3
        )
        keyElement.parent = keyValueGroup
        valueElement.parent = keyValueGroup

        // Level 2: optional group attributes (MAP)
        let mapWrapper = SchemaElement(
            name: "attributes",
            elementType: .group(logicalType: .map),
            repetitionType: .optional,
            fieldId: nil,
            children: [keyValueGroup],
            parent: nil,
            depth: 2
        )
        keyValueGroup.parent = mapWrapper

        // Level 1: required int32 id
        let idField = SchemaElement(
            name: "id",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        // Level 0: root schema
        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [idField, mapWrapper],
            parent: nil,
            depth: 0
        )
        idField.parent = root
        mapWrapper.parent = root

        return Schema(root: root)
    }

    // MARK: - map<string, int32> Tests

    func testWriteMapStringInt32Simple() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let schema = createStringInt32MapSchema()

        // Write file with simple maps
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        // Write id column
        let idWriter = try rowGroup.int32ColumnWriter(at: 0)
        try idWriter.writeValues([0, 1, 2])
        try rowGroup.finalizeColumn(at: 0)

        // Write map column
        let mapWriter = try rowGroup.stringInt32MapColumnWriter(at: 1)
        let maps: [[String: Int32]?] = [
            ["a": 1, "b": 2],           // Row 0: 2 entries
            ["x": 10, "y": 20, "z": 30], // Row 1: 3 entries
            ["foo": 100]                 // Row 2: 1 entry
        ]
        try mapWriter.writeMaps(maps)
        try rowGroup.finalizeColumn(at: 1)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 3, "Should have 3 rows")

        let readRowGroup = try reader.rowGroup(at: 0)

        // Read id column
        let readIds = try readRowGroup.int32Column(at: 0).readAll()
        XCTAssertEqual(readIds, [0, 1, 2])

        // Read map column
        let readMaps = try readRowGroup.readMap(at: ["attributes"])
        XCTAssertEqual(readMaps.count, 3)

        // Verify row 0
        guard let map0 = readMaps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 2)
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "a" && $0.value as? Int32 == 1 }))
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "b" && $0.value as? Int32 == 2 }))

        // Verify row 1
        guard let map1 = readMaps[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(map1.count, 3)
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "x" && $0.value as? Int32 == 10 }))
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "y" && $0.value as? Int32 == 20 }))
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "z" && $0.value as? Int32 == 30 }))

        // Verify row 2
        guard let map2 = readMaps[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(map2.count, 1)
        XCTAssertTrue(map2.contains(where: { $0.key as? String == "foo" && $0.value as? Int32 == 100 }))
    }

    func testWriteMapStringInt32WithNulls() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let schema = createStringInt32MapSchema()

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        // Write id column
        let idWriter = try rowGroup.int32ColumnWriter(at: 0)
        try idWriter.writeValues([0, 1, 2, 3])
        try rowGroup.finalizeColumn(at: 0)

        // Write map column with NULL map and empty map
        let mapWriter = try rowGroup.stringInt32MapColumnWriter(at: 1)
        let maps: [[String: Int32]?] = [
            ["a": 1],   // Row 0: present
            nil,        // Row 1: NULL map
            [:],        // Row 2: empty map
            ["b": 2]    // Row 3: present
        ]
        try mapWriter.writeMaps(maps)
        try rowGroup.finalizeColumn(at: 1)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 4)

        let readRowGroup = try reader.rowGroup(at: 0)
        let readMaps = try readRowGroup.readMap(at: ["attributes"])

        XCTAssertEqual(readMaps.count, 4)

        // Row 0: present
        XCTAssertNotNil(readMaps[0])
        XCTAssertEqual(readMaps[0]!.count, 1)

        // Row 1: NULL map
        XCTAssertNil(readMaps[1], "Row 1 should be NULL map")

        // Row 2: empty map
        XCTAssertNotNil(readMaps[2])
        XCTAssertEqual(readMaps[2]!.count, 0, "Row 2 should be empty map")

        // Row 3: present
        XCTAssertNotNil(readMaps[3])
        XCTAssertEqual(readMaps[3]!.count, 1)
    }

    // MARK: - map<string, int64> Tests

    func testWriteMapStringInt64() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create map<string, int64> schema (similar to int32 but with int64)
        let valueElement = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int64, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 4
        )

        let keyElement = SchemaElement(
            name: "key",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 4
        )

        let keyValueGroup = SchemaElement(
            name: "key_value",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [keyElement, valueElement],
            parent: nil,
            depth: 3
        )
        keyElement.parent = keyValueGroup
        valueElement.parent = keyValueGroup

        let mapWrapper = SchemaElement(
            name: "data",
            elementType: .group(logicalType: .map),
            repetitionType: .optional,
            fieldId: nil,
            children: [keyValueGroup],
            parent: nil,
            depth: 1
        )
        keyValueGroup.parent = mapWrapper

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [mapWrapper],
            parent: nil,
            depth: 0
        )
        mapWrapper.parent = root

        let schema = Schema(root: root)

        // Write file
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        let mapWriter = try rowGroup.stringInt64MapColumnWriter(at: 0)
        let maps: [[String: Int64]?] = [
            ["count": 1000, "total": 2000]
        ]
        try mapWriter.writeMaps(maps)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 1)

        let readRowGroup = try reader.rowGroup(at: 0)
        let readMaps = try readRowGroup.readMap(at: ["data"])

        XCTAssertEqual(readMaps.count, 1)
        guard let map0 = readMaps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 2)
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "count" && $0.value as? Int64 == 1000 }))
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "total" && $0.value as? Int64 == 2000 }))
    }

    // MARK: - map<string, string> Tests

    func testWriteMapStringString() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create map<string, string> schema
        let valueElement = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 4
        )

        let keyElement = SchemaElement(
            name: "key",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 4
        )

        let keyValueGroup = SchemaElement(
            name: "key_value",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [keyElement, valueElement],
            parent: nil,
            depth: 3
        )
        keyElement.parent = keyValueGroup
        valueElement.parent = keyValueGroup

        let mapWrapper = SchemaElement(
            name: "metadata",
            elementType: .group(logicalType: .map),
            repetitionType: .optional,
            fieldId: nil,
            children: [keyValueGroup],
            parent: nil,
            depth: 1
        )
        keyValueGroup.parent = mapWrapper

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [mapWrapper],
            parent: nil,
            depth: 0
        )
        mapWrapper.parent = root

        let schema = Schema(root: root)

        // Write file
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        let mapWriter = try rowGroup.stringStringMapColumnWriter(at: 0)
        let maps: [[String: String]?] = [
            ["name": "Alice", "city": "NYC"],
            ["lang": "Swift"]
        ]
        try mapWriter.writeMaps(maps)
        try rowGroup.finalizeColumn(at: 0)

        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 2)

        let readRowGroup = try reader.rowGroup(at: 0)
        let readMaps = try readRowGroup.readMap(at: ["metadata"])

        XCTAssertEqual(readMaps.count, 2)

        // Verify row 0
        guard let map0 = readMaps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 2)
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "name" && $0.value as? String == "Alice" }))
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "city" && $0.value as? String == "NYC" }))

        // Verify row 1
        guard let map1 = readMaps[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(map1.count, 1)
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "lang" && $0.value as? String == "Swift" }))
    }
}
