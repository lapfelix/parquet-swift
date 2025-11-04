// PyArrowValidationTests.swift - Generate files for PyArrow cross-validation
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

/// Generates Parquet files for external validation with PyArrow.
///
/// This test writes various data structures (lists, maps, structs) to files
/// that can be read by PyArrow to verify cross-implementation compatibility.
final class PyArrowValidationTests: XCTestCase {

    private let validationDir = FileManager.default.temporaryDirectory.appendingPathComponent("parquet-validation")

    override func setUp() {
        super.setUp()
        // Create validation directory
        try? FileManager.default.createDirectory(at: validationDir, withIntermediateDirectories: true)
        print("Validation files will be written to: \(validationDir.path)")
    }

    // MARK: - List Validation Files

    func testGenerateListFile() throws {
        let url = validationDir.appendingPathComponent("lists.parquet")

        // Create schema for list<int32>
        let elementField = SchemaElement(
            name: "element",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 3
        )

        let listGroup = SchemaElement(
            name: "list",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [elementField],
            parent: nil,
            depth: 2
        )
        elementField.parent = listGroup

        let listWrapper = SchemaElement(
            name: "numbers",
            elementType: .group(logicalType: .list),
            repetitionType: .optional,
            fieldId: nil,
            children: [listGroup],
            parent: nil,
            depth: 1
        )
        listGroup.parent = listWrapper

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [listWrapper],
            parent: nil,
            depth: 0
        )
        listWrapper.parent = root

        let schema = Schema(root: root)

        // Write file
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.int32ListColumnWriter(at: 0)

        // Test data with various list patterns
        let lists: [[Int32]?] = [
            [1, 2, 3],          // Row 0: normal list
            nil,                // Row 1: NULL list
            [],                 // Row 2: empty list
            [42],               // Row 3: single element
            [10, 20, 30, 40]    // Row 4: larger list
        ]

        try listWriter.writeValues(lists)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        print("✓ Generated lists.parquet with 5 rows")
    }

    func testGenerateNestedListFile() throws {
        let url = validationDir.appendingPathComponent("nested_lists.parquet")

        // Create schema for list<list<int32>>
        let int32Element = SchemaElement(
            name: "element",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 5
        )

        let innerListGroup = SchemaElement(
            name: "list",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [int32Element],
            parent: nil,
            depth: 4
        )
        int32Element.parent = innerListGroup

        let innerListWrapper = SchemaElement(
            name: "element",
            elementType: .group(logicalType: .list),
            repetitionType: .optional,
            fieldId: nil,
            children: [innerListGroup],
            parent: nil,
            depth: 3
        )
        innerListGroup.parent = innerListWrapper

        let outerListGroup = SchemaElement(
            name: "list",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [innerListWrapper],
            parent: nil,
            depth: 2
        )
        innerListWrapper.parent = outerListGroup

        let outerListWrapper = SchemaElement(
            name: "matrix",
            elementType: .group(logicalType: .list),
            repetitionType: .optional,
            fieldId: nil,
            children: [outerListGroup],
            parent: nil,
            depth: 1
        )
        outerListGroup.parent = outerListWrapper

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [outerListWrapper],
            parent: nil,
            depth: 0
        )
        outerListWrapper.parent = root

        let schema = Schema(root: root)

        // Write file
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.int32ListColumnWriter(at: 0)

        // Test data: 2-level nested lists
        let lists: [[[Int32]?]?] = [
            [[1, 2], [3]],      // Row 0: normal nested
            nil,                // Row 1: NULL outer
            [nil],              // Row 2: NULL inner
            [[]]                // Row 3: empty inner
        ]

        try listWriter.writeNestedValues(
            lists,
            repeatedAncestorDefLevels: [1, 3],
            nullListDefLevels: [0, 2]
        )

        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        print("✓ Generated nested_lists.parquet with 4 rows")
    }

    // MARK: - Map Validation Files

    func testGenerateMapFile() throws {
        let url = validationDir.appendingPathComponent("maps.parquet")

        // Create schema for map<string, int32>
        let valueElement = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
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
            name: "properties",
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
        let mapWriter = try rowGroup.stringInt32MapColumnWriter(at: 0)

        // Test data with various map patterns
        let maps: [[String: Int32]?] = [
            ["a": 1, "b": 2, "c": 3],   // Row 0: normal map
            nil,                         // Row 1: NULL map
            [:],                         // Row 2: empty map
            ["x": 100]                   // Row 3: single entry
        ]

        try mapWriter.writeMaps(maps)
        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        print("✓ Generated maps.parquet with 4 rows")
    }

    // MARK: - Struct Validation Files

    func testGenerateStructFile() throws {
        let url = validationDir.appendingPathComponent("structs.parquet")

        // Create schema for struct with two fields
        let nameField = SchemaElement(
            name: "name",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 1
        )

        let ageField = SchemaElement(
            name: "age",
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
            repetitionType: nil,
            fieldId: nil,
            children: [nameField, ageField],
            parent: nil,
            depth: 0
        )
        nameField.parent = root
        ageField.parent = root

        let schema = Schema(root: root)

        // Write file
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        // Write struct fields manually (Phase 4 pattern)
        let names: [String?] = ["Alice", "Bob", nil, "Charlie"]
        let ages: [Int32?] = [30, nil, 25, 35]

        let nameWriter = try rowGroup.stringColumnWriter(at: 0)
        try nameWriter.writeOptionalValues(names)
        try rowGroup.finalizeColumn(at: 0)

        let ageWriter = try rowGroup.int32ColumnWriter(at: 1)
        try ageWriter.writeOptionalValues(ages)
        try rowGroup.finalizeColumn(at: 1)

        try writer.close()

        print("✓ Generated structs.parquet with 4 rows")
    }

    // MARK: - Combined Test

    func testGenerateAllValidationFiles() throws {
        // Generate all validation files in sequence
        try testGenerateListFile()
        try testGenerateNestedListFile()
        try testGenerateMapFile()
        try testGenerateStructFile()

        print("\n" + String(repeating: "=", count: 60))
        print("All validation files generated at:")
        print(validationDir.path)
        print(String(repeating: "=", count: 60))
    }
}
