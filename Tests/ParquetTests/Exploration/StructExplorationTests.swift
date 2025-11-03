// Struct Exploration Tests - Understanding struct schema representation
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class StructExplorationTests: XCTestCase {

    func testSimpleStructSchema() throws {
        // This test explores how structs are represented in Parquet schema
        // File created with PyArrow: struct with name and age fields
        let url = URL(fileURLWithPath: "/tmp/simple_struct.parquet")

        guard FileManager.default.fileExists(atPath: url.path) else {
            throw XCTSkip("Test file not found. Create with generate_struct_fixtures.py")
        }

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        print("\n=== SCHEMA STRUCTURE ===")
        print(reader.metadata.schema.root)

        print("\n=== COLUMNS (Leaf nodes only) ===")
        for (i, column) in reader.metadata.schema.columns.enumerated() {
            print("Column \(i): \(column.path.joined(separator: ".")) - \(column.physicalType.name)")
            print("  repetition: \(column.repetitionType.rawValue)")
            print("  maxDef: \(column.maxDefinitionLevel), maxRep: \(column.maxRepetitionLevel)")
            if let logical = column.logicalType {
                print("  logical: \(logical.name)")
            }
        }

        print("\n=== SCHEMA TREE EXPLORATION ===")
        let root = reader.metadata.schema.root
        print("Root: \(root.name), isGroup: \(root.isGroup), children: \(root.children.count)")

        for child in root.children {
            print("\nChild: \(child.name)")
            print("  isGroup: \(child.isGroup)")
            print("  isLeaf: \(child.isLeaf)")
            print("  elementType: \(child.elementType)")
            if child.isGroup {
                print("  children: \(child.children.count)")
                for grandchild in child.children {
                    print("    - \(grandchild.name): \(grandchild.elementType)")
                }
            }
        }

        print("\n=== READING DATA ===")
        let rowGroup = try reader.rowGroup(at: 0)

        // Try reading columns individually
        let idColumn = try rowGroup.int32Column(at: 0)
        let idValues = try idColumn.readAll()
        print("ID values: \(idValues)")

        let nameColumn = try rowGroup.stringColumn(at: 1)
        let nameValues = try nameColumn.readAll()
        print("Name values: \(nameValues)")

        let ageColumn = try rowGroup.int32Column(at: 2)
        let ageValues = try ageColumn.readAll()
        print("Age values: \(ageValues)")

        // Verify we can read the struct fields correctly
        XCTAssertEqual(idValues.count, 3)
        XCTAssertEqual(nameValues.count, 3)
        XCTAssertEqual(ageValues.count, 3)

        // Test new schema helpers (refactored to use SchemaElement)
        print("\n=== SCHEMA HELPERS (SchemaElement API) ===")

        if let userElement = reader.metadata.schema.element(at: ["user"]) {
            print("Found 'user' element: isStruct=\(userElement.isStruct), isMap=\(userElement.isMap)")
            XCTAssertTrue(userElement.isStruct)
            XCTAssertFalse(userElement.isMap)

            // Test child navigation
            if let nameChild = userElement.child(named: "name") {
                print("  Found child 'name': \(nameChild.name)")
                XCTAssertTrue(nameChild.isLeaf)
            } else {
                XCTFail("Should find 'name' child")
            }
        } else {
            XCTFail("Should find 'user' element")
        }

        if let idElement = reader.metadata.schema.element(at: ["id"]) {
            print("Found 'id' element: isStruct=\(idElement.isStruct), isLeaf=\(idElement.isLeaf)")
            XCTAssertFalse(idElement.isStruct)
            XCTAssertTrue(idElement.isLeaf)
        }

        // Test structFields helper on Schema
        if let fields = reader.metadata.schema.structFields(at: ["user"]) {
            print("\nStruct fields for 'user' (via Schema.structFields):")
            for field in fields {
                print("  - \(field.path.joined(separator: ".")): \(field.physicalType.name)")
            }
            XCTAssertEqual(fields.count, 2)
            XCTAssertTrue(fields.contains(where: { $0.path == ["user", "name"] }))
            XCTAssertTrue(fields.contains(where: { $0.path == ["user", "age"] }))
        } else {
            XCTFail("Should find struct fields for 'user'")
        }
    }
}
