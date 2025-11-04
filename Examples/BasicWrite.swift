// BasicWrite.swift - Example of writing a Parquet file
//
// This example demonstrates:
// - Creating a schema
// - Writing primitive columns
// - Writing lists and maps
// - Writing structs (manual field extraction)

import Foundation
import Parquet

func basicWriteExample() throws {
    // Output file path
    let outputURL = URL(fileURLWithPath: "output.parquet")

    // === Create Schema ===
    // Define columns: id (int32, required), name (string, optional), age (int32, optional)

    let idField = SchemaElement(
        name: "id",
        elementType: .primitive(physicalType: .int32, logicalType: nil),
        repetitionType: .required,
        fieldId: nil,
        children: [],
        parent: nil,
        depth: 1
    )

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
        children: [idField, nameField, ageField],
        parent: nil,
        depth: 0
    )
    idField.parent = root
    nameField.parent = root
    ageField.parent = root

    let schema = Schema(root: root)

    // === Create Writer ===
    let writer = try ParquetFileWriter(url: outputURL)
    try writer.setSchema(schema)

    // Set properties (optional, defaults are fine)
    var properties = WriterProperties.default
    properties.compressionType = .snappy  // Use Snappy compression
    writer.setProperties(properties)

    // === Write Data ===
    let rowGroup = try writer.createRowGroup()

    // Sample data
    let ids: [Int32] = [1, 2, 3, 4, 5]
    let names: [String?] = ["Alice", "Bob", nil, "Charlie", "Diana"]
    let ages: [Int32?] = [30, nil, 25, 35, 28]

    // Write ID column (required)
    let idWriter = try rowGroup.int32ColumnWriter(at: 0)
    try idWriter.writeValues(ids)
    try rowGroup.finalizeColumn(at: 0)

    // Write name column (optional)
    let nameWriter = try rowGroup.stringColumnWriter(at: 1)
    try nameWriter.writeOptionalValues(names)
    try rowGroup.finalizeColumn(at: 1)

    // Write age column (optional)
    let ageWriter = try rowGroup.int32ColumnWriter(at: 2)
    try ageWriter.writeOptionalValues(ages)
    try rowGroup.finalizeColumn(at: 2)

    // === Close Writer ===
    try writer.close()

    print("✓ Successfully wrote \(ids.count) rows to \(outputURL.path)")
}

func writeListExample() throws {
    let outputURL = URL(fileURLWithPath: "lists.parquet")

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
    let writer = try ParquetFileWriter(url: outputURL)
    try writer.setSchema(schema)
    writer.setProperties(.default)

    let rowGroup = try writer.createRowGroup()
    let listWriter = try rowGroup.int32ListColumnWriter(at: 0)

    // Write various list patterns
    try listWriter.writeValues([
        [1, 2, 3],          // Normal list
        nil,                // NULL list
        [],                 // Empty list
        [42],               // Single element
        [10, 20, 30, 40]    // Larger list
    ])

    try rowGroup.finalizeColumn(at: 0)
    try writer.close()

    print("✓ Successfully wrote list file to \(outputURL.path)")
}

func writeMapExample() throws {
    let outputURL = URL(fileURLWithPath: "maps.parquet")

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
    let writer = try ParquetFileWriter(url: outputURL)
    try writer.setSchema(schema)
    writer.setProperties(.default)

    let rowGroup = try writer.createRowGroup()
    let mapWriter = try rowGroup.stringInt32MapColumnWriter(at: 0)

    // Write various map patterns
    try mapWriter.writeMaps([
        ["a": 1, "b": 2, "c": 3],   // Normal map
        nil,                         // NULL map
        [:],                         // Empty map
        ["x": 100]                   // Single entry
    ])

    try rowGroup.finalizeColumn(at: 0)
    try writer.close()

    print("✓ Successfully wrote map file to \(outputURL.path)")
}

func writeStructExample() throws {
    let outputURL = URL(fileURLWithPath: "structs.parquet")

    // User-defined struct
    struct User {
        let name: String?
        let age: Int32?
    }

    let users = [
        User(name: "Alice", age: 30),
        User(name: "Bob", age: nil),
        User(name: nil, age: 25),
        User(name: "Charlie", age: 35)
    ]

    // Create schema
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
    let writer = try ParquetFileWriter(url: outputURL)
    try writer.setSchema(schema)
    writer.setProperties(.default)

    let rowGroup = try writer.createRowGroup()

    // Extract fields manually
    let names = users.map { $0.name }
    let ages = users.map { $0.age }

    // Write columns sequentially
    let nameWriter = try rowGroup.stringColumnWriter(at: 0)
    try nameWriter.writeOptionalValues(names)
    try rowGroup.finalizeColumn(at: 0)

    let ageWriter = try rowGroup.int32ColumnWriter(at: 1)
    try ageWriter.writeOptionalValues(ages)
    try rowGroup.finalizeColumn(at: 1)

    try writer.close()

    print("✓ Successfully wrote struct file to \(outputURL.path)")
}

// Run examples
do {
    print("=== Basic Write Example ===")
    try basicWriteExample()
    print()

    print("=== List Write Example ===")
    try writeListExample()
    print()

    print("=== Map Write Example ===")
    try writeMapExample()
    print()

    print("=== Struct Write Example ===")
    try writeStructExample()
    print()

    print("✓ All examples completed successfully!")
} catch {
    print("Error: \(error)")
}
