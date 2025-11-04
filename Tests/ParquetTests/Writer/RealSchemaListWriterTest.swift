// RealSchemaListWriterTest.swift - Test list writer with real Parquet schema
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class RealSchemaListWriterTest: XCTestCase {

    /// Test that list writers work with schemas loaded from real Parquet files
    /// This verifies that Schema correctly computes maxRepetitionLevel from the tree structure
    func testListWriterWithRealParquetSchema() throws {
        // First, write a file with a manually constructed schema (known to work)
        let tempWrite = FileManager.default.temporaryDirectory
            .appendingPathComponent("schema_test_write_\(UUID().uuidString).parquet")
        defer { try? FileManager.default.removeItem(at: tempWrite) }

        // Create standard 3-level list schema
        let element = SchemaElement(
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
            children: [element],
            parent: nil,
            depth: 2
        )
        element.parent = listGroup

        let numbersGroup = SchemaElement(
            name: "numbers",
            elementType: .group(logicalType: .list),
            repetitionType: .optional,
            fieldId: nil,
            children: [listGroup],
            parent: nil,
            depth: 1
        )
        listGroup.parent = numbersGroup

        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [numbersGroup],
            parent: nil,
            depth: 0
        )
        numbersGroup.parent = root

        let writeSchema = Schema(root: root)

        // Write test data
        let writer = try ParquetFileWriter(url: tempWrite)
        try writer.setSchema(writeSchema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.int32ListColumnWriter(at: 0)

        try listWriter.writeValues([[1, 2], [3, 4]])

        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // NOW THE CRITICAL TEST: Read the file back and load its schema
        // This schema will be parsed from Thrift metadata, not manually constructed
        let reader = try ParquetFileReader(url: tempWrite)
        defer { try? reader.close() }

        let readSchema = reader.metadata.schema

        // Verify the schema was parsed correctly
        XCTAssertEqual(readSchema.columnCount, 1, "Should have 1 column")

        let column = readSchema.column(at: 0)!

        // CRITICAL CHECKS: Does the parsed schema have correct levels?
        print("Column name: \(column.name)")
        print("Column repetitionType: \(column.repetitionType)")
        print("Column maxRepetitionLevel: \(column.maxRepetitionLevel)")
        print("Column maxDefinitionLevel: \(column.maxDefinitionLevel)")
        print("Column repeatedAncestorDefLevel: \(column.repeatedAncestorDefLevel ?? -999)")

        // The leaf element has repetitionType = .optional (not .repeated)
        XCTAssertEqual(column.repetitionType, .optional,
            "Leaf element should be optional in 3-level list")

        // But maxRepetitionLevel should be 1 (inherited from parent repeated group)
        XCTAssertEqual(column.maxRepetitionLevel, 1,
            "maxRepetitionLevel should be 1 for list column")

        XCTAssertEqual(column.maxDefinitionLevel, 3,
            "maxDefinitionLevel should be 3 for optional list with optional elements")

        XCTAssertEqual(column.repeatedAncestorDefLevel, 1,
            "repeatedAncestorDefLevel should be 1")

        // NOW THE REAL TEST: Can we write to this column using the parsed schema?
        let tempWrite2 = FileManager.default.temporaryDirectory
            .appendingPathComponent("schema_test_write2_\(UUID().uuidString).parquet")
        defer { try? FileManager.default.removeItem(at: tempWrite2) }

        let writer2 = try ParquetFileWriter(url: tempWrite2)
        try writer2.setSchema(readSchema)  // Use the PARSED schema
        writer2.setProperties(.default)

        let rowGroup2 = try writer2.createRowGroup()

        // This will fail if validateListColumnAccess incorrectly checks isRepeated
        // instead of maxRepetitionLevel
        let listWriter2 = try rowGroup2.int32ListColumnWriter(at: 0)

        try listWriter2.writeValues([[5, 6], [7, 8]])

        try rowGroup2.finalizeColumn(at: 0)
        try writer2.close()

        // Verify the second file is readable
        let reader2 = try ParquetFileReader(url: tempWrite2)
        defer { try? reader2.close() }

        XCTAssertEqual(reader2.metadata.numRows, 2)

        let readRowGroup = try reader2.rowGroup(at: 0)
        let readColumn = try readRowGroup.int32Column(at: 0)
        let readLists = try readColumn.readAllRepeated()

        XCTAssertEqual(readLists.count, 2)
        XCTAssertEqual(readLists[0]!, [5, 6])
        XCTAssertEqual(readLists[1]!, [7, 8])
    }

    /// Test that list writer correctly rejects non-list columns
    func testListWriterRejectsNonListColumn() throws {
        let tempWrite = FileManager.default.temporaryDirectory
            .appendingPathComponent("non_list_test_\(UUID().uuidString).parquet")
        defer { try? FileManager.default.removeItem(at: tempWrite) }

        // Create a simple schema with a regular (non-list) int32 column
        let int32Col = SchemaElement(
            name: "value",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,  // Optional but NOT repeated
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
            children: [int32Col],
            parent: nil,
            depth: 0
        )
        int32Col.parent = root

        let schema = Schema(root: root)

        // Verify the column is not a list
        let column = schema.column(at: 0)!
        XCTAssertEqual(column.maxRepetitionLevel, 0, "Non-list column should have maxRepetitionLevel = 0")

        // Attempt to use list writer on non-list column should throw
        let writer = try ParquetFileWriter(url: tempWrite)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        // This should throw because maxRepetitionLevel = 0
        XCTAssertThrowsError(try rowGroup.int32ListColumnWriter(at: 0)) { error in
            guard case WriterError.invalidState(let message) = error else {
                XCTFail("Expected WriterError.invalidState, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("not a repeated field"),
                "Error message should mention 'not a repeated field', got: \(message)")
            XCTAssertTrue(message.contains("maxRepetitionLevel"),
                "Error message should mention maxRepetitionLevel, got: \(message)")
        }
    }
}
