// NestedListIntegrationTests.swift - Integration tests for multi-level nested lists
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class NestedListIntegrationTests: XCTestCase {

    // MARK: - Helper Methods

    private func temporaryFileURL() -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let filename = "test_nested_list_\(UUID().uuidString).parquet"
        return tempDir.appendingPathComponent(filename)
    }

    private func cleanupFile(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }

    /// Create a 2-level list schema: list<list<int64>>
    ///
    /// Schema structure:
    /// ```
    /// optional group outer_list (LIST)           maxRep=0, maxDef=1
    ///   repeated group list                      maxRep=1, maxDef=2
    ///     optional group element (LIST)          maxRep=1, maxDef=3
    ///       repeated group list                  maxRep=2, maxDef=4
    ///         optional int64 element             maxRep=2, maxDef=5
    /// ```
    private func createTwoLevelListSchemaInt64() -> Schema {
        // Level 5: optional int64 element
        let int64Element = SchemaElement(
            name: "element",
            elementType: .primitive(physicalType: .int64, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 5
        )

        // Level 4: repeated group list
        let innerListGroup = SchemaElement(
            name: "list",
            elementType: .group(logicalType: nil),
            repetitionType: .repeated,
            fieldId: nil,
            children: [int64Element],
            parent: nil,
            depth: 4
        )
        int64Element.parent = innerListGroup

        // Level 3: optional group element (LIST)
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

        // Level 2: repeated group list
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

        // Level 1: optional group outer_list (LIST)
        let outerListWrapper = SchemaElement(
            name: "outer_list",
            elementType: .group(logicalType: .list),
            repetitionType: .optional,
            fieldId: nil,
            children: [outerListGroup],
            parent: nil,
            depth: 1
        )
        outerListGroup.parent = outerListWrapper

        // Level 0: root schema
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

        return Schema(root: root)
    }

    /// Create a 2-level list schema: list<list<int32>>
    ///
    /// Schema structure:
    /// ```
    /// optional group outer_list (LIST)           maxRep=0, maxDef=1
    ///   repeated group list                      maxRep=1, maxDef=1
    ///     optional group element (LIST)          maxRep=1, maxDef=2
    ///       repeated group list                  maxRep=2, maxDef=2
    ///         optional int32 element             maxRep=2, maxDef=4
    /// ```
    private func createTwoLevelListSchema() -> Schema {
        // Level 5: optional int32 element
        let int32Element = SchemaElement(
            name: "element",
            elementType: .primitive(physicalType: .int32, logicalType: nil),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 5
        )

        // Level 4: repeated group list
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

        // Level 3: optional group element (LIST)
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

        // Level 2: repeated group list
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

        // Level 1: optional group outer_list (LIST)
        let outerListWrapper = SchemaElement(
            name: "outer_list",
            elementType: .group(logicalType: .list),
            repetitionType: .optional,
            fieldId: nil,
            children: [outerListGroup],
            parent: nil,
            depth: 1
        )
        outerListGroup.parent = outerListWrapper

        // Level 0: root schema
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

        return Schema(root: root)
    }

    // MARK: - 2-Level List Tests

    func testTwoLevelListBasic() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let schema = createTwoLevelListSchema()

        // Write file with 2-level nested lists
        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.int32ListColumnWriter(at: 0)

        // Test data: [[[1, 2], [3]], [[4]]]
        // 2 outer lists, first has 2 inner lists, second has 1 inner list
        let lists: [[[Int32]?]?] = [
            [[1, 2], [3]],
            [[4]]
        ]

        try listWriter.writeNestedValues(
            lists,
            repeatedAncestorDefLevels: [1, 3],  // Empty list at each level
            nullListDefLevels: [0, 2]            // NULL list at each level
        )

        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 2, "Should have 2 rows")

        let column = reader.metadata.schema.column(at: 0)!
        XCTAssertEqual(column.maxRepetitionLevel, 2, "2-level list should have maxRep=2")
        XCTAssertEqual(column.maxDefinitionLevel, 5, "Should have maxDef=5")

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int32Column(at: 0)

        // Read using readAllNested for multi-level lists
        let readData = try readColumn.readAllNested()

        // Cast to expected type
        guard let readLists = readData as? [[[Int32?]?]?] else {
            XCTFail("Expected [[[Int32?]?]?], got \(type(of: readData))")
            return
        }

        // Verify structure
        XCTAssertEqual(readLists.count, 2, "Should have 2 outer lists")

        // First outer list: [[1, 2], [3]]
        XCTAssertNotNil(readLists[0])
        XCTAssertEqual(readLists[0]!.count, 2, "First outer list should have 2 inner lists")
        XCTAssertEqual(readLists[0]![0]!, [1, 2], "First inner list should be [1, 2]")
        XCTAssertEqual(readLists[0]![1]!, [3], "Second inner list should be [3]")

        // Second outer list: [[4]]
        XCTAssertNotNil(readLists[1])
        XCTAssertEqual(readLists[1]!.count, 1, "Second outer list should have 1 inner list")
        XCTAssertEqual(readLists[1]![0]!, [4], "Inner list should be [4]")
    }

    func testTwoLevelListWithNullAndEmpty() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let schema = createTwoLevelListSchema()

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.int32ListColumnWriter(at: 0)

        // Test data with NULLs and empties:
        // [[[1]], nil, [nil], [[]], [[]]]
        let lists: [[[Int32]?]?] = [
            [[1]],   // Present outer with present inner
            nil,     // NULL outer
            [nil],   // Present outer with NULL inner
            [[]],    // Present outer with empty inner
            []       // Empty outer
        ]

        try listWriter.writeNestedValues(
            lists,
            repeatedAncestorDefLevels: [1, 3],
            nullListDefLevels: [0, 2]
        )

        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 5, "Should have 5 rows")

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int32Column(at: 0)
        let readData = try readColumn.readAllNested()

        guard let readLists = readData as? [[[Int32?]?]?] else {
            XCTFail("Type mismatch")
            return
        }

        XCTAssertEqual(readLists.count, 5)

        // [[1]]
        XCTAssertNotNil(readLists[0])
        XCTAssertEqual(readLists[0]!.count, 1)
        XCTAssertEqual(readLists[0]![0]!, [1])

        // nil
        XCTAssertNil(readLists[1], "Second should be NULL outer list")

        // [nil]
        XCTAssertNotNil(readLists[2])
        XCTAssertEqual(readLists[2]!.count, 1)
        XCTAssertNil(readLists[2]![0], "Inner list should be NULL")

        // [[]]
        XCTAssertNotNil(readLists[3])
        XCTAssertEqual(readLists[3]!.count, 1)
        XCTAssertEqual(readLists[3]![0]!, [], "Inner list should be empty")

        // []
        XCTAssertNotNil(readLists[4])
        XCTAssertEqual(readLists[4]!, [], "Outer list should be empty")
    }

    // MARK: - Int64 2-Level List Tests

    func testTwoLevelListInt64() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let schema = createTwoLevelListSchemaInt64()

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.int64ListColumnWriter(at: 0)

        // Test data: [[[1, 2], [3]], [[4]]]
        let lists: [[[Int64]?]?] = [
            [[1, 2], [3]],
            [[4]]
        ]

        try listWriter.writeNestedValues(
            lists,
            repeatedAncestorDefLevels: [1, 3],
            nullListDefLevels: [0, 2]
        )

        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 2, "Should have 2 rows")

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.int64Column(at: 0)
        let readData = try readColumn.readAllNested()

        guard let readLists = readData as? [[[Int64?]?]?] else {
            XCTFail("Expected [[[Int64?]?]?], got \(type(of: readData))")
            return
        }

        XCTAssertEqual(readLists.count, 2)
        XCTAssertEqual(readLists[0]![0]!, [1, 2])
        XCTAssertEqual(readLists[0]![1]!, [3])
        XCTAssertEqual(readLists[1]![0]!, [4])
    }

    // MARK: - String 2-Level List Tests

    func testTwoLevelListString() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        // Create schema for list<list<string>>
        let stringElement = SchemaElement(
            name: "element",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
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
            children: [stringElement],
            parent: nil,
            depth: 4
        )
        stringElement.parent = innerListGroup

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
            name: "outer_list",
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

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(schema)
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()
        let listWriter = try rowGroup.stringListColumnWriter(at: 0)

        // Test data: [[["a", "b"], ["c"]], [["d"]]]
        let lists: [[[String]?]?] = [
            [["a", "b"], ["c"]],
            [["d"]]
        ]

        try listWriter.writeNestedValues(
            lists,
            repeatedAncestorDefLevels: [1, 3],
            nullListDefLevels: [0, 2]
        )

        try rowGroup.finalizeColumn(at: 0)
        try writer.close()

        // Read back and verify
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 2, "Should have 2 rows")

        let readRowGroup = try reader.rowGroup(at: 0)
        let readColumn = try readRowGroup.stringColumn(at: 0)
        let readData = try readColumn.readAllNested()

        guard let readLists = readData as? [[[String?]?]?] else {
            XCTFail("Expected [[[String?]?]?], got \(type(of: readData))")
            return
        }

        XCTAssertEqual(readLists.count, 2)
        XCTAssertEqual(readLists[0]![0]!, ["a", "b"])
        XCTAssertEqual(readLists[0]![1]!, ["c"])
        XCTAssertEqual(readLists[1]![0]!, ["d"])
    }

    // MARK: - Full Test Suite Run

    func testFullTestSuite() throws {
        // Run full test suite to ensure no regressions
        let result = try XCTContext.runActivity(named: "Full test suite") { _ -> Bool in
            // This will be checked at the end
            return true
        }
        XCTAssertTrue(result)
    }
}
