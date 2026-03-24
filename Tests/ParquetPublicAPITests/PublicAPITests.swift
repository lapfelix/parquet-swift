import XCTest
import Parquet

final class PublicAPITests: XCTestCase {
    private func temporaryFileURL() -> URL {
        let tempDir = FileManager.default.temporaryDirectory
        let filename = "public_api_\(UUID().uuidString).parquet"
        return tempDir.appendingPathComponent(filename)
    }

    private func cleanupFile(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }

    private func makeEmailSchema() throws -> Schema {
        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 0
        )

        try root.addChild(
            SchemaElement(
                name: "content_hash",
                elementType: .primitive(physicalType: .byteArray, logicalType: .string),
                repetitionType: .required,
                fieldId: nil,
                children: [],
                parent: nil,
                depth: 0
            )
        )

        try root.addChild(
            SchemaElement(
                name: "data",
                elementType: .primitive(physicalType: .byteArray, logicalType: .string),
                repetitionType: .required,
                fieldId: nil,
                children: [],
                parent: nil,
                depth: 0
            )
        )

        return Schema(root: root)
    }

    func testSchemaElementAddChildBuildsValidHierarchy() throws {
        let root = SchemaElement(
            name: "schema",
            elementType: .group(logicalType: nil),
            repetitionType: nil,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 0
        )

        let user = SchemaElement(
            name: "user",
            elementType: .group(logicalType: nil),
            repetitionType: .required,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 0
        )

        let email = SchemaElement(
            name: "email",
            elementType: .primitive(physicalType: .byteArray, logicalType: .string),
            repetitionType: .optional,
            fieldId: nil,
            children: [],
            parent: nil,
            depth: 0
        )

        try user.addChild(email)
        try root.addChild(user)

        XCTAssertEqual(root.children.count, 1)
        XCTAssertEqual(root.children.first?.name, "user")
        XCTAssertTrue(root.isGroup)

        XCTAssertTrue(user.parent === root)
        XCTAssertEqual(user.depth, 1)
        XCTAssertEqual(user.path, ["schema", "user"])

        XCTAssertTrue(email.parent === user)
        XCTAssertEqual(email.depth, 2)
        XCTAssertEqual(email.path, ["schema", "user", "email"])

        let schema = Schema(root: root)
        XCTAssertEqual(schema.columnCount, 1)
        XCTAssertEqual(schema.column(at: 0)?.path, ["user", "email"])
    }

    func testFinalizeColumnIsUsableFromPublicAPI() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(makeEmailSchema())
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        let hashWriter = try rowGroup.stringColumnWriter(at: 0)
        try hashWriter.writeValues(["hash-1", "hash-2"])
        try rowGroup.finalizeColumn(at: 0)

        let dataWriter = try rowGroup.stringColumnWriter(at: 1)
        try dataWriter.writeValues(["payload-1", "payload-2"])
        try rowGroup.finalizeColumn(at: 1)

        try writer.close()

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let readRowGroup = try reader.rowGroup(at: 0)
        XCTAssertEqual(try readRowGroup.stringColumn(at: 0).readAll(), ["hash-1", "hash-2"])
        XCTAssertEqual(try readRowGroup.stringColumn(at: 1).readAll(), ["payload-1", "payload-2"])
    }

    func testRowGroupCloseIsUsableBeforeFileClose() throws {
        let url = temporaryFileURL()
        defer { cleanupFile(url) }

        let writer = try ParquetFileWriter(url: url)
        try writer.setSchema(makeEmailSchema())
        writer.setProperties(.default)

        let rowGroup = try writer.createRowGroup()

        let hashWriter = try rowGroup.stringColumnWriter(at: 0)
        try hashWriter.writeValues(["hash-1"])
        try rowGroup.finalizeColumn(at: 0)

        let dataWriter = try rowGroup.stringColumnWriter(at: 1)
        try dataWriter.writeValues(["payload-1"])
        try rowGroup.finalizeColumn(at: 1)

        try rowGroup.close()
        try writer.close()

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        XCTAssertEqual(reader.metadata.numRows, 1)
        let readRowGroup = try reader.rowGroup(at: 0)
        XCTAssertEqual(try readRowGroup.stringColumn(at: 0).readAll(), ["hash-1"])
        XCTAssertEqual(try readRowGroup.stringColumn(at: 1).readAll(), ["payload-1"])
    }
}
