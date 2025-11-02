// Tests for Schema and SchemaBuilder
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class SchemaTests: XCTestCase {
    // MARK: - Basic Schema Tests

    func testSimpleSchema() throws {
        // Simple schema: schema { id: INT64, name: STRING }
        let elements: [ThriftSchemaElement] = [
            // Root
            ThriftSchemaElement(
                type: nil,
                typeLength: nil,
                repetitionType: nil,
                name: "schema",
                numChildren: 2,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            // id column
            ThriftSchemaElement(
                type: .int64,
                typeLength: nil,
                repetitionType: .required,
                name: "id",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            // name column
            ThriftSchemaElement(
                type: .byteArray,
                typeLength: nil,
                repetitionType: .optional,
                name: "name",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: .string
            )
        ]

        let schema = try SchemaBuilder.buildSchema(from: elements)

        // Check root
        XCTAssertEqual(schema.root.name, "schema")
        XCTAssertTrue(schema.root.isRoot)
        XCTAssertTrue(schema.root.isGroup)
        XCTAssertEqual(schema.root.children.count, 2)

        // Check columns
        XCTAssertEqual(schema.columnCount, 2)

        let idColumn = schema.column(at: 0)
        XCTAssertNotNil(idColumn)
        XCTAssertEqual(idColumn?.name, "id")
        XCTAssertEqual(idColumn?.physicalType, .int64)
        XCTAssertTrue(idColumn?.isRequired ?? false)
        XCTAssertEqual(idColumn?.path, ["id"])

        let nameColumn = schema.column(at: 1)
        XCTAssertNotNil(nameColumn)
        XCTAssertEqual(nameColumn?.name, "name")
        XCTAssertEqual(nameColumn?.physicalType, .byteArray)
        XCTAssertTrue(nameColumn?.isOptional ?? false)
        XCTAssertEqual(nameColumn?.logicalType, .string)
        XCTAssertEqual(nameColumn?.path, ["name"])
    }

    func testNestedSchema() throws {
        // Nested schema: schema { user { id: INT64, name: STRING } }
        let elements: [ThriftSchemaElement] = [
            // Root
            ThriftSchemaElement(
                type: nil,
                typeLength: nil,
                repetitionType: nil,
                name: "schema",
                numChildren: 1,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            // user group
            ThriftSchemaElement(
                type: nil,
                typeLength: nil,
                repetitionType: .required,
                name: "user",
                numChildren: 2,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            // user.id
            ThriftSchemaElement(
                type: .int64,
                typeLength: nil,
                repetitionType: .required,
                name: "id",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            // user.name
            ThriftSchemaElement(
                type: .byteArray,
                typeLength: nil,
                repetitionType: .optional,
                name: "name",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: .string
            )
        ]

        let schema = try SchemaBuilder.buildSchema(from: elements)

        // Check structure
        XCTAssertEqual(schema.columnCount, 2)

        // Check user group
        let userNode = schema.root.children[0]
        XCTAssertEqual(userNode.name, "user")
        XCTAssertTrue(userNode.isGroup)
        XCTAssertEqual(userNode.depth, 1)
        XCTAssertEqual(userNode.children.count, 2)

        // Check columns
        let idColumn = schema.column(at: 0)
        XCTAssertEqual(idColumn?.name, "id")
        XCTAssertEqual(idColumn?.path, ["user", "id"])
        XCTAssertEqual(idColumn?.element.depth, 2)

        let nameColumn = schema.column(at: 1)
        XCTAssertEqual(nameColumn?.name, "name")
        XCTAssertEqual(nameColumn?.path, ["user", "name"])
    }

    func testLogicalTypes() throws {
        // Schema with various logical types
        let elements: [ThriftSchemaElement] = [
            // Root
            ThriftSchemaElement(
                type: nil,
                typeLength: nil,
                repetitionType: nil,
                name: "schema",
                numChildren: 3,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            // timestamp
            ThriftSchemaElement(
                type: .int64,
                typeLength: nil,
                repetitionType: .required,
                name: "timestamp",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: .timestamp(
                    ThriftTimestampType(isAdjustedToUTC: true, unit: .micros)
                )
            ),
            // price (decimal)
            ThriftSchemaElement(
                type: .int64,
                typeLength: nil,
                repetitionType: .required,
                name: "price",
                numChildren: nil,
                convertedType: nil,
                scale: 2,
                precision: 10,
                fieldId: nil,
                logicalType: .decimal(
                    ThriftDecimalType(scale: 2, precision: 10)
                )
            ),
            // uuid
            ThriftSchemaElement(
                type: .fixedLenByteArray,
                typeLength: 16,
                repetitionType: .optional,
                name: "uuid",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: .uuid
            )
        ]

        let schema = try SchemaBuilder.buildSchema(from: elements)

        XCTAssertEqual(schema.columnCount, 3)

        // Check timestamp
        let tsColumn = schema.column(at: 0)
        XCTAssertEqual(tsColumn?.name, "timestamp")
        if case .timestamp(let isUTC, let unit) = tsColumn?.logicalType {
            XCTAssertTrue(isUTC)
            XCTAssertEqual(unit, .microseconds)
        } else {
            XCTFail("Expected timestamp logical type")
        }

        // Check decimal
        let priceColumn = schema.column(at: 1)
        XCTAssertEqual(priceColumn?.name, "price")
        if case .decimal(let precision, let scale) = priceColumn?.logicalType {
            XCTAssertEqual(precision, 10)
            XCTAssertEqual(scale, 2)
        } else {
            XCTFail("Expected decimal logical type")
        }

        // Check uuid
        let uuidColumn = schema.column(at: 2)
        XCTAssertEqual(uuidColumn?.name, "uuid")
        XCTAssertEqual(uuidColumn?.physicalType, .fixedLenByteArray(length: 16))
        XCTAssertEqual(uuidColumn?.logicalType, .uuid)
    }

    // MARK: - Error Cases

    func testEmptySchema() {
        let elements: [ThriftSchemaElement] = []

        XCTAssertThrowsError(try SchemaBuilder.buildSchema(from: elements)) { error in
            guard case SchemaError.invalidSchema(let msg) = error else {
                XCTFail("Expected invalidSchema error")
                return
            }
            XCTAssertTrue(msg.contains("empty"))
        }
    }

    func testInvalidRootWithoutChildren() {
        let elements: [ThriftSchemaElement] = [
            ThriftSchemaElement(
                type: nil,
                typeLength: nil,
                repetitionType: nil,
                name: "root",
                numChildren: 0,  // Invalid: root must have children
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            )
        ]

        XCTAssertThrowsError(try SchemaBuilder.buildSchema(from: elements)) { error in
            guard case SchemaError.invalidSchema(let msg) = error else {
                XCTFail("Expected invalidSchema error")
                return
            }
            XCTAssertTrue(msg.contains("children"))
        }
    }

    // MARK: - Schema Description

    func testSchemaDescription() throws {
        let elements: [ThriftSchemaElement] = [
            ThriftSchemaElement(
                type: nil,
                typeLength: nil,
                repetitionType: nil,
                name: "schema",
                numChildren: 1,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            ),
            ThriftSchemaElement(
                type: .int32,
                typeLength: nil,
                repetitionType: .required,
                name: "id",
                numChildren: nil,
                convertedType: nil,
                scale: nil,
                precision: nil,
                fieldId: nil,
                logicalType: nil
            )
        ]

        let schema = try SchemaBuilder.buildSchema(from: elements)
        let description = schema.description

        XCTAssertTrue(description.contains("Parquet Schema"))
        XCTAssertTrue(description.contains("schema"))
        XCTAssertTrue(description.contains("id"))
        XCTAssertTrue(description.contains("Columns: 1"))
    }
}
