// StructReaderTests - Tests for reading struct columns
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class StructReaderTests: XCTestCase {

    func testStructSimple() throws {
        // Test reading a simple struct without NULLs
        // Schema: user { name: string, age: int32 }
        // Data: 3 rows, all fields present
        let url = fixtureURL("struct_simple.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Read struct column
        let users = try rowGroup.readStruct(at: ["user"])

        // Verify structure
        XCTAssertEqual(users.count, 3, "Should have 3 rows")

        // Row 0: {name: 'Alice', age: 30}
        guard let user0 = users[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(user0.get("name", as: String.self), "Alice")
        XCTAssertEqual(user0.get("age", as: Int32.self), 30)

        // Row 1: {name: 'Bob', age: 25}
        guard let user1 = users[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(user1.get("name", as: String.self), "Bob")
        XCTAssertEqual(user1.get("age", as: Int32.self), 25)

        // Row 2: {name: 'Charlie', age: 35}
        guard let user2 = users[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(user2.get("name", as: String.self), "Charlie")
        XCTAssertEqual(user2.get("age", as: Int32.self), 35)

        // Verify field names
        XCTAssertTrue(user0.fields.contains("name"))
        XCTAssertTrue(user0.fields.contains("age"))
        XCTAssertEqual(user0.fieldCount, 2)
    }

    func testStructNullable() throws {
        // Test reading a struct with all NULL combinations
        // Row 0: {name: 'Alice', age: 30}      - All fields present
        // Row 1: {name: NULL, age: 25}         - name is NULL
        // Row 2: {name: 'Charlie', age: NULL}  - age is NULL
        // Row 3: {name: NULL, age: NULL}       - All fields NULL (struct present)
        // Row 4: NULL                          - Struct is NULL
        let url = fixtureURL("struct_nullable.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Read struct column
        let users = try rowGroup.readStruct(at: ["user"])

        XCTAssertEqual(users.count, 5, "Should have 5 rows")

        // Row 0: All fields present
        guard let user0 = users[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(user0.get("name", as: String.self), "Alice")
        XCTAssertEqual(user0.get("age", as: Int64.self), 30)  // PyArrow uses Int64

        // Row 1: name is NULL
        guard let user1 = users[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertNil(user1.get("name", as: String.self), "Name should be NULL")
        XCTAssertEqual(user1.get("age", as: Int64.self), 25)

        // Row 2: age is NULL
        guard let user2 = users[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(user2.get("name", as: String.self), "Charlie")
        XCTAssertNil(user2.get("age", as: Int64.self), "Age should be NULL")

        // Row 3: All fields NULL (but struct present)
        guard let user3 = users[3] else {
            XCTFail("Row 3 should not be NULL")
            return
        }
        XCTAssertNil(user3.get("name", as: String.self), "Name should be NULL")
        XCTAssertNil(user3.get("age", as: Int64.self), "Age should be NULL")

        // Row 4: Struct is NULL
        XCTAssertNil(users[4], "Row 4 should be NULL struct")
    }

    // MARK: - Helper

    private func fixtureURL(_ filename: String) -> URL {
        let bundle = Bundle.module
        guard let url = bundle.url(forResource: filename.replacingOccurrences(of: ".parquet", with: ""),
                                    withExtension: "parquet",
                                    subdirectory: "Fixtures") else {
            fatalError("Could not find fixture: \(filename)")
        }
        return url
    }
}
