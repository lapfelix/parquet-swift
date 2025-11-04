// MapReaderTests - Tests for reading map columns
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class MapReaderTests: XCTestCase {

    func testMapSimple() throws {
        // Test reading a simple map without NULLs
        // Schema: map<string, int64>
        // Row 0: {"a": 1, "b": 2}
        // Row 1: {"x": 10, "y": 20, "z": 30}
        // Row 2: {"foo": 100}
        let url = fixtureURL("map_simple.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Read map column
        let maps = try rowGroup.readMap(at: ["attributes"])

        // Verify structure
        XCTAssertEqual(maps.count, 3, "Should have 3 rows")

        // Row 0: {"a": 1, "b": 2}
        guard let map0 = maps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 2, "Row 0 should have 2 entries")
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "a" && $0.value as? Int64 == 1 }))
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "b" && $0.value as? Int64 == 2 }))

        // Row 1: {"x": 10, "y": 20, "z": 30}
        guard let map1 = maps[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(map1.count, 3, "Row 1 should have 3 entries")
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "x" && $0.value as? Int64 == 10 }))
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "y" && $0.value as? Int64 == 20 }))
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "z" && $0.value as? Int64 == 30 }))

        // Row 2: {"foo": 100}
        guard let map2 = maps[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(map2.count, 1, "Row 2 should have 1 entry")
        XCTAssertTrue(map2.contains(where: { $0.key as? String == "foo" && $0.value as? Int64 == 100 }))
    }

    func testMapNullable() throws {
        // Test reading a map with all NULL combinations
        // Row 0: {"a": 1, "b": 2}           - All present
        // Row 1: {"x": 10, "y": NULL}       - Map present, one NULL value
        // Row 2: {}                         - Empty map
        // Row 3: NULL                       - NULL map
        // Row 4: {"k": NULL}                - Map with only NULL values
        let url = fixtureURL("map_nullable.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Read map column
        let maps = try rowGroup.readMap(at: ["attributes"])

        XCTAssertEqual(maps.count, 5, "Should have 5 rows")

        // Row 0: All present
        guard let map0 = maps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 2, "Row 0 should have 2 entries")
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "a" && $0.value as? Int64 == 1 }))
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "b" && $0.value as? Int64 == 2 }))

        // Row 1: One NULL value
        guard let map1 = maps[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(map1.count, 2, "Row 1 should have 2 entries")
        let entry1_x = map1.first(where: { $0.key as? String == "x" })
        XCTAssertNotNil(entry1_x, "Should have 'x' key")
        XCTAssertEqual(entry1_x?.value as? Int64, 10, "Value for 'x' should be 10")

        let entry1_y = map1.first(where: { $0.key as? String == "y" })
        XCTAssertNotNil(entry1_y, "Should have 'y' key")
        XCTAssertNil(entry1_y?.value, "Value for 'y' should be NULL")

        // Row 2: Empty map
        guard let map2 = maps[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(map2.count, 0, "Row 2 should be empty map")

        // Row 3: NULL map
        XCTAssertNil(maps[3], "Row 3 should be NULL map")

        // Row 4: Only NULL values
        guard let map4 = maps[4] else {
            XCTFail("Row 4 should not be NULL")
            return
        }
        XCTAssertEqual(map4.count, 1, "Row 4 should have 1 entry")
        let entry4_k = map4.first(where: { $0.key as? String == "k" })
        XCTAssertNotNil(entry4_k, "Should have 'k' key")
        XCTAssertNil(entry4_k?.value, "Value for 'k' should be NULL")
    }

    func testMapStringValues() throws {
        // Test reading a map with string values
        // Schema: map<string, string>
        // Row 0: {"name": "Alice", "city": "NYC"}
        // Row 1: {"lang": "Swift", "framework": "SwiftUI"}
        // Row 2: {"key": NULL}
        let url = fixtureURL("map_string_values.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Read map column
        let maps = try rowGroup.readMap(at: ["metadata"])

        XCTAssertEqual(maps.count, 3, "Should have 3 rows")

        // Row 0
        guard let map0 = maps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 2, "Row 0 should have 2 entries")
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "name" && $0.value as? String == "Alice" }))
        XCTAssertTrue(map0.contains(where: { $0.key as? String == "city" && $0.value as? String == "NYC" }))

        // Row 1
        guard let map1 = maps[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(map1.count, 2, "Row 1 should have 2 entries")
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "lang" && $0.value as? String == "Swift" }))
        XCTAssertTrue(map1.contains(where: { $0.key as? String == "framework" && $0.value as? String == "SwiftUI" }))

        // Row 2
        guard let map2 = maps[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(map2.count, 1, "Row 2 should have 1 entry")
        let entry2 = map2.first(where: { $0.key as? String == "key" })
        XCTAssertNotNil(entry2, "Should have 'key' key")
        XCTAssertNil(entry2?.value, "Value should be NULL")
    }

    func testMapIntKeys() throws {
        // Test reading a map with integer keys
        // Schema: map<int32, string>
        // Row 0: {1: "one", 2: "two", 3: "three"}
        // Row 1: {100: "hundred"}
        // Row 2: {}
        let url = fixtureURL("map_int_keys.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Read map column
        let maps = try rowGroup.readMap(at: ["lookup"])

        XCTAssertEqual(maps.count, 3, "Should have 3 rows")

        // Row 0
        guard let map0 = maps[0] else {
            XCTFail("Row 0 should not be NULL")
            return
        }
        XCTAssertEqual(map0.count, 3, "Row 0 should have 3 entries")
        XCTAssertTrue(map0.contains(where: { $0.key as? Int32 == 1 && $0.value as? String == "one" }))
        XCTAssertTrue(map0.contains(where: { $0.key as? Int32 == 2 && $0.value as? String == "two" }))
        XCTAssertTrue(map0.contains(where: { $0.key as? Int32 == 3 && $0.value as? String == "three" }))

        // Row 1
        guard let map1 = maps[1] else {
            XCTFail("Row 1 should not be NULL")
            return
        }
        XCTAssertEqual(map1.count, 1, "Row 1 should have 1 entry")
        XCTAssertTrue(map1.contains(where: { $0.key as? Int32 == 100 && $0.value as? String == "hundred" }))

        // Row 2: Empty map
        guard let map2 = maps[2] else {
            XCTFail("Row 2 should not be NULL")
            return
        }
        XCTAssertEqual(map2.count, 0, "Row 2 should be empty map")
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
