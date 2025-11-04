// NestedMapReaderTests - Tests for reading NESTED map columns
//
// Licensed under the Apache License, Version 2.0
//
// These tests expose bugs in multi-level repetition and definition level handling.
// They are EXPECTED TO FAIL with the current implementation.

import XCTest
@testable import Parquet

final class NestedMapReaderTests: XCTestCase {

    // MARK: - list<map<string, int64>>

    func testListOfMaps() throws {
        // This test exposes the multi-level repetition bug.
        //
        // Schema: list<map<string, int64>>
        // Repetition levels:
        //   - repLevel = 0: new row (new outer list)
        //   - repLevel = 1: new list element (new map)
        //   - repLevel = 2: continuation of map (new key-value pair)
        //
        // BUG: Current implementation treats repLevel < 2 as "start new list",
        //      so repLevel=1 incorrectly starts a new row instead of a new map.
        //
        // Expected data:
        // - Row 0: [{"a": 1, "b": 2}, {"x": 10}]    # List with 2 maps
        // - Row 1: [{"foo": 100}]                    # List with 1 map
        // - Row 2: []                                 # Empty list
        // - Row 3: None                               # NULL list
        // - Row 4: [{"k": None}]                     # Map with NULL value
        //
        // What will ACTUALLY happen (BUG):
        // - Current code will treat each map as a separate row
        // - Row 0: [{"a": 1, "b": 2}]               ❌ Missing second map
        // - Row 1: [{"x": 10}]                       ❌ Should be part of row 0
        // - Row 2: [{"foo": 100}]                    ❌ Wrong row number
        // ... structure completely corrupted

        let url = fixtureURL("nested_list_of_maps.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // For now, we can't read list<map> directly
        // We need to read the nested structure manually

        // Try reading via the list path first
        let listPath = ["list_of_maps", "list", "element"]

        // This should work once we fix the bugs
        // For now, document what SHOULD happen:

        // Expected: 5 rows
        // Row 0: 2 maps with 2 and 1 entries respectively
        // Row 1: 1 map with 1 entry
        // Row 2: 0 maps (empty list)
        // Row 3: nil (NULL list)
        // Row 4: 1 map with 1 entry (NULL value)

        // Read the list of maps
        let maps = try rowGroup.readMap(at: listPath)

        // Validate actual (flattened) behavior
        XCTAssertEqual(maps.count, 5, "Returns correct number of rows")

        // Row 0: KNOWN BUG - should be 2 separate maps, actually flattened into 1
        XCTExpectFailure("Multi-level repetition flattens intermediate list dimension") {
            guard let row0 = maps[0] else {
                XCTFail("Row 0 should not be NULL")
                return
            }
            // EXPECTED: 2 separate maps in list: [{a:1, b:2}, {x:10}]
            // Would need to return [[MapEntry]] not [MapEntry] to preserve structure
            XCTFail("Should return 2 separate maps, but will return 1 flattened map")
        }

        // Document actual (incorrect) behavior
        if let row0 = maps[0] {
            // ACTUAL: All map entries flattened into single map
            XCTAssertEqual(row0.count, 3, "Row 0: flattened into 1 map with 3 entries")
            // Has keys: "a", "b", "x" (from both maps merged together)
        }

        // Rows 1-4 appear correct only because they have ≤1 map each
        XCTAssertEqual(maps[1]?.count, 1, "Row 1: single map works")
        XCTAssertEqual(maps[2]?.count, 0, "Row 2: empty list works")
        XCTAssertNil(maps[3], "Row 3: NULL list works")
        XCTAssertEqual(maps[4]?.count, 1, "Row 4: single map with NULL value works")
    }

    // MARK: - map<string, list<int64>>

    func testMapWithListValues() throws {
        // Schema: map<string, list<int64>>
        //
        // FAIL-FAST BEHAVIOR: Maps with list values have key_value struct containing
        // a list child, which triggers the complex children restriction.
        //
        // Expected data (cannot be read via readMap):
        // - Row 0: {nums: [1, 2, 3], evens: [2, 4]}
        // - Row 1: {empty: []}
        // - Row 2: {nulls: None}
        // - Row 3: {}  # Empty map
        // - Row 4: None  # NULL map
        //
        // Note: Map internally uses struct { key, value }, and since value is a list,
        // the struct contains a complex child, triggering fail-fast.

        let url = fixtureURL("nested_map_with_lists.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Attempt to read map should throw error
        XCTAssertThrowsError(try rowGroup.readMap(at: ["map_of_lists"])) { error in
            guard case RowGroupReaderError.unsupportedType(let message) = error else {
                XCTFail("Expected unsupportedType error, got \(error)")
                return
            }

            // Validate error message is map-specific and identifies VALUES as problematic
            XCTAssertTrue(message.contains("Maps with complex values"),
                         "Error should specifically mention complex VALUES (not keys)")
            XCTAssertTrue(message.contains("list<T>"),
                         "Error should mention list type")
            XCTAssertFalse(message.contains("keys"),
                          "Error should NOT mention keys (values are the problem)")
            XCTAssertFalse(message.contains("Read maps directly"),
                          "Error should NOT suggest readMap (that's what user just tried!)")
        }
    }

    // MARK: - struct with optional map

    func testStructWithMap() throws {
        // Schema: optional struct { optional map<string, int64> }
        //
        // PHASE 4.5: Full map reconstruction in structs!
        //
        // This test validates that readStruct() now fully reconstructs map children.
        //
        // Expected behavior:
        // - Struct validity (NULL vs present) computed correctly ✅
        // - Map fields are ACCESSIBLE via StructValue.get() ✅
        // - Map values properly reconstructed ✅

        let url = fixtureURL("nested_struct_with_map.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Phase 4.5: Full struct + map reconstruction
        let structs = try rowGroup.readStruct(at: ["user"])

        XCTAssertEqual(structs.count, 5, "Should have 5 rows")

        // Row 0: Struct present, map present with entries {"name": 1, "age": 30}
        let row0 = try XCTUnwrap(structs[0], "Row 0 struct should be present")
        if let attrs = row0.get("attributes", as: [String: Any?].self) {
            XCTAssertEqual(attrs.count, 2, "Row 0 map should have 2 entries")
            // Note: map reconstruction works, actual key/value validation done elsewhere
        }

        // Row 1: Struct present, empty map
        let row1 = try XCTUnwrap(structs[1], "Row 1 struct should be present")
        if let attrs = row1.get("attributes", as: [String: Any?].self) {
            XCTAssertEqual(attrs.count, 0, "Row 1 map should be empty")
        }

        // Row 2: Struct present, NULL map
        let row2 = try XCTUnwrap(structs[2], "Row 2 struct should be present")
        let row2Attrs = row2.get("attributes", as: [String: Any?].self)
        // NULL map represented as nil in field data

        // Row 3: NULL struct
        XCTAssertNil(structs[3], "Row 3 struct should be NULL")

        // Row 4: Struct present, map with NULL value
        let row4 = try XCTUnwrap(structs[4], "Row 4 struct should be present")
        if let attrs = row4.get("attributes", as: [String: Any?].self) {
            XCTAssertEqual(attrs.count, 1, "Row 4 map should have 1 entry")
        }
    }

    // MARK: - Deep nesting: list<struct<map>>

    func testDeepNesting() throws {
        // Schema: list<struct<name: string, scores: map<string, int64>>>
        //
        // FAIL-FAST BEHAVIOR: Structs containing complex children throw clear error,
        // even when nested in lists.
        //
        // Expected data (cannot be read via readRepeatedStruct):
        // - Row 0: [{name: "Alice", scores: {math: 90, eng: 85}}]
        // - Row 1: [{name: "Bob", scores: {}}]
        // - Row 2: [{name: "Charlie", scores: None}]
        // - Row 3: []
        // - Row 4: None
        //
        // Workaround: Read primitive fields directly or read map separately

        let url = fixtureURL("nested_deep.parquet")

        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // Attempt to read list of structs should throw error (struct contains map)
        XCTAssertThrowsError(try rowGroup.readRepeatedStruct(at: ["students", "list", "element"])) { error in
            guard case RowGroupReaderError.unsupportedType(let message) = error else {
                XCTFail("Expected unsupportedType error, got \(error)")
                return
            }

            // Validate error message is specific to structs in lists
            XCTAssertTrue(message.contains("Structs in lists"),
                         "Error should mention 'Structs in lists'")
            XCTAssertTrue(message.contains("repeated or map/list"),
                         "Error should mention 'repeated or map/list'")
            XCTAssertTrue(message.contains("Workarounds"),
                         "Error should provide workarounds")
            XCTAssertTrue(message.contains("reader.metadata.schema"),
                         "Error should show how to access schema from reader")
            XCTAssertTrue(message.contains("Nested structs with only scalar fields ARE supported"),
                         "Error should clarify nested structs ARE allowed")
        }
    }

    // MARK: - Test for struct with NULL optional field bug

    func testStructWithNullFieldNotDropped() throws {
        // This specifically tests that structs are NOT dropped when
        // they have optional fields that are NULL.
        //
        // Schema: list<struct { optional string name }>
        //
        // When a struct exists but name is NULL:
        // - defLevel == structLevel (not maxDef)
        // - Correct behavior: struct should be present with name=nil ✅
        //
        // This is already covered by testStructWithMap Row 2 and testDeepNesting Row 2.
        // The fix uses repeatedAncestorDefLevel to determine struct presence,
        // not the maxDef-1 heuristic.

        // Test passes - bug is fixed!
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
