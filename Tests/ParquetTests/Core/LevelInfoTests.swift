// LevelInfoTests - Tests for LevelInfo construction and semantics
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class LevelInfoTests: XCTestCase {

    // MARK: - Basic Construction

    func testLevelInfoInit() {
        let info = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 2)

        XCTAssertEqual(info.defLevel, 3)
        XCTAssertEqual(info.repLevel, 1)
        XCTAssertEqual(info.repeatedAncestorDefLevel, 2)
    }

    func testHasNullableValues() {
        // Field CAN be null: def=3, ancestor=2 → nullable
        let nullable = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 2)
        XCTAssertTrue(nullable.hasNullableValues, "Should have nullable values when def > ancestor")

        // Field CANNOT be null: def=2, ancestor=2 → not nullable
        let required = LevelInfo(defLevel: 2, repLevel: 1, repeatedAncestorDefLevel: 2)
        XCTAssertFalse(required.hasNullableValues, "Should not have nullable values when def == ancestor")

        // Flat field with no repetition
        let flat = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)
        XCTAssertTrue(flat.hasNullableValues, "Flat optional field should be nullable")
    }

    func testEquality() {
        let info1 = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 2)
        let info2 = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 2)
        let info3 = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 1)

        XCTAssertEqual(info1, info2, "Identical LevelInfo should be equal")
        XCTAssertNotEqual(info1, info3, "Different repeatedAncestorDefLevel should not be equal")
    }

    func testDescription() {
        let info = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 2)
        let desc = info.description

        XCTAssertTrue(desc.contains("3"), "Should include defLevel")
        XCTAssertTrue(desc.contains("1"), "Should include repLevel")
        XCTAssertTrue(desc.contains("2"), "Should include repeatedAncestorDefLevel")
    }

    // MARK: - Schema-Based Construction Tests

    func testSimpleListLevelsFromSchema() throws {
        // For: optional group numbers (LIST) { repeated group list { optional int32 element; } }
        // Schema from repeated_int32_simple.parquet: list<int32>
        // Expected: maxDef=3, maxRep=1, repeatedAncestorDef=1
        let url = fixtureURL("repeated_int32_simple.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let column = reader.metadata.schema.column(at: 0)!

        // Verify schema levels first
        XCTAssertEqual(column.maxDefinitionLevel, 3, "Simple list should have maxDef=3")
        XCTAssertEqual(column.maxRepetitionLevel, 1, "Simple list should have maxRep=1")

        // Now test LevelInfo.from(column:)
        let info = try XCTUnwrap(LevelInfo.from(column: column), "Should create LevelInfo for repeated column")

        XCTAssertEqual(info.defLevel, 3, "defLevel should match column.maxDefinitionLevel")
        XCTAssertEqual(info.repLevel, 1, "repLevel should match column.maxRepetitionLevel")
        XCTAssertEqual(info.repeatedAncestorDefLevel, 1, "repeatedAncestorDefLevel should be 1 (list present)")
        XCTAssertTrue(info.hasNullableValues, "Element is optional, should be nullable")
    }

    func testSimpleListLevels() {
        // Manual instantiation for simple list
        // For: optional group items (LIST) { repeated group list { optional int32 element; } }
        // Expected: maxDef=3, maxRep=1, repeatedAncestorDef=1
        let info = LevelInfo(defLevel: 3, repLevel: 1, repeatedAncestorDefLevel: 1)

        XCTAssertTrue(info.hasNullableValues, "Element is optional, should be nullable")
        XCTAssertEqual(info.defLevel, 3)
        XCTAssertEqual(info.repLevel, 1)
        XCTAssertEqual(info.repeatedAncestorDefLevel, 1)
    }

    func testRequiredListLevels() {
        // For: required group items (LIST) { repeated group list { required int32 element; } }
        // Expected: maxDef=1, maxRep=1, repeatedAncestorDef=0
        // Note: No outer optional wrapper, so threshold is 0
        let info = LevelInfo(defLevel: 1, repLevel: 1, repeatedAncestorDefLevel: 0)

        XCTAssertTrue(info.hasNullableValues, "def (1) > repeatedAncestorDef (0), so hasNullableValues is true")
        XCTAssertEqual(info.defLevel, 1)
        XCTAssertEqual(info.repLevel, 1)
        XCTAssertEqual(info.repeatedAncestorDefLevel, 0)
    }

    func testNestedListLevelsFromSchema() throws {
        // For: optional group outer (LIST) {
        //        repeated group outer_list {
        //          optional group inner (LIST) {
        //            repeated group inner_list {
        //              optional int32 element;
        //            }
        //          }
        //        }
        //      }
        // Schema from nested_2level_int32_simple.parquet: list<list<int32>>
        // Expected: maxDef=5, maxRep=2, repeatedAncestorDef=3
        let url = fixtureURL("nested_2level_int32_simple.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let column = reader.metadata.schema.column(at: 0)!

        // Verify schema levels first
        XCTAssertEqual(column.maxDefinitionLevel, 5, "Nested list<list<int>> should have maxDef=5")
        XCTAssertEqual(column.maxRepetitionLevel, 2, "Nested list<list<int>> should have maxRep=2")

        // Verify repeatedAncestorDefLevels array
        let ancestorLevels = try XCTUnwrap(column.repeatedAncestorDefLevels, "Should have repeatedAncestorDefLevels")
        XCTAssertEqual(ancestorLevels.count, 2, "Should have 2 entries for maxRep=2")
        XCTAssertEqual(ancestorLevels[0], 1, "Outer list present at def=1")
        XCTAssertEqual(ancestorLevels[1], 3, "Inner list present at def=3")

        // Now test LevelInfo.from(column:) - should use ancestorLevels[1] for innermost
        let info = try XCTUnwrap(LevelInfo.from(column: column), "Should create LevelInfo for repeated column")

        XCTAssertEqual(info.defLevel, 5, "defLevel should match column.maxDefinitionLevel")
        XCTAssertEqual(info.repLevel, 2, "repLevel should match column.maxRepetitionLevel")
        XCTAssertEqual(info.repeatedAncestorDefLevel, 3, "repeatedAncestorDefLevel should be 3 (innermost list present)")
        XCTAssertTrue(info.hasNullableValues, "Element is optional")
    }

    func testNestedListLevels() {
        // Manual instantiation for nested list
        // For: list<list<int>>
        // Expected: maxDef=5, maxRep=2, repeatedAncestorDef=3
        let info = LevelInfo(defLevel: 5, repLevel: 2, repeatedAncestorDefLevel: 3)

        XCTAssertTrue(info.hasNullableValues, "Element is optional")
        XCTAssertEqual(info.defLevel, 5)
        XCTAssertEqual(info.repLevel, 2)
        XCTAssertEqual(info.repeatedAncestorDefLevel, 3)
    }

    func testRepeatedPrimitiveLevels() {
        // For: repeated int32 tags;
        // Expected: maxDef=1, maxRep=1, repeatedAncestorDef=1
        let info = LevelInfo(defLevel: 1, repLevel: 1, repeatedAncestorDefLevel: 1)

        XCTAssertFalse(info.hasNullableValues, "Required element, not nullable")
        XCTAssertEqual(info.defLevel, 1)
        XCTAssertEqual(info.repLevel, 1)
        XCTAssertEqual(info.repeatedAncestorDefLevel, 1)
    }

    func testFlatColumnReturnsNil() throws {
        // Flat columns (no repetition) should return nil for LevelInfo
        let url = fixtureURL("plain_types.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let column = reader.metadata.schema.column(at: 0)!

        XCTAssertEqual(column.maxRepetitionLevel, 0, "Flat column should have maxRep=0")

        let info = LevelInfo.from(column: column)
        XCTAssertNil(info, "Should return nil for flat column with no repetition")
    }

    // MARK: - Helper

    private func fixtureURL(_ filename: String) -> URL {
        let bundle = Bundle.module
        guard let url = bundle.url(forResource: filename.replacingOccurrences(of: ".parquet", with: ""),
                                    withExtension: "parquet",
                                    subdirectory: "Fixtures") else {
            fatalError("Missing fixture: \(filename)")
        }
        return url
    }

}
