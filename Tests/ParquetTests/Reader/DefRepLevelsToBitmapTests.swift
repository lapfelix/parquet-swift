// DefRepLevelsToBitmapTests - Tests for DefRepLevelsToBitmap (struct validity with repeated children)
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class DefRepLevelsToBitmapTests: XCTestCase {

    // MARK: - Basic Struct with List Child

    func testStructWithListChild() throws {
        // Schema: struct { list<int32> items; }
        // Data: [{items: [1, 2]}, {items: []}, {items: [3]}]
        // All structs are present, varying list contents

        // Child list column levels (from the items list):
        // - def=3, rep=0: struct present, list present, value present (1)
        // - def=3, rep=1: continuation of list, value present (2)
        // - def=2, rep=0: struct present, list present but empty
        // - def=3, rep=0: struct present, list present, value present (3)
        let defLevels: [UInt16] = [3, 3, 2, 3]
        let repLevels: [UInt16] = [0, 1, 0, 0]

        // For struct validity, we use levels BEFORE the list:
        // - struct present when def >= 1 (before list adds +2 for list present + element)
        // - But since we're using child levels, we need to map back
        //
        // The child list has: defLevel=3, repLevel=1, repeatedAncestorDefLevel=1
        // For the struct, we need: defLevel=1, repLevel=0, repeatedAncestorDefLevel=0
        // (struct present at def >= 1, no repetition at struct level)
        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)

        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        // All structs are present
        XCTAssertEqual(output.validBits, [true, true, true], "All structs should be present")
        XCTAssertEqual(output.valuesRead, 3, "Should have read 3 structs")
        XCTAssertEqual(output.nullCount, 0, "Should have no NULL structs")
    }

    func testStructWithNullList() throws {
        // Schema: struct { list<int32> items; }
        // Data: [{items: [1]}, {items: NULL}, {items: [2]}]
        //
        // Child list column levels:
        // - def=3, rep=0: struct present, list present, value present (1)
        // - def=1, rep=0: struct present, list NULL (def doesn't reach list level)
        // - def=3, rep=0: struct present, list present, value present (2)
        let defLevels: [UInt16] = [3, 1, 3]
        let repLevels: [UInt16] = [0, 0, 0]

        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)
        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        // All structs are present (list being NULL doesn't make struct NULL)
        XCTAssertEqual(output.validBits, [true, true, true], "All structs should be present")
        XCTAssertEqual(output.valuesRead, 3, "Should have read 3 structs")
        XCTAssertEqual(output.nullCount, 0, "Should have no NULL structs")
    }

    func testStructWithNullStructs() throws {
        // Schema: struct { list<int32> items; }
        // Data: [{items: [1]}, NULL, {items: [2]}]
        //
        // Child list column levels:
        // - def=3, rep=0: struct present, list present, value present (1)
        // - def=0, rep=0: struct NULL (def doesn't even reach struct level)
        // - def=3, rep=0: struct present, list present, value present (2)
        let defLevels: [UInt16] = [3, 0, 3]
        let repLevels: [UInt16] = [0, 0, 0]

        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)
        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        // Middle struct is NULL
        XCTAssertEqual(output.validBits, [true, false, true], "Second struct should be NULL")
        XCTAssertEqual(output.valuesRead, 3, "Should have read 3 structs")
        XCTAssertEqual(output.nullCount, 1, "Should have 1 NULL struct")
    }

    // MARK: - Struct with Map Child

    func testStructWithMapChild() throws {
        // Schema: struct { map<string, int32> props; }
        // Data: [{props: {a: 1, b: 2}}, {props: {}}, {props: {c: 3}}]
        //
        // Maps are encoded as list<struct<key, value>>, so similar to list case
        // Child column levels (from the map key/value):
        // - def=4, rep=0: struct present, map present, entry present, key/value present (a: 1)
        // - def=4, rep=2: continuation of map, entry present (b: 2)
        // - def=2, rep=0: struct present, map present but empty
        // - def=4, rep=0: struct present, map present, entry present (c: 3)
        let defLevels: [UInt16] = [4, 4, 2, 4]
        let repLevels: [UInt16] = [0, 2, 0, 0]

        // For struct validity with map child:
        // - Map has: defLevel=4, repLevel=2 (map entries repeat at level 2)
        // - Struct has: defLevel=1, repLevel=0
        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)
        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        // All structs are present
        XCTAssertEqual(output.validBits, [true, true, true], "All structs should be present")
        XCTAssertEqual(output.valuesRead, 3, "Should have read 3 structs")
        XCTAssertEqual(output.nullCount, 0, "Should have no NULL structs")
    }

    // MARK: - Optional Struct (Struct Can Be NULL)

    func testOptionalStructWithList() throws {
        // Schema: struct? { list<int32> items; }  (struct itself is optional)
        // Data: [{items: [1]}, NULL, {items: []}]
        //
        // When the struct is optional, it adds +1 to definition levels
        // Child list column levels:
        // - def=4, rep=0: outer optional present, struct present, list present, value present (1)
        // - def=0, rep=0: outer optional NULL (struct doesn't exist)
        // - def=3, rep=0: outer optional present, struct present, list present but empty
        let defLevels: [UInt16] = [4, 0, 3]
        let repLevels: [UInt16] = [0, 0, 0]

        // For optional struct: defLevel=2 (outer optional + struct), repLevel=0
        let levelInfo = LevelInfo(defLevel: 2, repLevel: 0, repeatedAncestorDefLevel: 0)
        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        // First and third present, second NULL
        XCTAssertEqual(output.validBits, [true, false, true], "Second struct should be NULL")
        XCTAssertEqual(output.valuesRead, 3, "Should have read 3 structs")
        XCTAssertEqual(output.nullCount, 1, "Should have 1 NULL struct")
    }

    // MARK: - Edge Cases

    func testEmptyInput() throws {
        let defLevels: [UInt16] = []
        let repLevels: [UInt16] = []
        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)

        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        XCTAssertEqual(output.validBits, [], "Should have no bits for empty input")
        XCTAssertEqual(output.valuesRead, 0, "Should have read 0 structs")
        XCTAssertEqual(output.nullCount, 0, "Should have no NULLs")
    }

    func testMismatchedLevelCounts() {
        let defLevels: [UInt16] = [3, 3]
        let repLevels: [UInt16] = [0]  // Mismatched count
        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)

        var output = ArrayReconstructor.ValidityBitmapOutput()

        XCTAssertThrowsError(
            try ArrayReconstructor.defRepLevelsToBitmap(
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                levelInfo: levelInfo,
                output: &output
            )
        ) { error in
            guard case ColumnReaderError.internalError(let message) = error else {
                XCTFail("Expected ColumnReaderError.internalError, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("must match"), "Error should mention mismatched counts")
        }
    }

    func testUpperBoundEnforcement() throws {
        // Schema: struct { list<int32> items; }
        // Data: [{items: [1]}, {items: [2]}, {items: [3]}]
        let defLevels: [UInt16] = [3, 3, 3]
        let repLevels: [UInt16] = [0, 0, 0]
        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)

        // Set upper bound to 2, should fail on third struct
        var output = ArrayReconstructor.ValidityBitmapOutput(valuesReadUpperBound: 2)

        XCTAssertThrowsError(
            try ArrayReconstructor.defRepLevelsToBitmap(
                definitionLevels: defLevels,
                repetitionLevels: repLevels,
                levelInfo: levelInfo,
                output: &output
            )
        ) { error in
            guard case ColumnReaderError.internalError(let message) = error else {
                XCTFail("Expected ColumnReaderError.internalError, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("upper bound"), "Error should mention upper bound")
        }
    }

    // MARK: - Level Bumping Verification

    func testLevelBumpingIsCorrect() throws {
        // This test verifies that level bumping works correctly by checking
        // the output for a scenario where bumping matters

        // Schema: struct { list<int32> items; }
        // Child list has: defLevel=3, repLevel=1, repeatedAncestorDefLevel=1
        // Struct should use: defLevel=1, repLevel=0, repeatedAncestorDefLevel=0

        // Data: [{items: [1, 2]}, NULL]
        // - def=3, rep=0: struct present, list present, value present
        // - def=3, rep=1: continuation (second element in list)
        // - def=0, rep=0: struct NULL
        let defLevels: [UInt16] = [3, 3, 0]
        let repLevels: [UInt16] = [0, 1, 0]

        let levelInfo = LevelInfo(defLevel: 1, repLevel: 0, repeatedAncestorDefLevel: 0)
        var output = ArrayReconstructor.ValidityBitmapOutput()

        try ArrayReconstructor.defRepLevelsToBitmap(
            definitionLevels: defLevels,
            repetitionLevels: repLevels,
            levelInfo: levelInfo,
            output: &output
        )

        // Should have 2 structs: first present, second NULL
        // The rep=1 entry should be filtered out (belongs to first struct's list)
        XCTAssertEqual(output.validBits, [true, false], "Should have one present, one NULL")
        XCTAssertEqual(output.valuesRead, 2, "Should have read 2 structs (not 3)")
        XCTAssertEqual(output.nullCount, 1, "Should have 1 NULL struct")
    }
}
