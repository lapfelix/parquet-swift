// NestedListLevelComputerTests.swift - Tests for multi-level list level computation
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class NestedListLevelComputerTests: XCTestCase {

    // MARK: - 2-Level Lists (maxRepetitionLevel = 2)

    /// Test basic 2-level list: [[[1, 2], [3]], [[4]]]
    ///
    /// Schema structure (2-level list of int32):
    /// ```
    /// optional group outer_list (LIST)           maxRep=0, maxDef=1
    ///   repeated group list                      maxRep=1, maxDef=2
    ///     optional group element (LIST)          maxRep=1, maxDef=3
    ///       repeated group list                  maxRep=2, maxDef=4
    ///         optional int32 element             maxRep=2, maxDef=5
    /// ```
    ///
    /// repeatedAncestorDefLevels = [1, 3] (empty list at each level)
    /// nullListDefLevels = [0, 2] (NULL list at each level)
    func testTwoLevelListBasic() throws {
        // Type: [[[Int32]?]?]
        let lists: [[[Int32]?]?] = [
            [[1, 2], [3]],  // Outer list [0]: has 2 inner lists
            [[4]]           // Outer list [1]: has 1 inner list
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        // Expected traversal:
        // [0][0][0] = 1  → rep=0 (new outer), def=5
        // [0][0][1] = 2  → rep=2 (continue inner), def=5
        // [0][1][0] = 3  → rep=1 (continue outer, new inner), def=5
        // [1][0][0] = 4  → rep=0 (new outer), def=5

        XCTAssertEqual(result.values, [1, 2, 3, 4], "Values should be flattened in traversal order")
        XCTAssertEqual(result.repetitionLevels, [0, 2, 1, 0], "Rep levels should mark list boundaries")
        XCTAssertEqual(result.definitionLevels, [5, 5, 5, 5], "All values present: def=maxDef")
    }

    /// Test 2-level list with NULL outer list: [[[1]], nil, [[2]]]
    func testTwoLevelListWithNullOuter() throws {
        let lists: [[[Int32]?]?] = [
            [[1]],   // Present outer list with 1 inner list
            nil,     // NULL outer list
            [[2]]    // Present outer list with 1 inner list
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        // Expected:
        // [0][0][0] = 1  → rep=0, def=5
        // [1] = nil      → rep=0, def=0 (NULL outer list)
        // [2][0][0] = 2  → rep=0, def=5

        XCTAssertEqual(result.values, [1, 2], "Only non-NULL list values emitted")
        XCTAssertEqual(result.repetitionLevels, [0, 0, 0], "All rep=0 (new outer lists)")
        XCTAssertEqual(result.definitionLevels, [5, 0, 5], "NULL outer list: def=0")
    }

    /// Test 2-level list with NULL inner list: [[[1], nil, [2]]]
    func testTwoLevelListWithNullInner() throws {
        let lists: [[[Int32]?]?] = [
            [[1], nil, [2]]  // Outer list with 3 inner lists (1 is NULL)
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        // Expected:
        // [0][0][0] = 1  → rep=0, def=5 (new outer, first inner)
        // [0][1] = nil   → rep=1, def=2 (continue outer, NULL inner)
        // [0][2][0] = 2  → rep=1, def=5 (continue outer, third inner)

        XCTAssertEqual(result.values, [1, 2], "Only non-NULL inner list values emitted")
        XCTAssertEqual(result.repetitionLevels, [0, 1, 1], "rep=1 for continuing outer list")
        XCTAssertEqual(result.definitionLevels, [5, 2, 5], "NULL inner list: def=2")
    }

    /// Test 2-level list with empty inner list: [[[1], [], [2]]]
    func testTwoLevelListWithEmptyInner() throws {
        let lists: [[[Int32]?]?] = [
            [[1], [], [2]]  // Outer list with 3 inner lists (middle is empty)
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        // Expected:
        // [0][0][0] = 1  → rep=0, def=5
        // [0][1] = []    → rep=1, def=3 (continue outer, empty inner)
        // [0][2][0] = 2  → rep=1, def=5

        XCTAssertEqual(result.values, [1, 2], "Empty list produces no values")
        XCTAssertEqual(result.repetitionLevels, [0, 1, 1], "Empty list still emits rep level")
        XCTAssertEqual(result.definitionLevels, [5, 3, 5], "Empty inner list: def=3")
    }

    /// Test 2-level list with empty outer list: [[], [[1]]]
    func testTwoLevelListWithEmptyOuter() throws {
        let lists: [[[Int32]?]?] = [
            [],      // Empty outer list
            [[1]]    // Non-empty outer list
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        // Expected:
        // [0] = []       → rep=0, def=1 (empty outer)
        // [1][0][0] = 1  → rep=0, def=5

        XCTAssertEqual(result.values, [1], "Empty outer list produces no values")
        XCTAssertEqual(result.repetitionLevels, [0, 0], "Both are new outer lists")
        XCTAssertEqual(result.definitionLevels, [1, 5], "Empty outer list: def=1")
    }

    // MARK: - 3-Level Lists (maxRepetitionLevel = 3)

    /// Test basic 3-level list: [[[[1, 2]]]]
    ///
    /// Schema structure (3-level list):
    /// maxRep progression: 0 → 1 → 2 → 3
    /// maxDef progression: 1 → 2 → 4 → 5 → 7
    func testThreeLevelListBasic() throws {
        // Type: [[[[Int32]?]?]?]
        let lists: [[[[Int32]?]?]?] = [
            [[[1, 2]]]  // One outer, one middle, one inner with 2 values
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 7,
                maxRepetitionLevel: 3,
                repeatedAncestorDefLevels: [1, 3, 5],
                nullListDefLevels: [0, 2, 4]
            )

        // Expected:
        // [0][0][0][0] = 1  → rep=0, def=7 (new at all levels)
        // [0][0][0][1] = 2  → rep=3, def=7 (continue innermost)

        XCTAssertEqual(result.values, [1, 2])
        XCTAssertEqual(result.repetitionLevels, [0, 3], "rep=3 for continuing innermost list")
        XCTAssertEqual(result.definitionLevels, [7, 7])
    }

    /// Test 3-level list with multiple middle lists: [[[[1]], [[2]]]]
    func testThreeLevelListMultipleMiddle() throws {
        let lists: [[[[Int32]?]?]?] = [
            [[[1]], [[2]]]  // One outer with 2 middle lists
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 7,
                maxRepetitionLevel: 3,
                repeatedAncestorDefLevels: [1, 3, 5],
                nullListDefLevels: [0, 2, 4]
            )

        // Expected:
        // [0][0][0][0] = 1  → rep=0, def=7
        // [0][1][0][0] = 2  → rep=1, def=7 (continue level 1, new at level 2)

        XCTAssertEqual(result.values, [1, 2])
        XCTAssertEqual(result.repetitionLevels, [0, 1], "rep=1 for new middle list")
        XCTAssertEqual(result.definitionLevels, [7, 7])
    }

    // MARK: - Edge Cases

    /// Test all-empty nested lists: [[], [[]], [[[]]]]
    func testAllEmptyNestedLists() throws {
        let lists: [[[Int32]?]?] = [
            [],     // Empty outer
            [[]]    // Outer with empty inner
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        XCTAssertEqual(result.values, [], "No values for all-empty lists")
        XCTAssertEqual(result.repetitionLevels, [0, 0], "Two empty lists")
        XCTAssertEqual(result.definitionLevels, [1, 3], "def=1 for empty outer, def=3 for empty inner")
    }

    /// Test all-NULL nested lists: [nil, [nil]]
    func testAllNullNestedLists() throws {
        let lists: [[[Int32]?]?] = [
            nil,     // NULL outer
            [nil]    // Outer with NULL inner
        ]

        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: 5,
                maxRepetitionLevel: 2,
                repeatedAncestorDefLevels: [1, 3],
                nullListDefLevels: [0, 2]
            )

        XCTAssertEqual(result.values, [], "No values for all-NULL lists")
        XCTAssertEqual(result.repetitionLevels, [0, 0], "Two NULL lists")
        XCTAssertEqual(result.definitionLevels, [0, 2], "def=0 for NULL outer, def=2 for NULL inner")
    }
}
