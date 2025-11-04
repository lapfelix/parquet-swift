// LevelComputerTests.swift - Tests for level computation (inverse of ArrayReconstructor)
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class LevelComputerTests: XCTestCase {

    // MARK: - Single-Level Lists (Non-Nullable Elements)

    func testSimpleListWithValues() throws {
        // Inverse of DefRepLevelsToListInfoTests.testSimpleListWithValues
        // Data: [[1, 2], [3]]
        let lists: [[Int32]] = [[1, 2], [3]]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0  // repeatedAncestorDefLevel - 1
        )

        // Verify values
        XCTAssertEqual(result.values, [1, 2, 3])

        // Verify rep levels: [0, 1, 0]
        // - 0: new list [1, 2]
        // - 1: continuation of [1, 2]
        // - 0: new list [3]
        XCTAssertEqual(result.repetitionLevels, [0, 1, 0])

        // Verify def levels: [3, 3, 3] (all values present)
        XCTAssertEqual(result.definitionLevels, [3, 3, 3])
    }

    func testListWithEmptyList() throws {
        // Inverse of DefRepLevelsToListInfoTests.testListWithEmptyList
        // Data: [[1, 2], [], [3]]
        let lists: [[Int32]?] = [[1, 2], [], [3]]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        // Verify values: [1, 2, 3] (empty list produces no value)
        XCTAssertEqual(result.values, [1, 2, 3])

        // Verify rep levels: [0, 1, 0, 0]
        // - 0: new list [1, 2]
        // - 1: continuation
        // - 0: new list [] (empty)
        // - 0: new list [3]
        XCTAssertEqual(result.repetitionLevels, [0, 1, 0, 0])

        // Verify def levels: [3, 3, 1, 3]
        // - 3, 3: values present
        // - 1: empty list (def == repeatedAncestorDefLevel)
        // - 3: value present
        XCTAssertEqual(result.definitionLevels, [3, 3, 1, 3])
    }

    func testListWithNullList() throws {
        // Inverse of DefRepLevelsToListInfoTests.testListWithNullList
        // Data: [[1, 2], nil, [3]]
        let lists: [[Int32]?] = [[1, 2], nil, [3]]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        // Verify values: [1, 2, 3] (NULL list produces no value)
        XCTAssertEqual(result.values, [1, 2, 3])

        // Verify rep levels: [0, 1, 0, 0]
        XCTAssertEqual(result.repetitionLevels, [0, 1, 0, 0])

        // Verify def levels: [3, 3, 0, 3]
        // - 3, 3: values present
        // - 0: NULL list (def < repeatedAncestorDefLevel)
        // - 3: value present
        XCTAssertEqual(result.definitionLevels, [3, 3, 0, 3])
    }

    // MARK: - Single-Level Lists (Nullable Elements)

    func testListWithNullElements() throws {
        // Inverse of DefRepLevelsToListInfoTests.testListWithNullElements
        // Data: [[1, nil, 2], [nil], [3]]
        let lists: [[Int32?]?] = [[1, nil, 2], [nil], [3]]

        let result = LevelComputer.computeLevelsForListWithNullableElements(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0,
            nullElementDefLevel: 2  // maxDefinitionLevel - 1
        )

        // Verify values: [1, 2, 3] (NULL elements produce no values)
        XCTAssertEqual(result.values, [1, 2, 3])

        // Verify rep levels: [0, 1, 1, 0, 0]
        // - 0: new list [1, nil, 2]
        // - 1, 1: continuation
        // - 0: new list [nil]
        // - 0: new list [3]
        XCTAssertEqual(result.repetitionLevels, [0, 1, 1, 0, 0])

        // Verify def levels: [3, 2, 3, 2, 3]
        // - 3: value 1 present
        // - 2: nil element (def = maxDef - 1)
        // - 3: value 2 present
        // - 2: nil element
        // - 3: value 3 present
        XCTAssertEqual(result.definitionLevels, [3, 2, 3, 2, 3])
    }

    func testListWithAllNullElements() throws {
        // Data: [[nil, nil, nil]]
        let lists: [[Int32?]?] = [[nil, nil, nil]]

        let result = LevelComputer.computeLevelsForListWithNullableElements(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0,
            nullElementDefLevel: 2  // maxDefinitionLevel - 1
        )

        // No values (all NULL)
        XCTAssertEqual(result.values, [])

        // Rep levels for 3 elements
        XCTAssertEqual(result.repetitionLevels, [0, 1, 1])

        // All elements NULL (def = 2)
        XCTAssertEqual(result.definitionLevels, [2, 2, 2])
    }

    func testEmptyListWithNullableElements() throws {
        // Data: [[1], [], [nil]]
        let lists: [[Int32?]?] = [[1], [], [nil]]

        let result = LevelComputer.computeLevelsForListWithNullableElements(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0,
            nullElementDefLevel: 2  // maxDefinitionLevel - 1
        )

        // Values: [1] (empty list and nil element produce no values)
        XCTAssertEqual(result.values, [1])

        // Rep levels: [0, 0, 0]
        // - 0: new list [1]
        // - 0: new list [] (empty)
        // - 0: new list [nil]
        XCTAssertEqual(result.repetitionLevels, [0, 0, 0])

        // Def levels: [3, 1, 2]
        // - 3: value present
        // - 1: empty list (def == repeatedAncestorDefLevel)
        // - 2: nil element
        XCTAssertEqual(result.definitionLevels, [3, 1, 2])
    }

    // MARK: - Required Lists (No Outer Nullability)

    func testRequiredList() throws {
        // Inverse of DefRepLevelsToListInfoTests.testRequiredList
        // Data: [[1, 2], [3]]
        // Schema: required list<int32> (no outer optional wrapper)
        let lists: [[Int32]] = [[1, 2], [3]]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 1,        // Only element level
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 0,  // No outer optional, so threshold is 0
            nullListDefLevel: -1          // Required list, no NULL possible
        )

        // Verify values
        XCTAssertEqual(result.values, [1, 2, 3])

        // Verify rep levels
        XCTAssertEqual(result.repetitionLevels, [0, 1, 0])

        // Verify def levels: all 1 (required elements)
        XCTAssertEqual(result.definitionLevels, [1, 1, 1])
    }

    func testRequiredListWithEmptyList() throws {
        // Data: [[1], [], [2]]
        // Schema: required list<int32>
        let lists: [[Int32]] = [[1], [], [2]]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 1,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 0,
            nullListDefLevel: -1  // Required list, no NULL possible
        )

        // Values (empty list produces no value)
        XCTAssertEqual(result.values, [1, 2])

        // Rep levels
        XCTAssertEqual(result.repetitionLevels, [0, 0, 0])

        // Def levels: [1, 0, 1]
        // - 1: value present
        // - 0: empty list (def == repeatedAncestorDefLevel)
        // - 1: value present
        XCTAssertEqual(result.definitionLevels, [1, 0, 1])
    }

    // MARK: - Edge Cases

    func testSingletonList() throws {
        // Data: [[42]]
        let lists: [[Int32]?] = [[42]]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        XCTAssertEqual(result.values, [42])
        XCTAssertEqual(result.repetitionLevels, [0])
        XCTAssertEqual(result.definitionLevels, [3])
    }

    func testAllEmptyLists() throws {
        // Data: [[], [], []]
        let lists: [[Int32]?] = [[], [], []]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        // No values
        XCTAssertEqual(result.values, [])

        // All rep=0 (new list boundary)
        XCTAssertEqual(result.repetitionLevels, [0, 0, 0])

        // All def=1 (empty lists)
        XCTAssertEqual(result.definitionLevels, [1, 1, 1])
    }

    func testAllNullLists() throws {
        // Data: [nil, nil, nil]
        let lists: [[Int32]?] = [nil, nil, nil]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        // No values
        XCTAssertEqual(result.values, [])

        // All rep=0
        XCTAssertEqual(result.repetitionLevels, [0, 0, 0])

        // All def=0 (NULL lists)
        XCTAssertEqual(result.definitionLevels, [0, 0, 0])
    }

    func testMixedNullEmptyAndValues() throws {
        // Data: [[1, 2], nil, [], [3], nil, []]
        let lists: [[Int32]?] = [[1, 2], nil, [], [3], nil, []]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        // Values: [1, 2, 3]
        XCTAssertEqual(result.values, [1, 2, 3])

        // Rep levels: [0, 1, 0, 0, 0, 0, 0]
        XCTAssertEqual(result.repetitionLevels, [0, 1, 0, 0, 0, 0, 0])

        // Def levels: [3, 3, 0, 1, 3, 0, 1]
        // - 3, 3: values in first list
        // - 0: NULL list
        // - 1: empty list
        // - 3: value in fourth list
        // - 0: NULL list
        // - 1: empty list
        XCTAssertEqual(result.definitionLevels, [3, 3, 0, 1, 3, 0, 1])
    }

    // MARK: - Large Lists

    func testLargeList() throws {
        // Data: [[1, 2, ..., 1000]]
        let largeList: [Int32] = Array(1...1000).map { Int32($0) }
        let lists: [[Int32]] = [largeList]

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: 3,
            maxRepetitionLevel: 1,
            repeatedAncestorDefLevel: 1,
            nullListDefLevel: 0
        )

        // All values present
        XCTAssertEqual(result.values.count, 1000)
        XCTAssertEqual(result.values, largeList)

        // First rep=0, rest rep=1
        XCTAssertEqual(result.repetitionLevels.count, 1000)
        XCTAssertEqual(result.repetitionLevels[0], 0)
        XCTAssertTrue(result.repetitionLevels.dropFirst().allSatisfy { $0 == 1 })

        // All def=3
        XCTAssertTrue(result.definitionLevels.allSatisfy { $0 == 3 })
    }
}
