// Tests for reading multi-level nested columns (maxRepetitionLevel > 1)
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class NestedListTests: XCTestCase {

    // MARK: - 2-Level Tests (maxRepetitionLevel == 2)

    func testDummy() {
        // Simple dummy test to verify test file works
        XCTAssertTrue(true)
    }

    func testSimple2LevelInt32() throws {
        // Test: [[[1, 2], [3]], [[4]]]
        let url = fixtureURL("nested_2level_int32_simple.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllNested()
        let typed = result as! [[[Int32?]?]?]

        XCTAssertEqual(typed.count, 2, "Should have 2 outer lists")

        // [[[1, 2], [3]]]
        XCTAssertNotNil(typed[0], "First outer list should not be null")
        XCTAssertEqual(typed[0]!.count, 2, "First outer list should have 2 inner lists")

        XCTAssertNotNil(typed[0]![0], "First inner list should not be null")
        XCTAssertEqual(typed[0]![0]!, [1, 2])

        XCTAssertNotNil(typed[0]![1], "Second inner list should not be null")
        XCTAssertEqual(typed[0]![1]!, [3])

        // [[4]]
        XCTAssertNotNil(typed[1], "Second outer list should not be null")
        XCTAssertEqual(typed[1]!.count, 1, "Second outer list should have 1 inner list")

        XCTAssertNotNil(typed[1]![0], "Inner list should not be null")
        XCTAssertEqual(typed[1]![0]!, [4])
    }

    func test2LevelWithEmptyInner() throws {
        // Test: [[[1], []], [[2, 3]]]
        let url = fixtureURL("nested_2level_int32_empty_inner.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllNested()
        let typed = result as! [[[Int32?]?]?]

        XCTAssertEqual(typed.count, 2)

        // [[[1], []]]
        XCTAssertNotNil(typed[0])
        XCTAssertEqual(typed[0]!.count, 2)

        XCTAssertNotNil(typed[0]![0])
        XCTAssertEqual(typed[0]![0]!, [1])

        XCTAssertNotNil(typed[0]![1], "Empty inner list should not be null")
        XCTAssertEqual(typed[0]![1]!.count, 0, "Second inner list should be empty")

        // [[2, 3]]
        XCTAssertNotNil(typed[1])
        XCTAssertEqual(typed[1]!.count, 1)
        XCTAssertNotNil(typed[1]![0])
        XCTAssertEqual(typed[1]![0]!, [2, 3])
    }

    func test2LevelWithEmptyOuter() throws {
        // Test: [[[], [1]], [], [[2]]]
        let url = fixtureURL("nested_2level_int32_empty_outer.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllNested()
        let typed = result as! [[[Int32?]?]?]

        XCTAssertEqual(typed.count, 3)

        // [[[], [1]]]
        XCTAssertNotNil(typed[0])
        XCTAssertEqual(typed[0]!.count, 2)

        XCTAssertNotNil(typed[0]![0], "Empty inner list should not be null")
        XCTAssertEqual(typed[0]![0]!.count, 0)

        XCTAssertNotNil(typed[0]![1])
        XCTAssertEqual(typed[0]![1]!, [1])

        // []
        XCTAssertNotNil(typed[1], "Empty outer list should not be null")
        XCTAssertEqual(typed[1]!.count, 0, "Second outer list should be empty")

        // [[2]]
        XCTAssertNotNil(typed[2])
        XCTAssertEqual(typed[2]!.count, 1)
        XCTAssertNotNil(typed[2]![0])
        XCTAssertEqual(typed[2]![0]!, [2])
    }

    func test2LevelWithNullInner() throws {
        // Test: [[[1], None, [2]], [[3]]]
        let url = fixtureURL("nested_2level_int32_null_inner.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllNested()
        let typed = result as! [[[Int32?]?]?]

        XCTAssertEqual(typed.count, 2)

        // [[[1], None, [2]]]
        XCTAssertNotNil(typed[0])
        XCTAssertEqual(typed[0]!.count, 3)

        XCTAssertNotNil(typed[0]![0])
        XCTAssertEqual(typed[0]![0]!, [1])

        XCTAssertNil(typed[0]![1], "Second inner list should be NULL")

        XCTAssertNotNil(typed[0]![2])
        XCTAssertEqual(typed[0]![2]!, [2])

        // [[3]]
        XCTAssertNotNil(typed[1])
        XCTAssertEqual(typed[1]!.count, 1)
        XCTAssertNotNil(typed[1]![0])
        XCTAssertEqual(typed[1]![0]!, [3])
    }

    func test2LevelWithNullOuter() throws {
        // Test: [[[1]], None, [[2]]]
        let url = fixtureURL("nested_2level_int32_null_outer.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllNested()
        let typed = result as! [[[Int32?]?]?]

        XCTAssertEqual(typed.count, 3)

        // [[[1]]]
        XCTAssertNotNil(typed[0])
        XCTAssertEqual(typed[0]!.count, 1)
        XCTAssertNotNil(typed[0]![0])
        XCTAssertEqual(typed[0]![0]!, [1])

        // None
        XCTAssertNil(typed[1], "Second outer list should be NULL")

        // [[2]]
        XCTAssertNotNil(typed[2])
        XCTAssertEqual(typed[2]!.count, 1)
        XCTAssertNotNil(typed[2]![0])
        XCTAssertEqual(typed[2]![0]!, [2])
    }

    func test2LevelComplex() throws {
        // Test: [[[1, 2], []], None, [[], [3], None], [[]], [None, [4]]]
        let url = fixtureURL("nested_2level_int32_complex.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllNested()
        let typed = result as! [[[Int32?]?]?]

        XCTAssertEqual(typed.count, 5)

        // [[[1, 2], []]]
        XCTAssertNotNil(typed[0])
        XCTAssertEqual(typed[0]!.count, 2)
        XCTAssertNotNil(typed[0]![0])
        XCTAssertEqual(typed[0]![0]!, [1, 2])
        XCTAssertNotNil(typed[0]![1])
        XCTAssertEqual(typed[0]![1]!.count, 0)

        // None
        XCTAssertNil(typed[1])

        // [[], [3], None]
        XCTAssertNotNil(typed[2])
        XCTAssertEqual(typed[2]!.count, 3)
        XCTAssertNotNil(typed[2]![0])
        XCTAssertEqual(typed[2]![0]!.count, 0)
        XCTAssertNotNil(typed[2]![1])
        XCTAssertEqual(typed[2]![1]!, [3])
        XCTAssertNil(typed[2]![2])

        // [[]]
        XCTAssertNotNil(typed[3])
        XCTAssertEqual(typed[3]!.count, 1)
        XCTAssertNotNil(typed[3]![0])
        XCTAssertEqual(typed[3]![0]!.count, 0)

        // [None, [4]]
        XCTAssertNotNil(typed[4])
        XCTAssertEqual(typed[4]!.count, 2)
        XCTAssertNil(typed[4]![0])
        XCTAssertNotNil(typed[4]![1])
        XCTAssertEqual(typed[4]![1]!, [4])
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
