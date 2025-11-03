// Tests for reading repeated columns (arrays/lists) using readAllRepeated()
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class RepeatedColumnTests: XCTestCase {

    // MARK: - Int32 Tests

    func testSimpleRepeatedInt32() throws {
        // Test: [[1, 2], [3], [4, 5, 6]]
        let url = fixtureURL("repeated_int32_simple.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertNotNil(result[2], "Third array should not be null")
        XCTAssertEqual(result[0]!, [1, 2])
        XCTAssertEqual(result[1]!, [3])
        XCTAssertEqual(result[2]!, [4, 5, 6])
    }

    func testRepeatedInt32WithEmpty() throws {
        // Test: [[1, 2], [], [3]]
        let url = fixtureURL("repeated_int32_empty.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertNotNil(result[2], "Third array should not be null")
        XCTAssertEqual(result[0]!, [1, 2])
        XCTAssertEqual(result[1]!, [], "Second array should be empty")
        XCTAssertEqual(result[2]!, [3])
    }

    func testRepeatedInt32WithNulls() throws {
        // Test: [[1, None, 2], [None], [3, 4]]
        let url = fixtureURL("repeated_int32_nulls.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertEqual(result[0]!.count, 3)
        XCTAssertEqual(result[0]![0], 1)
        XCTAssertNil(result[0]![1], "Second element should be nil")
        XCTAssertEqual(result[0]![2], 2)

        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertEqual(result[1]!.count, 1)
        XCTAssertNil(result[1]![0], "Single element should be nil")

        XCTAssertNotNil(result[2], "Third array should not be null")
        XCTAssertEqual(result[2]!, [3, 4])
    }

    func testRepeatedInt32AllEmpty() throws {
        // Test: [[], [], []]
        let url = fixtureURL("repeated_int32_all_empty.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertNotNil(result[2], "Third array should not be null")
        XCTAssertEqual(result[0]!, [], "First array should be empty")
        XCTAssertEqual(result[1]!, [], "Second array should be empty")
        XCTAssertEqual(result[2]!, [], "Third array should be empty")
    }

    func testRepeatedInt32SingleElement() throws {
        // Test: [[1], [2], [3], [4], [5]]
        let url = fixtureURL("repeated_int32_single.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 5, "Should have 5 arrays")
        for i in 0..<5 {
            XCTAssertNotNil(result[i], "Array at index \(i) should not be null")
            XCTAssertEqual(result[i]!, [Int32(i + 1)])
        }
    }

    func testRepeatedInt32Large() throws {
        // Test: [[1, 2, 3, ..., 100]]
        let url = fixtureURL("repeated_int32_large.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 1, "Should have 1 array")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertEqual(result[0]!.count, 100, "Array should have 100 elements")

        let expected = Array(1...100).map { Int32($0) }
        XCTAssertEqual(result[0]!, expected)
    }

    // MARK: - Int64 Tests

    func testRepeatedInt64() throws {
        // Test: [[100, 200], [300]]
        let url = fixtureURL("repeated_int64.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int64Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 2, "Should have 2 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertEqual(result[0]!, [100, 200])
        XCTAssertEqual(result[1]!, [300])
    }

    // MARK: - Float Tests

    func testRepeatedFloat() throws {
        // Test: [[1.5, 2.5], [], [3.5]]
        let url = fixtureURL("repeated_float.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.floatColumn(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertNotNil(result[2], "Third array should not be null")
        XCTAssertEqual(result[0]!, [1.5, 2.5])
        XCTAssertEqual(result[1]!, [], "Second array should be empty")
        XCTAssertEqual(result[2]!, [3.5])
    }

    // MARK: - Double Tests

    func testRepeatedDouble() throws {
        // Test: [[1.1, 2.2], [3.3, 4.4]]
        let url = fixtureURL("repeated_double.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.doubleColumn(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 2, "Should have 2 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertEqual(result[0]!, [1.1, 2.2])
        XCTAssertEqual(result[1]!, [3.3, 4.4])
    }

    // MARK: - String Tests

    func testRepeatedString() throws {
        // Test: [["Alice", "Bob"], [], ["Charlie"]]
        let url = fixtureURL("repeated_string.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.stringColumn(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 arrays")
        XCTAssertNotNil(result[0], "First array should not be null")
        XCTAssertNotNil(result[1], "Second array should not be null")
        XCTAssertNotNil(result[2], "Third array should not be null")
        XCTAssertEqual(result[0]!, ["Alice", "Bob"])
        XCTAssertEqual(result[1]!, [], "Second array should be empty")
        XCTAssertEqual(result[2]!, ["Charlie"])
    }

    // MARK: - Mixed Column Tests

    func testRepeatedMixed() throws {
        // Test multiple repeated columns in same file
        let url = fixtureURL("repeated_mixed.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)

        // int_lists: [[1, 2], [3], []]
        let intColumn = try rowGroup.int32Column(at: 0)
        let intResult = try intColumn.readAllRepeated()

        XCTAssertEqual(intResult.count, 3)
        XCTAssertNotNil(intResult[0], "First int array should not be null")
        XCTAssertNotNil(intResult[1], "Second int array should not be null")
        XCTAssertNotNil(intResult[2], "Third int array should not be null")
        XCTAssertEqual(intResult[0]!, [1, 2])
        XCTAssertEqual(intResult[1]!, [3])
        XCTAssertEqual(intResult[2]!, [])

        // str_lists: [["a"], [], ["b", "c"]]
        let strColumn = try rowGroup.stringColumn(at: 1)
        let strResult = try strColumn.readAllRepeated()

        XCTAssertEqual(strResult.count, 3)
        XCTAssertNotNil(strResult[0], "First string array should not be null")
        XCTAssertNotNil(strResult[1], "Second string array should not be null")
        XCTAssertNotNil(strResult[2], "Third string array should not be null")
        XCTAssertEqual(strResult[0]!, ["a"])
        XCTAssertEqual(strResult[1]!, [])
        XCTAssertEqual(strResult[2]!, ["b", "c"])
    }

    // MARK: - Null vs Empty List Tests

    func testInt32WithNullLists() throws {
        // Test: [[1, 2], None, [], [3]]
        // Verify distinction between NULL list (None) and EMPTY list ([])
        let url = fixtureURL("repeated_int32_with_null_list.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 4, "Should have 4 elements")

        // [1, 2]
        XCTAssertNotNil(result[0], "First element should be a list")
        XCTAssertEqual(result[0]!.count, 2)
        XCTAssertEqual(result[0]![0], 1)
        XCTAssertEqual(result[0]![1], 2)

        // None (NULL list)
        XCTAssertNil(result[1], "Second element should be NULL list (nil)")

        // [] (EMPTY list)
        XCTAssertNotNil(result[2], "Third element should be EMPTY list (not nil)")
        XCTAssertEqual(result[2]!.count, 0, "Third element should be empty array")

        // [3]
        XCTAssertNotNil(result[3], "Fourth element should be a list")
        XCTAssertEqual(result[3]!, [3])
    }

    func testFloatWithNullLists() throws {
        // Test: [None, [1.5], None, [], [2.5, 3.5]]
        let url = fixtureURL("repeated_float_with_null_list.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.floatColumn(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 5, "Should have 5 elements")

        // None (NULL list)
        XCTAssertNil(result[0], "First element should be NULL list")

        // [1.5]
        XCTAssertNotNil(result[1], "Second element should be a list")
        XCTAssertEqual(result[1]!, [1.5])

        // None (NULL list)
        XCTAssertNil(result[2], "Third element should be NULL list")

        // [] (EMPTY list)
        XCTAssertNotNil(result[3], "Fourth element should be EMPTY list (not nil)")
        XCTAssertEqual(result[3]!.count, 0, "Fourth element should be empty array")

        // [2.5, 3.5]
        XCTAssertNotNil(result[4], "Fifth element should be a list")
        XCTAssertEqual(result[4]!, [2.5, 3.5])
    }

    func testStringWithNullLists() throws {
        // Test: [["a", "b"], None, [], None, ["c"]]
        let url = fixtureURL("repeated_string_with_null_list.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.stringColumn(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 5, "Should have 5 elements")

        // ["a", "b"]
        XCTAssertNotNil(result[0], "First element should be a list")
        XCTAssertEqual(result[0]!, ["a", "b"])

        // None (NULL list)
        XCTAssertNil(result[1], "Second element should be NULL list")

        // [] (EMPTY list)
        XCTAssertNotNil(result[2], "Third element should be EMPTY list (not nil)")
        XCTAssertEqual(result[2]!.count, 0, "Third element should be empty array")

        // None (NULL list)
        XCTAssertNil(result[3], "Fourth element should be NULL list")

        // ["c"]
        XCTAssertNotNil(result[4], "Fifth element should be a list")
        XCTAssertEqual(result[4]!, ["c"])
    }

    func testAllNullLists() throws {
        // Test: [None, None, None]
        let url = fixtureURL("repeated_int32_all_null.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 3, "Should have 3 elements")
        XCTAssertNil(result[0], "First element should be NULL list")
        XCTAssertNil(result[1], "Second element should be NULL list")
        XCTAssertNil(result[2], "Third element should be NULL list")
    }

    func testMixedNullsEmptiesAndValues() throws {
        // Test: [None, [], [1], None, [2, 3], [], None, [4]]
        let url = fixtureURL("repeated_int32_mixed_null_empty.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        let result = try column.readAllRepeated()

        XCTAssertEqual(result.count, 8, "Should have 8 elements")

        XCTAssertNil(result[0], "Element 0 should be NULL list")

        XCTAssertNotNil(result[1], "Element 1 should be EMPTY list")
        XCTAssertEqual(result[1]!.count, 0, "Element 1 should be empty array")

        XCTAssertNotNil(result[2], "Element 2 should be a list")
        XCTAssertEqual(result[2]!, [1])

        XCTAssertNil(result[3], "Element 3 should be NULL list")

        XCTAssertNotNil(result[4], "Element 4 should be a list")
        XCTAssertEqual(result[4]!, [2, 3])

        XCTAssertNotNil(result[5], "Element 5 should be EMPTY list")
        XCTAssertEqual(result[5]!.count, 0, "Element 5 should be empty array")

        XCTAssertNil(result[6], "Element 6 should be NULL list")

        XCTAssertNotNil(result[7], "Element 7 should be a list")
        XCTAssertEqual(result[7]!, [4])
    }

    // MARK: - Error Tests

    func testFlatMethodsRejectRepeatedColumns() throws {
        // Verify that readAll() throws error on repeated column
        let url = fixtureURL("repeated_int32_simple.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int32Column(at: 0)

        // Should throw unsupportedFeature error
        XCTAssertThrowsError(try column.readAll()) { error in
            guard case ColumnReaderError.unsupportedFeature(let message) = error else {
                XCTFail("Expected unsupportedFeature error, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("repeated"))
            XCTAssertTrue(message.contains("readAllRepeated"))
        }

        // readBatch should also throw
        XCTAssertThrowsError(try column.readBatch(count: 10)) { error in
            guard case ColumnReaderError.unsupportedFeature = error else {
                XCTFail("Expected unsupportedFeature error")
                return
            }
        }

        // readOne should also throw
        XCTAssertThrowsError(try column.readOne()) { error in
            guard case ColumnReaderError.unsupportedFeature = error else {
                XCTFail("Expected unsupportedFeature error")
                return
            }
        }
    }

    func testReadAllRepeatedRejectsFlatColumns() throws {
        // Verify that readAllRepeated() throws error on flat column
        let url = fixtureURL("pyarrow_test.parquet")
        let reader = try ParquetFileReader(url: url)
        defer { try? reader.close() }

        let rowGroup = try reader.rowGroup(at: 0)
        let column = try rowGroup.int64Column(at: 0)  // Non-repeated INT64 column

        // Should throw unsupportedFeature error
        XCTAssertThrowsError(try column.readAllRepeated()) { error in
            guard case ColumnReaderError.unsupportedFeature(let message) = error else {
                XCTFail("Expected unsupportedFeature error, got \(error)")
                return
            }
            XCTAssertTrue(message.contains("not repeated"))
            XCTAssertTrue(message.contains("readAll"))
        }
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
