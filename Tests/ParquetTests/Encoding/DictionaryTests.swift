// DictionaryTests - Tests for dictionary encoding support
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class DictionaryTests: XCTestCase {

    // MARK: - Int32 Dictionary Tests

    func testInt32DictionaryCreation() throws {
        // Create dictionary page with 5 Int32 values: [10, 20, 30, 40, 50]
        var data = Data()
        for value in [Int32(10), 20, 30, 40, 50] {
            var v = value.littleEndian
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 5, encoding: .plain)
        let dictionary = try Dictionary.int32(page: page)

        XCTAssertEqual(dictionary.count, 5)
        XCTAssertEqual(try dictionary.value(at: 0), 10)
        XCTAssertEqual(try dictionary.value(at: UInt32(1)), 20)
        XCTAssertEqual(try dictionary.value(at: 2), 30)
        XCTAssertEqual(try dictionary.value(at: 3), 40)
        XCTAssertEqual(try dictionary.value(at: 4), 50)
    }

    func testInt32DictionaryBatchLookup() throws {
        // Dictionary: [100, 200, 300]
        var data = Data()
        for value in [Int32(100), 200, 300] {
            var v = value.littleEndian
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.int32(page: page)

        // Look up indices: [0, 2, 1, 0, 2]
        let indices: [UInt32] = [0, 2, 1, 0, 2]
        let values = try dictionary.values(at: indices)

        XCTAssertEqual(values, [100, 300, 200, 100, 300])
    }

    func testInt32IndexOutOfBounds() throws {
        // Dictionary: [1, 2, 3]
        var data = Data()
        for value in [Int32(1), 2, 3] {
            var v = value.littleEndian
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.int32(page: page)

        // Valid index
        XCTAssertNoThrow(try dictionary.value(at: 2))

        // Out of bounds
        XCTAssertThrowsError(try dictionary.value(at: 3)) { error in
            guard case DictionaryError.indexOutOfBounds(let index, let max) = error else {
                XCTFail("Expected indexOutOfBounds error, got \(error)")
                return
            }
            XCTAssertEqual(index, 3)
            XCTAssertEqual(max, 2)
        }

        // Negative index
        XCTAssertThrowsError(try dictionary.value(at: -1)) { error in
            XCTAssertTrue(error is DictionaryError)
        }

        // UInt32 out of bounds
        XCTAssertThrowsError(try dictionary.value(at: UInt32(3))) { error in
            guard case DictionaryError.indexOutOfBounds = error else {
                XCTFail("Expected indexOutOfBounds error, got \(error)")
                return
            }
        }
    }

    // MARK: - Int64 Dictionary Tests

    func testInt64Dictionary() throws {
        // Dictionary: [1000000000, 2000000000, 3000000000]
        var data = Data()
        for value in [Int64(1000000000), 2000000000, 3000000000] {
            var v = value.littleEndian
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.int64(page: page)

        XCTAssertEqual(dictionary.count, 3)
        XCTAssertEqual(try dictionary.value(at: 0), 1000000000)
        XCTAssertEqual(try dictionary.value(at: 1), 2000000000)
        XCTAssertEqual(try dictionary.value(at: 2), 3000000000)
    }

    // MARK: - Float Dictionary Tests

    func testFloatDictionary() throws {
        // Dictionary: [1.5, 2.5, 3.5]
        var data = Data()
        for value in [Float(1.5), 2.5, 3.5] {
            var bits = value.bitPattern.littleEndian
            withUnsafeBytes(of: &bits) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.float(page: page)

        XCTAssertEqual(dictionary.count, 3)
        XCTAssertEqual(try dictionary.value(at: 0), 1.5, accuracy: 0.001)
        XCTAssertEqual(try dictionary.value(at: 1), 2.5, accuracy: 0.001)
        XCTAssertEqual(try dictionary.value(at: 2), 3.5, accuracy: 0.001)
    }

    // MARK: - Double Dictionary Tests

    func testDoubleDictionary() throws {
        // Dictionary: [1.123456, 2.234567, 3.345678]
        var data = Data()
        for value in [Double(1.123456), 2.234567, 3.345678] {
            var bits = value.bitPattern.littleEndian
            withUnsafeBytes(of: &bits) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.double(page: page)

        XCTAssertEqual(dictionary.count, 3)
        XCTAssertEqual(try dictionary.value(at: 0), 1.123456, accuracy: 0.000001)
        XCTAssertEqual(try dictionary.value(at: 1), 2.234567, accuracy: 0.000001)
        XCTAssertEqual(try dictionary.value(at: 2), 3.345678, accuracy: 0.000001)
    }

    // MARK: - String Dictionary Tests

    func testStringDictionary() throws {
        // Dictionary: ["apple", "banana", "cherry"]
        var data = Data()
        for string in ["apple", "banana", "cherry"] {
            let bytes = string.data(using: .utf8)!
            var length = UInt32(bytes.count).littleEndian
            withUnsafeBytes(of: &length) { data.append(contentsOf: $0) }
            data.append(bytes)
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.string(page: page)

        XCTAssertEqual(dictionary.count, 3)
        XCTAssertEqual(try dictionary.value(at: 0), "apple")
        XCTAssertEqual(try dictionary.value(at: 1), "banana")
        XCTAssertEqual(try dictionary.value(at: 2), "cherry")
    }

    func testStringDictionaryBatchLookup() throws {
        // Dictionary: ["red", "green", "blue"]
        var data = Data()
        for string in ["red", "green", "blue"] {
            let bytes = string.data(using: .utf8)!
            var length = UInt32(bytes.count).littleEndian
            withUnsafeBytes(of: &length) { data.append(contentsOf: $0) }
            data.append(bytes)
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.string(page: page)

        // Look up indices: [1, 0, 2, 1, 0]
        let indices: [UInt32] = [1, 0, 2, 1, 0]
        let values = try dictionary.values(at: indices)

        XCTAssertEqual(values, ["green", "red", "blue", "green", "red"])
    }

    func testStringDictionaryEmptyString() throws {
        // Dictionary: ["", "hello", ""]
        var data = Data()
        for string in ["", "hello", ""] {
            let bytes = string.data(using: .utf8)!
            var length = UInt32(bytes.count).littleEndian
            withUnsafeBytes(of: &length) { data.append(contentsOf: $0) }
            data.append(bytes)
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plain)
        let dictionary = try Dictionary.string(page: page)

        XCTAssertEqual(try dictionary.value(at: 0), "")
        XCTAssertEqual(try dictionary.value(at: 1), "hello")
        XCTAssertEqual(try dictionary.value(at: 2), "")
    }

    // MARK: - Error Tests

    func testPlainDictionaryEncoding() throws {
        // Older writers use PLAIN_DICTIONARY encoding for dictionary pages
        var data = Data()
        for value in [Int32(100), 200, 300] {
            var v = value.littleEndian
            withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 3, encoding: .plainDictionary)
        let dictionary = try Dictionary.int32(page: page)

        XCTAssertEqual(dictionary.count, 3)
        XCTAssertEqual(try dictionary.value(at: 0), 100)
        XCTAssertEqual(try dictionary.value(at: 1), 200)
        XCTAssertEqual(try dictionary.value(at: 2), 300)
    }

    func testUnsupportedEncoding() throws {
        // Create dictionary page with RLE encoding (unsupported for dictionary values)
        let data = Data([0x01, 0x02, 0x03])
        let page = DictionaryPage(data: data, numValues: 3, encoding: .rle)

        XCTAssertThrowsError(try Dictionary.int32(page: page)) { error in
            guard case DictionaryError.unsupportedEncoding(let encoding) = error else {
                XCTFail("Expected unsupportedEncoding error, got \(error)")
                return
            }
            XCTAssertEqual(encoding, .rle)
        }
    }

    func testEmptyDictionary() throws {
        // Empty dictionary (0 values)
        let data = Data()
        let page = DictionaryPage(data: data, numValues: 0, encoding: .plain)
        let dictionary = try Dictionary.int32(page: page)

        XCTAssertEqual(dictionary.count, 0)

        // Any index access should fail
        XCTAssertThrowsError(try dictionary.value(at: 0)) { error in
            XCTAssertTrue(error is DictionaryError)
        }
    }

    func testSingleValueDictionary() throws {
        // Dictionary with single value: [42]
        var data = Data()
        var value = Int32(42).littleEndian
        withUnsafeBytes(of: &value) { data.append(contentsOf: $0) }

        let page = DictionaryPage(data: data, numValues: 1, encoding: .plain)
        let dictionary = try Dictionary.int32(page: page)

        XCTAssertEqual(dictionary.count, 1)
        XCTAssertEqual(try dictionary.value(at: 0), 42)

        // Index 1 should be out of bounds
        XCTAssertThrowsError(try dictionary.value(at: 1)) { error in
            XCTAssertTrue(error is DictionaryError)
        }
    }

    func testLargeDictionary() throws {
        // Dictionary with 10,000 values
        var data = Data()
        for i in 0..<10000 {
            var value = Int32(i).littleEndian
            withUnsafeBytes(of: &value) { data.append(contentsOf: $0) }
        }

        let page = DictionaryPage(data: data, numValues: 10000, encoding: .plain)
        let dictionary = try Dictionary.int32(page: page)

        XCTAssertEqual(dictionary.count, 10000)
        XCTAssertEqual(try dictionary.value(at: 0), 0)
        XCTAssertEqual(try dictionary.value(at: 5000), 5000)
        XCTAssertEqual(try dictionary.value(at: 9999), 9999)

        // Out of bounds
        XCTAssertThrowsError(try dictionary.value(at: 10000))
    }
}
