// Tests for PLAIN encoding decoder
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class PlainDecoderTests: XCTestCase {
    // MARK: - Boolean Tests

    func testBooleanDecoding() throws {
        // Test data: 0xB2 = 10110010 (binary, MSB first)
        // Bit-packed LSB first means: bit0=0, bit1=1, bit2=0, bit3=0, bit4=1, bit5=1, bit6=0, bit7=1
        // Result: [false, true, false, false, true, true, false, true]
        let data = Data([0xB2])
        let decoder = PlainDecoder<Bool>(data: data)

        let values = try decoder.decode(count: 8)
        XCTAssertEqual(values, [false, true, false, false, true, true, false, true])
    }

    func testBooleanSingleValue() throws {
        let data = Data([0x01]) // true
        let decoder = PlainDecoder<Bool>(data: data)

        let value = try decoder.decodeOne()
        XCTAssertTrue(value)
    }

    func testBooleanInsufficientData() throws {
        let data = Data([0xFF])
        let decoder = PlainDecoder<Bool>(data: data)

        // Can decode 8 values
        _ = try decoder.decode(count: 8)

        // But not 9
        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            XCTAssertEqual(error as? DecoderError, .unexpectedEOF)
        }
    }

    // MARK: - Int32 Tests

    func testInt32Decoding() throws {
        // Test values: [42, -100, 0, Int32.max, Int32.min]
        var data = Data()
        data.append(contentsOf: Int32(42).littleEndianBytes)
        data.append(contentsOf: Int32(-100).littleEndianBytes)
        data.append(contentsOf: Int32(0).littleEndianBytes)
        data.append(contentsOf: Int32.max.littleEndianBytes)
        data.append(contentsOf: Int32.min.littleEndianBytes)

        let decoder = PlainDecoder<Int32>(data: data)
        let values = try decoder.decode(count: 5)

        XCTAssertEqual(values, [42, -100, 0, Int32.max, Int32.min])
    }

    func testInt32SingleValue() throws {
        let data = Data([0x2A, 0x00, 0x00, 0x00]) // 42 in little-endian
        let decoder = PlainDecoder<Int32>(data: data)

        let value = try decoder.decodeOne()
        XCTAssertEqual(value, 42)
    }

    func testInt32InsufficientData() throws {
        let data = Data([0x00, 0x00, 0x00]) // Only 3 bytes
        let decoder = PlainDecoder<Int32>(data: data)

        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.insufficientData = error else {
                XCTFail("Expected insufficientData error")
                return
            }
        }
    }

    // MARK: - Int64 Tests

    func testInt64Decoding() throws {
        // Test values: [42, -100, 0, Int64.max, Int64.min]
        var data = Data()
        data.append(contentsOf: Int64(42).littleEndianBytes)
        data.append(contentsOf: Int64(-100).littleEndianBytes)
        data.append(contentsOf: Int64(0).littleEndianBytes)
        data.append(contentsOf: Int64.max.littleEndianBytes)
        data.append(contentsOf: Int64.min.littleEndianBytes)

        let decoder = PlainDecoder<Int64>(data: data)
        let values = try decoder.decode(count: 5)

        XCTAssertEqual(values, [42, -100, 0, Int64.max, Int64.min])
    }

    func testInt64SingleValue() throws {
        let data = Data([0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]) // 42
        let decoder = PlainDecoder<Int64>(data: data)

        let value = try decoder.decodeOne()
        XCTAssertEqual(value, 42)
    }

    // MARK: - Int96 Tests

    func testInt96Decoding() throws {
        // Create 12 bytes for Int96
        let bytes1 = Data([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C])
        let bytes2 = Data([0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4])

        var data = Data()
        data.append(bytes1)
        data.append(bytes2)

        let decoder = PlainDecoder<Int96>(data: data)
        let values = try decoder.decode(count: 2)

        XCTAssertEqual(values[0].bytes, bytes1)
        XCTAssertEqual(values[1].bytes, bytes2)
    }

    func testInt96InsufficientData() throws {
        let data = Data([0x00, 0x00, 0x00, 0x00, 0x00]) // Only 5 bytes
        let decoder = PlainDecoder<Int96>(data: data)

        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.insufficientData = error else {
                XCTFail("Expected insufficientData error")
                return
            }
        }
    }

    // MARK: - Float Tests

    func testFloatDecoding() throws {
        // Test values: [1.5, -2.75, 0.0, Float.infinity, -Float.infinity]
        var data = Data()
        data.append(contentsOf: Float(1.5).littleEndianBytes)
        data.append(contentsOf: Float(-2.75).littleEndianBytes)
        data.append(contentsOf: Float(0.0).littleEndianBytes)
        data.append(contentsOf: Float.infinity.littleEndianBytes)
        data.append(contentsOf: (-Float.infinity).littleEndianBytes)

        let decoder = PlainDecoder<Float>(data: data)
        let values = try decoder.decode(count: 5)

        XCTAssertEqual(values[0], 1.5)
        XCTAssertEqual(values[1], -2.75)
        XCTAssertEqual(values[2], 0.0)
        XCTAssertTrue(values[3].isInfinite && values[3] > 0)
        XCTAssertTrue(values[4].isInfinite && values[4] < 0)
    }

    func testFloatNaN() throws {
        var data = Data()
        data.append(contentsOf: Float.nan.littleEndianBytes)

        let decoder = PlainDecoder<Float>(data: data)
        let value = try decoder.decodeOne()

        XCTAssertTrue(value.isNaN)
    }

    // MARK: - Double Tests

    func testDoubleDecoding() throws {
        // Test values: [1.5, -2.75, 0.0, Double.pi]
        var data = Data()
        data.append(contentsOf: Double(1.5).littleEndianBytes)
        data.append(contentsOf: Double(-2.75).littleEndianBytes)
        data.append(contentsOf: Double(0.0).littleEndianBytes)
        data.append(contentsOf: Double.pi.littleEndianBytes)

        let decoder = PlainDecoder<Double>(data: data)
        let values = try decoder.decode(count: 4)

        XCTAssertEqual(values[0], 1.5)
        XCTAssertEqual(values[1], -2.75)
        XCTAssertEqual(values[2], 0.0)
        XCTAssertEqual(values[3], Double.pi)
    }

    // MARK: - ByteArray Tests

    func testByteArrayDecoding() throws {
        // Create byte arrays with length prefixes
        // Array 1: "hello" (5 bytes)
        // Array 2: "world" (5 bytes)
        // Array 3: "" (empty)
        var data = Data()

        // "hello"
        data.append(contentsOf: UInt32(5).littleEndianBytes)
        data.append(contentsOf: "hello".utf8)

        // "world"
        data.append(contentsOf: UInt32(5).littleEndianBytes)
        data.append(contentsOf: "world".utf8)

        // Empty
        data.append(contentsOf: UInt32(0).littleEndianBytes)

        let decoder = PlainDecoder<Data>(data: data)
        let values = try decoder.decode(count: 3)

        XCTAssertEqual(values[0], Data("hello".utf8))
        XCTAssertEqual(values[1], Data("world".utf8))
        XCTAssertEqual(values[2], Data())
    }

    func testByteArrayInvalidLength() throws {
        // Negative length
        var data = Data()
        data.append(contentsOf: Int32(-1).littleEndianBytes)

        let decoder = PlainDecoder<Data>(data: data)

        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.invalidData = error else {
                XCTFail("Expected invalidData error")
                return
            }
        }
    }

    func testByteArrayInsufficientData() throws {
        // Length says 100 bytes, but only 10 provided
        var data = Data()
        data.append(contentsOf: UInt32(100).littleEndianBytes)
        data.append(contentsOf: Data(repeating: 0, count: 10))

        let decoder = PlainDecoder<Data>(data: data)

        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.insufficientData = error else {
                XCTFail("Expected insufficientData error")
                return
            }
        }
    }

    // MARK: - FixedLenByteArray Tests

    func testFixedLenByteArrayDecoding() throws {
        // Three 4-byte fixed arrays
        let bytes1 = Data([0x01, 0x02, 0x03, 0x04])
        let bytes2 = Data([0x05, 0x06, 0x07, 0x08])
        let bytes3 = Data([0x09, 0x0A, 0x0B, 0x0C])

        var data = Data()
        data.append(bytes1)
        data.append(bytes2)
        data.append(bytes3)

        let decoder = PlainDecoder<Data>(data: data, fixedLength: 4)
        let values = try decoder.decode(count: 3)

        XCTAssertEqual(values[0], bytes1)
        XCTAssertEqual(values[1], bytes2)
        XCTAssertEqual(values[2], bytes3)
    }

    func testFixedLenByteArrayInsufficientData() throws {
        let data = Data([0x01, 0x02, 0x03]) // Only 3 bytes, need 4

        let decoder = PlainDecoder<Data>(data: data, fixedLength: 4)

        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.insufficientData = error else {
                XCTFail("Expected insufficientData error")
                return
            }
        }
    }

    func testFixedLenByteArrayPartialValue() throws {
        // 10 bytes with fixed length 4: can decode 2 values, 3rd fails
        let data = Data([0x01, 0x02, 0x03, 0x04,  // Value 1
                         0x05, 0x06, 0x07, 0x08,  // Value 2
                         0x09, 0x0A])              // Incomplete value 3

        let decoder = PlainDecoder<Data>(data: data, fixedLength: 4)

        // First two values should succeed
        _ = try decoder.decodeOne()
        _ = try decoder.decodeOne()

        // Third should fail (only 2 bytes remaining)
        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.insufficientData = error else {
                XCTFail("Expected insufficientData error")
                return
            }
        }
    }

    // MARK: - String Tests

    func testStringDecoding() throws {
        // Strings are byte arrays with UTF-8 encoding
        var data = Data()

        // "hello"
        data.append(contentsOf: UInt32(5).littleEndianBytes)
        data.append(contentsOf: "hello".utf8)

        // "世界" (UTF-8: 6 bytes)
        let worldBytes = "世界".data(using: .utf8)!
        data.append(contentsOf: UInt32(worldBytes.count).littleEndianBytes)
        data.append(worldBytes)

        // Empty string
        data.append(contentsOf: UInt32(0).littleEndianBytes)

        let decoder = PlainDecoder<String>(data: data)
        let values = try decoder.decode(count: 3)

        XCTAssertEqual(values[0], "hello")
        XCTAssertEqual(values[1], "世界")
        XCTAssertEqual(values[2], "")
    }

    func testStringInvalidUTF8() throws {
        // Invalid UTF-8 sequence
        var data = Data()
        data.append(contentsOf: UInt32(2).littleEndianBytes)
        data.append(contentsOf: [0xFF, 0xFE]) // Invalid UTF-8

        let decoder = PlainDecoder<String>(data: data)

        XCTAssertThrowsError(try decoder.decodeOne()) { error in
            guard case DecoderError.invalidData = error else {
                XCTFail("Expected invalidData error")
                return
            }
        }
    }

    // MARK: - Mixed Decoding Tests

    func testSequentialDecoding() throws {
        // Test decoding values one at a time
        var data = Data()
        data.append(contentsOf: Int32(1).littleEndianBytes)
        data.append(contentsOf: Int32(2).littleEndianBytes)
        data.append(contentsOf: Int32(3).littleEndianBytes)

        let decoder = PlainDecoder<Int32>(data: data)

        XCTAssertEqual(try decoder.decodeOne(), 1)
        XCTAssertEqual(try decoder.decodeOne(), 2)
        XCTAssertEqual(try decoder.decodeOne(), 3)
    }

    func testLargeArrayDecoding() throws {
        // Test decoding many values efficiently
        let count = 10000
        var data = Data()

        for i in 0..<count {
            data.append(contentsOf: Int64(i).littleEndianBytes)
        }

        let decoder = PlainDecoder<Int64>(data: data)
        let values = try decoder.decode(count: count)

        XCTAssertEqual(values.count, count)
        XCTAssertEqual(values.first, 0)
        XCTAssertEqual(values.last, Int64(count - 1))
    }
}

// MARK: - Helper Extensions

extension Int32 {
    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.littleEndian) { Array($0) }
    }
}

extension Int64 {
    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.littleEndian) { Array($0) }
    }
}

extension UInt32 {
    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.littleEndian) { Array($0) }
    }
}

extension Float {
    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.bitPattern.littleEndian) { Array($0) }
    }
}

extension Double {
    var littleEndianBytes: [UInt8] {
        withUnsafeBytes(of: self.bitPattern.littleEndian) { Array($0) }
    }
}
