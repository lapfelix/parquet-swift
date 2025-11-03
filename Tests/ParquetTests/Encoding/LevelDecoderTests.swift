// LevelDecoderTests - Tests for level decoding
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class LevelDecoderTests: XCTestCase {

    // MARK: - Varint Decoding Tests

    func testVarintSingleByte() throws {
        // Value: 0 (varint: 0x00)
        // Format: <length: 4> <varint: 1 byte> <value: 0 bytes for bit-width 0>
        var data = Data()
        data.append(contentsOf: [1, 0, 0, 0])  // length = 1
        data.append(0x00)  // varint: 0 (RLE run of 0 values - edge case)

        let decoder = LevelDecoder()
        // maxLevel = 0 means bit-width = 0 (single value)
        let levels = try decoder.decodeLevels(from: data, numValues: 0, maxLevel: 0)
        XCTAssertEqual(levels.count, 0)
    }

    func testVarintMultiByte() throws {
        // RLE run of 300 values (300 << 1 = 600 = 0x258)
        // Varint encoding of 600:
        // 600 = 0b1001011000
        // Split into 7-bit chunks: 0000100 1011000
        // Reverse: 1011000 0000100
        // Add continuation bits: 11011000 00000100 = 0xD8 0x04

        var data = Data()
        data.append(contentsOf: [3, 0, 0, 0])  // length = 3 bytes
        data.append(contentsOf: [0xD8, 0x04])  // varint: 600 (RLE run)
        data.append(0x01)  // repeated value: 1

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 300, maxLevel: 1)

        XCTAssertEqual(levels.count, 300)
        XCTAssertTrue(levels.allSatisfy { $0 == 1 })
    }

    // MARK: - RLE Run Tests

    func testRLERunAllZeros() throws {
        // RLE run: 10 zeros
        // Header: (10 << 1) | 0 = 20 = 0x14
        // Value: 0 (1 byte for bit-width up to 8)

        var data = Data()
        data.append(contentsOf: [2, 0, 0, 0])  // length = 2 bytes
        data.append(0x14)  // varint: 20 (10 values, RLE)
        data.append(0x00)  // repeated value: 0

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 10, maxLevel: 1)

        XCTAssertEqual(levels.count, 10)
        XCTAssertTrue(levels.allSatisfy { $0 == 0 })
    }

    func testRLERunAllOnes() throws {
        // RLE run: 10 ones
        // Header: (10 << 1) | 0 = 20 = 0x14
        // Value: 1 (1 byte)

        var data = Data()
        data.append(contentsOf: [2, 0, 0, 0])  // length = 2 bytes
        data.append(0x14)  // varint: 20 (10 values, RLE)
        data.append(0x01)  // repeated value: 1

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 10, maxLevel: 1)

        XCTAssertEqual(levels.count, 10)
        XCTAssertTrue(levels.allSatisfy { $0 == 1 })
    }

    // MARK: - Bit-Packed Run Tests

    func testBitPackedRunSingleGroup() throws {
        // Bit-packed run: 8 values alternating 1,0,1,0,1,0,1,0
        // Header: (1 << 1) | 1 = 3 (1 group, bit-packed)
        // Values packed with bit-width 1 (LSB first):
        // 0b01010101 = 0x55
        // LSB-first means bit 0 is the first value

        var data = Data()
        data.append(contentsOf: [2, 0, 0, 0])  // length = 2 bytes
        data.append(0x03)  // varint: 3 (1 group, bit-packed)
        data.append(0x55)  // packed values: LSB first = 1,0,1,0,1,0,1,0

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 8, maxLevel: 1)

        XCTAssertEqual(levels.count, 8)
        XCTAssertEqual(levels, [1, 0, 1, 0, 1, 0, 1, 0])
    }

    func testBitPackedRunTwoGroups() throws {
        // Bit-packed run: 16 values, all 1s
        // Header: (2 << 1) | 1 = 5 (2 groups, bit-packed)
        // Values: 0xFF, 0xFF (all bits set)

        var data = Data()
        data.append(contentsOf: [3, 0, 0, 0])  // length = 3 bytes
        data.append(0x05)  // varint: 5 (2 groups, bit-packed)
        data.append(contentsOf: [0xFF, 0xFF])  // 16 bits all set

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 16, maxLevel: 1)

        XCTAssertEqual(levels.count, 16)
        XCTAssertTrue(levels.allSatisfy { $0 == 1 })
    }

    func testBitPackedRunWithPadding() throws {
        // Bit-packed run: 10 values (requires padding to 16)
        // Header: (2 << 1) | 1 = 5 (2 groups = 16 slots, only 10 used)
        // Values: first 10 are 1, last 6 are padded with 0

        var data = Data()
        data.append(contentsOf: [3, 0, 0, 0])  // length = 3 bytes
        data.append(0x05)  // varint: 5 (2 groups, bit-packed)
        // 10 ones followed by 6 zeros: 0xFF, 0x03 (0b11111111, 0b00000011)
        data.append(contentsOf: [0xFF, 0x03])

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 10, maxLevel: 1)

        XCTAssertEqual(levels.count, 10)
        XCTAssertEqual(levels, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    }

    // MARK: - Mixed Run Tests

    func testMixedRLEAndBitPacked() throws {
        // First: RLE run of 5 zeros
        // Header: (5 << 1) | 0 = 10 = 0x0A
        // Value: 0
        //
        // Second: Bit-packed run of 8 ones
        // Header: (1 << 1) | 1 = 3
        // Value: 0xFF

        var data = Data()
        data.append(contentsOf: [4, 0, 0, 0])  // length = 4 bytes
        data.append(0x0A)  // varint: 10 (5 values, RLE)
        data.append(0x00)  // repeated value: 0
        data.append(0x03)  // varint: 3 (1 group, bit-packed)
        data.append(0xFF)  // 8 ones

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 13, maxLevel: 1)

        XCTAssertEqual(levels.count, 13)
        XCTAssertEqual(levels, [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1])
    }

    // MARK: - Bit-Width Tests

    func testBitWidth2() throws {
        // maxLevel = 3 requires bit-width = 2
        // RLE run: 5 values of 2
        // Header: (5 << 1) | 0 = 10 = 0x0A
        // Value: 2 (1 byte)

        var data = Data()
        data.append(contentsOf: [2, 0, 0, 0])  // length = 2 bytes
        data.append(0x0A)  // varint: 10 (5 values, RLE)
        data.append(0x02)  // repeated value: 2

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 5, maxLevel: 3)

        XCTAssertEqual(levels.count, 5)
        XCTAssertTrue(levels.allSatisfy { $0 == 2 })
    }

    func testBitWidth3BitPacked() throws {
        // maxLevel = 7 requires bit-width = 3
        // Bit-packed: values [0, 1, 2, 3, 4, 5, 6, 7]
        // Header: (1 << 1) | 1 = 3 (1 group, bit-packed)
        // Packed: 8 values * 3 bits = 24 bits = 3 bytes
        //
        // LSB-first bit packing for [0,1,2,3,4,5,6,7] with bit-width 3:
        // Value 0 = 0b000 (bits at positions 0-2)
        // Value 1 = 0b001 (bits at positions 3-5)
        // Value 2 = 0b010 (bits at positions 6-8)
        // Value 3 = 0b011 (bits at positions 9-11)
        // Value 4 = 0b100 (bits at positions 12-14)
        // Value 5 = 0b101 (bits at positions 15-17)
        // Value 6 = 0b110 (bits at positions 18-20)
        // Value 7 = 0b111 (bits at positions 21-23)
        //
        // Pack into bytes (position N goes to byte N/8, bit N%8):
        // Byte 0 (positions 0-7):
        //   bit 0 (v0 bit 0) = 0
        //   bit 1 (v0 bit 1) = 0
        //   bit 2 (v0 bit 2) = 0
        //   bit 3 (v1 bit 0) = 1
        //   bit 4 (v1 bit 1) = 0
        //   bit 5 (v1 bit 2) = 0
        //   bit 6 (v2 bit 0) = 0
        //   bit 7 (v2 bit 1) = 1
        //   = 0b10001000 = 0x88
        // Byte 1 (positions 8-15):
        //   bit 0 (v2 bit 2) = 0
        //   bit 1 (v3 bit 0) = 1
        //   bit 2 (v3 bit 1) = 1
        //   bit 3 (v3 bit 2) = 0
        //   bit 4 (v4 bit 0) = 0
        //   bit 5 (v4 bit 1) = 0
        //   bit 6 (v4 bit 2) = 1
        //   bit 7 (v5 bit 0) = 1
        //   = 0b11000110 = 0xC6
        // Byte 2 (positions 16-23):
        //   bit 0 (v5 bit 1) = 0
        //   bit 1 (v5 bit 2) = 1
        //   bit 2 (v6 bit 0) = 0
        //   bit 3 (v6 bit 1) = 1
        //   bit 4 (v6 bit 2) = 1
        //   bit 5 (v7 bit 0) = 1
        //   bit 6 (v7 bit 1) = 1
        //   bit 7 (v7 bit 2) = 1
        //   = 0b11111010 = 0xFA

        var data = Data()
        data.append(contentsOf: [4, 0, 0, 0])  // length = 4 bytes
        data.append(0x03)  // varint: 3 (1 group, bit-packed)
        data.append(contentsOf: [0x88, 0xC6, 0xFA])

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 8, maxLevel: 7)

        XCTAssertEqual(levels.count, 8)
        XCTAssertEqual(levels, [0, 1, 2, 3, 4, 5, 6, 7])
    }

    // MARK: - Higher Bit-Width Tests (> 8 bits)

    // These tests verify that the decoder correctly handles bit-widths > 8,
    // which occurs when maxLevel > 255 (requiring more than 8 optional ancestors
    // in the schema). The RLE tests confirm multi-byte value reading works correctly.

    func testBitWidth9RLE() throws {
        // maxLevel = 256 requires bit-width = 9
        // RLE run: 5 values of 256
        // Header: (5 << 1) | 0 = 10 = 0x0A
        // Value: 256 (0x0100) = 2 bytes, little-endian

        var data = Data()
        data.append(contentsOf: [3, 0, 0, 0])  // length = 3 bytes
        data.append(0x0A)  // varint: 10 (5 values, RLE)
        data.append(contentsOf: [0x00, 0x01])  // value: 256 (little-endian)

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 5, maxLevel: 256)

        XCTAssertEqual(levels.count, 5)
        // Correctly returns 256 (not truncated)
        XCTAssertTrue(levels.allSatisfy { $0 == 256 })
    }

    func testBitWidth12RLE() throws {
        // maxLevel = 2047 requires bit-width = 12
        // RLE run: 3 values of 2047
        // Header: (3 << 1) | 0 = 6
        // Value: 2047 (0x07FF) = 2 bytes, little-endian

        var data = Data()
        data.append(contentsOf: [3, 0, 0, 0])  // length = 3 bytes
        data.append(0x06)  // varint: 6 (3 values, RLE)
        data.append(contentsOf: [0xFF, 0x07])  // value: 2047 (little-endian)

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 3, maxLevel: 2047)

        XCTAssertEqual(levels.count, 3)
        // Correctly returns 2047 (not truncated)
        XCTAssertTrue(levels.allSatisfy { $0 == 2047 })
    }

    func testBitWidth16RLE() throws {
        // maxLevel = 32767 requires bit-width = 16
        // RLE run: 3 values of 0x1234 (4660)
        // This verifies that 16-bit values > 255 are read correctly from RLE runs

        var data = Data()
        data.append(contentsOf: [3, 0, 0, 0])  // length = 3 bytes
        data.append(0x06)  // varint: 6 (3 values, RLE)
        data.append(contentsOf: [0x34, 0x12])  // value: 0x1234 = 4660 (little-endian)

        let decoder = LevelDecoder()
        let levels = try decoder.decodeLevels(from: data, numValues: 3, maxLevel: 32767)

        XCTAssertEqual(levels.count, 3)
        // Correctly returns 4660 (exercises multi-byte path with value > 255)
        XCTAssertTrue(levels.allSatisfy { $0 == 4660 })
    }

    // MARK: - Error Cases

    func testMissingLengthPrefix() {
        let data = Data([0, 1, 2])  // Only 3 bytes

        let decoder = LevelDecoder()
        XCTAssertThrowsError(try decoder.decodeLevels(from: data, numValues: 1, maxLevel: 1)) { error in
            XCTAssertEqual(error as? LevelError, .missingLengthPrefix)
        }
    }

    func testInvalidSize() {
        var data = Data()
        data.append(contentsOf: [10, 0, 0, 0])  // length = 10
        data.append(contentsOf: [0x00, 0x00])  // Only 2 bytes (should be 10)

        let decoder = LevelDecoder()
        XCTAssertThrowsError(try decoder.decodeLevels(from: data, numValues: 1, maxLevel: 1)) { error in
            guard case .invalidSize(let expected, let got) = error as? LevelError else {
                XCTFail("Expected invalidSize error")
                return
            }
            XCTAssertEqual(expected, 14)  // 4 + 10
            XCTAssertEqual(got, 6)  // 4 + 2
        }
    }

    func testTruncatedRuns() {
        var data = Data()
        data.append(contentsOf: [1, 0, 0, 0])  // length = 1 (only the varint header)
        data.append(0x14)  // varint: 20 (10 values, RLE)
        // Missing the repeated value byte! Size is correct (4 + 1 = 5), but run data is incomplete

        let decoder = LevelDecoder()
        XCTAssertThrowsError(try decoder.decodeLevels(from: data, numValues: 10, maxLevel: 1)) { error in
            XCTAssertEqual(error as? LevelError, .truncatedRuns)
        }
    }
}
