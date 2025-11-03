// RLEDecoderTests - Tests for RLE/Bit-Packing Hybrid decoder
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class RLEDecoderTests: XCTestCase {

    var decoder: RLEDecoder!

    override func setUp() {
        super.setUp()
        decoder = RLEDecoder()
    }

    // MARK: - Format Validation Tests

    func testMissingBitWidth() throws {
        let data = Data() // Empty
        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            XCTAssertEqual(error as? RLEError, .missingBitWidth)
        }
    }

    func testMissingLengthPrefix() throws {
        let data = Data([0x04]) // Only bit-width, no length
        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            XCTAssertEqual(error as? RLEError, .missingLengthPrefix)
        }
    }

    func testInvalidBitWidth() throws {
        var data = Data([0x21]) // bit-width: 33 (invalid, max is 32)
        data.append(contentsOf: [0x00, 0x00, 0x00, 0x00]) // length: 0

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 0)) { error in
            XCTAssertEqual(error as? RLEError, .invalidBitWidth(33))
        }
    }

    func testTruncatedData() throws {
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x0A, 0x00, 0x00, 0x00]) // length: 10
        data.append(Data(repeating: 0, count: 5)) // Only 5 bytes (need 10!)

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            guard case RLEError.truncatedData(expected: 15, got: 10) = error else {
                XCTFail("Expected truncatedData error, got \(error)")
                return
            }
        }
    }

    func testExtraneousData() throws {
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x02, 0x00, 0x00, 0x00]) // length: 2
        data.append(Data(repeating: 0, count: 2)) // 2 bytes of runs
        data.append(contentsOf: [0xFF, 0xFF]) // EXTRA BYTES (corruption!)

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 1)) { error in
            guard case RLEError.extraneousData(expected: 7, got: 9) = error else {
                XCTFail("Expected extraneousData error, got \(error)")
                return
            }
        }
    }

    // MARK: - Zero Bit-Width Tests

    func testZeroBitWidthEmptyPayload() throws {
        var data = Data([0x00]) // bit-width: 0
        data.append(contentsOf: [0x00, 0x00, 0x00, 0x00]) // length: 0 (no runs)

        let indices = try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 100)
        XCTAssertEqual(indices.count, 100)
        XCTAssertEqual(indices, Array(repeating: 0, count: 100))
    }

    func testZeroBitWidthWithRLERun() throws {
        var data = Data([0x00]) // bit-width: 0
        data.append(contentsOf: [0x01, 0x00, 0x00, 0x00]) // length: 1
        data.append(contentsOf: [0xC8, 0x01]) // RLE: (100 << 1) = 200 = 0xC8 0x01 (2 bytes!)

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 100)) { error in
            // varint 0xC8 0x01 is 2 bytes, but we declared length: 1
            // This triggers extraneousData (buffer is 7 bytes, expected 6)
            guard case RLEError.extraneousData(expected: 6, got: 7) = error else {
                XCTFail("Expected extraneousData error, got \(error)")
                return
            }
        }
    }

    func testZeroBitWidthCorrectLength() throws {
        var data = Data([0x00]) // bit-width: 0
        data.append(contentsOf: [0x02, 0x00, 0x00, 0x00]) // length: 2
        data.append(contentsOf: [0xC8, 0x01]) // RLE: 100 values (varint = 2 bytes)

        let indices = try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 100)
        XCTAssertEqual(indices.count, 100)
        XCTAssertEqual(indices, Array(repeating: 0, count: 100))
    }

    // MARK: - RLE Run Tests

    func testSimpleRLERun() throws {
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x02, 0x00, 0x00, 0x00]) // length: 2
        // RLE run: 10 repetitions of value 5
        // Header: (10 << 1) = 20 = 0x14 (varint: 1 byte)
        // Value: 5 (1 byte for bit-width 4)
        data.append(contentsOf: [0x14, 0x05])

        let indices = try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)
        XCTAssertEqual(indices, Array(repeating: 5, count: 10))
    }

    func testMultipleRLERuns() throws {
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x04, 0x00, 0x00, 0x00]) // length: 4
        // Run 1: 5 repetitions of value 3
        data.append(contentsOf: [0x0A, 0x03]) // (5 << 1) = 10, value=3
        // Run 2: 5 repetitions of value 7
        data.append(contentsOf: [0x0A, 0x07]) // (5 << 1) = 10, value=7

        let indices = try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)
        XCTAssertEqual(indices, [3, 3, 3, 3, 3, 7, 7, 7, 7, 7])
    }

    // MARK: - Bit-Packed Run Tests

    func testSimpleBitPackedRun() throws {
        var data = Data([0x03]) // bit-width: 3

        // Bit-packed run: 1 group of 8 values
        // Header: (1 << 1) | 1 = 3 = 1 byte
        // Data: 8 values × 3 bits = 24 bits = 3 bytes
        // Total run data: 1 + 3 = 4 bytes
        data.append(contentsOf: [0x04, 0x00, 0x00, 0x00]) // length: 4

        data.append(contentsOf: [0x03]) // Header

        // 8 values (0-7) packed in 3 bits each = 24 bits = 3 bytes
        let packed = packBits3([0, 1, 2, 3, 4, 5, 6, 7])
        data.append(contentsOf: packed)

        let indices = try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 8)
        XCTAssertEqual(indices, [0, 1, 2, 3, 4, 5, 6, 7])
    }

    func testBitPackedRunWithPadding() throws {
        // Decode only 10 values from a bit-packed run of 16 (2 groups)
        var data = Data([0x04]) // bit-width: 4

        // Bit-packed run: 2 groups of 8 = 16 values
        // Header: (2 << 1) | 1 = 5 = 1 byte
        // Data: 16 values × 4 bits = 64 bits = 8 bytes
        // Total run data: 1 + 8 = 9 bytes
        data.append(contentsOf: [0x09, 0x00, 0x00, 0x00]) // length: 9

        data.append(contentsOf: [0x05]) // Header

        // 16 values with 4 bits each = 64 bits = 8 bytes
        let packed = packBits4([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
        data.append(contentsOf: packed) // All 8 bytes

        // Only request 10 values (should trim padding)
        let indices = try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)
        XCTAssertEqual(indices.count, 10)
        XCTAssertEqual(indices, [0,1,2,3,4,5,6,7,8,9])
    }

    // MARK: - Overflow Protection Tests

    func testOversizedBitPackedRunBytesDetected() throws {
        var data = Data([0x20]) // bit-width: 32 (max, gives bytesPerGroup = 32)

        // With bitWidth = 32, bytesPerGroup = 32
        // The check is groupCount <= Int.max / 32
        // Use groupCount that passes Int.max / 8 check but fails Int.max / 32 check
        let oversizedGroupCount = UInt64(Int.max / 32) + 1000
        let header = (oversizedGroupCount << 1) | 1 // Bit-packed (odd)

        // Encode as varint
        var varint = Data()
        var remaining = header
        repeat {
            var byte = UInt8(remaining & 0x7F)
            remaining >>= 7
            if remaining > 0 {
                byte |= 0x80
            }
            varint.append(byte)
        } while remaining > 0

        // Set length to match varint size
        data.append(contentsOf: [UInt8(varint.count), 0x00, 0x00, 0x00])
        data.append(varint)

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            guard case RLEError.invalidRunHeader(let msg) = error else {
                XCTFail("Expected invalidRunHeader error, got \(error)")
                return
            }
            XCTAssertTrue(msg.contains("too many bytes"), "Expected 'too many bytes' in message, got: '\(msg)'")
        }
    }

    func testOversizedBitPackedRunDetected() throws {
        var data = Data([0x04]) // bit-width: 4

        // The check is groupCount <= Int.max / 8
        // Use groupCount that exceeds Int.max / 8
        let oversizedGroupCount = UInt64(Int.max / 8) + 1000
        let header = (oversizedGroupCount << 1) | 1 // Bit-packed (odd)

        // Encode as varint
        var varint = Data()
        var remaining = header
        repeat {
            var byte = UInt8(remaining & 0x7F)
            remaining >>= 7
            if remaining > 0 {
                byte |= 0x80
            }
            varint.append(byte)
        } while remaining > 0

        // Set length to match varint size
        data.append(contentsOf: [UInt8(varint.count), 0x00, 0x00, 0x00])
        data.append(varint)

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            guard case RLEError.invalidRunHeader(let msg) = error else {
                XCTFail("Expected invalidRunHeader error, got \(error)")
                return
            }
            XCTAssertTrue(msg.contains("too large"), "Expected 'too large' in message, got: '\(msg)'")
        }
    }

    func testOversizedRLERunDetected() throws {
        var data = Data([0x04]) // bit-width: 4

        // For RLE runs (even headers), the maximum rawRunLength = (UInt64.max - 1) >> 1 = Int.max
        // So the overflow check "rawRunLength > Int.max" is theoretically unreachable
        // because of encoding constraints. However, we test the boundary case here.

        // To actually exceed Int.max, we'd need a varint larger than UInt64.max,
        // but the varint reader caps at UInt64. So this test verifies that
        // extremely large run lengths don't cause issues.

        // Encode UInt64.max - 1 (maximum even value)
        let varint = Data([0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01])

        // Set length to match varint size (but no value byte, will fail elsewhere)
        data.append(contentsOf: [UInt8(varint.count), 0x00, 0x00, 0x00])
        data.append(varint)

        // This will fail (but not with "too large" - rather with truncated data)
        // because we declared 10 bytes but need an additional value byte
        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            XCTAssertTrue(error is RLEError, "Expected RLEError, got \(error)")
        }
    }

    func testOversizedZeroBitWidthRun() throws {
        var data = Data([0x00]) // bit-width: 0

        // For zero bit-width, the check is groupCount <= Int.max / 8
        // because we multiply by 8 (groups of 8 values)
        // Use groupCount that exceeds Int.max / 8
        let oversizedGroupCount = UInt64(Int.max / 8) + 1000
        let header = (oversizedGroupCount << 1) | 1 // Bit-packed (odd)

        // Encode as varint
        var varint = Data()
        var remaining = header
        repeat {
            var byte = UInt8(remaining & 0x7F)
            remaining >>= 7
            if remaining > 0 {
                byte |= 0x80
            }
            varint.append(byte)
        } while remaining > 0

        // Set length to match varint size
        data.append(contentsOf: [UInt8(varint.count), 0x00, 0x00, 0x00])
        data.append(varint)

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            guard case RLEError.invalidRunHeader(let msg) = error else {
                XCTFail("Expected invalidRunHeader error, got \(error)")
                return
            }
            XCTAssertTrue(msg.contains("too large"), "Expected 'too large' in message, got: '\(msg)'")
        }
    }

    // MARK: - Validation Tests

    func testUnconsumedDataDetected() throws {
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x05, 0x00, 0x00, 0x00]) // length: 5 bytes
        // RLE run: 10 values of 5 (2 bytes)
        data.append(contentsOf: [0x14, 0x05])
        // Extra bytes declared but not consumed (3 bytes of garbage)
        data.append(contentsOf: [0xFF, 0xFF, 0xFF])

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 10)) { error in
            guard case RLEError.unconsumedData(expectedBytes: 5, consumedBytes: 2) = error else {
                XCTFail("Expected unconsumedData error, got \(error)")
                return
            }
        }
    }

    func testTruncatedRunsDetected() throws {
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x01, 0x00, 0x00, 0x00]) // length: 1
        // RLE run header for 100 values, but value byte is missing!
        data.append(contentsOf: [0xC8, 0x01]) // (100 << 1) = 200
        // Missing: value byte

        XCTAssertThrowsError(try decoder.decodeIndicesWithLengthPrefix(from: data, numValues: 100)) { error in
            // Should fail because we can't read the value
            XCTAssertTrue(error is RLEError)
        }
    }

    // MARK: - Data Page Format Tests (No Length Prefix)

    func testDataPageFormatSimpleRLE() throws {
        // Data page format: <bit-width> <runs...>
        // No 4-byte length prefix!
        var data = Data([0x04]) // bit-width: 4
        data.append(contentsOf: [0x14, 0x00]) // RLE: (10 << 1) = 20, value: 0

        let indices = try decoder.decodeIndices(from: data, numValues: 10)
        XCTAssertEqual(indices.count, 10)
        XCTAssertTrue(indices.allSatisfy { $0 == 0 })
    }

    func testDataPageFormatSimpleBitPacked() throws {
        // Data page format: <bit-width> <runs...>
        var data = Data([0x03]) // bit-width: 3
        data.append(contentsOf: [0x03]) // Bit-packed: (1 << 1) | 1 = 3 (1 group = 8 values)

        // Pack 8 values with 3 bits each: [0,1,2,3,4,5,6,7]
        let packed = packBits3([0,1,2,3,4,5,6,7])
        data.append(packed)

        let indices = try decoder.decodeIndices(from: data, numValues: 8)
        XCTAssertEqual(indices, [0,1,2,3,4,5,6,7])
    }

    func testDataPageFormatMultipleRuns() throws {
        // Data page format with multiple runs
        var data = Data([0x04]) // bit-width: 4

        // Run 1: RLE - 5 values of 3
        data.append(contentsOf: [0x0A, 0x03]) // (5 << 1) = 10, value: 3

        // Run 2: RLE - 3 values of 7
        data.append(contentsOf: [0x06, 0x07]) // (3 << 1) = 6, value: 7

        let indices = try decoder.decodeIndices(from: data, numValues: 8)
        XCTAssertEqual(indices, [3,3,3,3,3,7,7,7])
    }

    func testDataPageFormatZeroBitWidth() throws {
        // Data page format with bit-width 0 (single-value dictionary)
        var data = Data([0x00]) // bit-width: 0
        data.append(contentsOf: [0x14]) // RLE: (10 << 1) = 20

        let indices = try decoder.decodeIndices(from: data, numValues: 10)
        XCTAssertEqual(indices.count, 10)
        XCTAssertTrue(indices.allSatisfy { $0 == 0 })
    }

    func testDataPageFormatActualDictionaryData() throws {
        // This mimics the actual data from alltypes_plain.parquet column 0
        // Bit-width 3, with bit-packed run
        var data = Data([0x03]) // bit-width: 3
        data.append(contentsOf: [0x03]) // Bit-packed: 1 group of 8 values
        data.append(contentsOf: [0x88, 0xC6, 0xFA]) // Packed data (from real file)

        let indices = try decoder.decodeIndices(from: data, numValues: 8)
        XCTAssertEqual(indices.count, 8)
        // Verify we get valid dictionary indices (all should be < 8 for bit-width 3)
        XCTAssertTrue(indices.allSatisfy { $0 < 8 })
    }

    // MARK: - Helper Methods

    /// Pack 8 values with 3 bits each into bytes (LSB-first)
    private func packBits3(_ values: [UInt32]) -> Data {
        precondition(values.count == 8)
        var data = Data()
        var bitBuffer: UInt64 = 0
        var bitsInBuffer = 0

        for value in values {
            bitBuffer |= UInt64(value & 0x07) << bitsInBuffer
            bitsInBuffer += 3

            while bitsInBuffer >= 8 {
                data.append(UInt8(bitBuffer & 0xFF))
                bitBuffer >>= 8
                bitsInBuffer -= 8
            }
        }

        // Flush remaining bits
        if bitsInBuffer > 0 {
            data.append(UInt8(bitBuffer & 0xFF))
        }

        return data
    }

    /// Pack values with 4 bits each into bytes (LSB-first)
    private func packBits4(_ values: [UInt32]) -> Data {
        var data = Data()
        var bitBuffer: UInt64 = 0
        var bitsInBuffer = 0

        for value in values {
            bitBuffer |= UInt64(value & 0x0F) << bitsInBuffer
            bitsInBuffer += 4

            while bitsInBuffer >= 8 {
                data.append(UInt8(bitBuffer & 0xFF))
                bitBuffer >>= 8
                bitsInBuffer -= 8
            }
        }

        // Flush remaining bits
        if bitsInBuffer > 0 {
            data.append(UInt8(bitBuffer & 0xFF))
        }

        return data
    }
}
