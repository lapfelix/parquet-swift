// Tests for ThriftReader - Compact Binary Protocol
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class ThriftReaderTests: XCTestCase {
    // MARK: - Varint Reading Tests

    func testReadVarintZero() throws {
        let data = Data([0x00])
        let reader = ThriftReader(data: data)
        let value = try reader.readVarint()
        XCTAssertEqual(value, 0)
    }

    func testReadVarintPositive() throws {
        // 150 encoded as zigzag varint: 150 * 2 = 300 = 0xAC 0x02
        let data = Data([0xAC, 0x02])
        let reader = ThriftReader(data: data)
        let value = try reader.readVarint()
        XCTAssertEqual(value, 150)
    }

    func testReadVarintNegative() throws {
        // -150 encoded as zigzag: (150 * 2) - 1 = 299 = 0xAB 0x02
        let data = Data([0xAB, 0x02])
        let reader = ThriftReader(data: data)
        let value = try reader.readVarint()
        XCTAssertEqual(value, -150)
    }

    func testReadVarint32() throws {
        let data = Data([0x0A]) // 5 zigzag encoded (5 * 2 = 10 = 0x0A)
        let reader = ThriftReader(data: data)
        let value = try reader.readVarint32()
        XCTAssertEqual(value, 5)
    }

    func testReadVarint16() throws {
        let data = Data([0x64]) // 50 zigzag encoded (50 * 2 = 100 = 0x64)
        let reader = ThriftReader(data: data)
        let value = try reader.readVarint16()
        XCTAssertEqual(value, 50)
    }

    func testReadI8() throws {
        let data = Data([0xFF])
        let reader = ThriftReader(data: data)
        let value = try reader.readI8()
        XCTAssertEqual(value, -1)
    }

    // MARK: - Basic Type Reading Tests

    func testReadByte() throws {
        let data = Data([0x42])
        let reader = ThriftReader(data: data)
        let value = try reader.readByte()
        XCTAssertEqual(value, 0x42)
    }

    func testReadDouble() throws {
        // 3.14159 in little-endian double
        let data = Data([0x6E, 0x86, 0x1B, 0xF0, 0xF9, 0x21, 0x09, 0x40])
        let reader = ThriftReader(data: data)
        let value = try reader.readDouble()
        XCTAssertEqual(value, 3.14159, accuracy: 0.00001)
    }

    func testReadString() throws {
        // Length (5) as unsigned varint + "Hello"
        let data = Data([0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F]) // 0x05 = 5 (unsigned)
        let reader = ThriftReader(data: data)
        let value = try reader.readString()
        XCTAssertEqual(value, "Hello")
    }

    func testReadBinary() throws {
        // Length (3) as unsigned varint + bytes [1, 2, 3]
        let data = Data([0x03, 0x01, 0x02, 0x03]) // 0x03 = 3 (unsigned)
        let reader = ThriftReader(data: data)
        let value = try reader.readBinary()
        XCTAssertEqual(value, Data([0x01, 0x02, 0x03]))
    }

    // MARK: - Field Header Reading Tests

    func testReadFieldHeaderSimple() throws {
        // Field type=i32 (0x05), delta=1
        let data = Data([0x15, 0x00]) // 0x15 = (1 << 4) | 0x05, 0x00 = STOP
        let reader = ThriftReader(data: data)
        var lastFieldId: Int16 = 0

        let field = try reader.readFieldHeader(lastFieldId: &lastFieldId)
        XCTAssertNotNil(field)
        XCTAssertEqual(field?.fieldId, 1)
        XCTAssertEqual(field?.type, .i32)
        XCTAssertEqual(lastFieldId, 1)
    }

    func testReadFieldHeaderStop() throws {
        let data = Data([0x00]) // STOP field
        let reader = ThriftReader(data: data)
        var lastFieldId: Int16 = 0

        let field = try reader.readFieldHeader(lastFieldId: &lastFieldId)
        XCTAssertNil(field)
    }

    func testReadFieldHeaderDelta() throws {
        // Two consecutive fields with delta encoding
        // Field 1: type=i32, delta=1
        // Field 2: type=binary, delta=1
        let data = Data([0x15, 0x18, 0x00]) // Field 1, Field 2, STOP
        let reader = ThriftReader(data: data)
        var lastFieldId: Int16 = 0

        let field1 = try reader.readFieldHeader(lastFieldId: &lastFieldId)
        XCTAssertEqual(field1?.fieldId, 1)
        XCTAssertEqual(field1?.type, .i32)

        let field2 = try reader.readFieldHeader(lastFieldId: &lastFieldId)
        XCTAssertEqual(field2?.fieldId, 2)
        XCTAssertEqual(field2?.type, .binary)
    }

    func testReadFieldHeaderBooleanTrue() throws {
        // Field type=boolTrue (0x01), delta=1
        let data = Data([0x11, 0x00]) // 0x11 = (1 << 4) | 0x01
        let reader = ThriftReader(data: data)
        var lastFieldId: Int16 = 0

        let field = try reader.readFieldHeader(lastFieldId: &lastFieldId)
        XCTAssertEqual(field?.fieldId, 1)
        XCTAssertEqual(field?.type, .boolTrue)
    }

    func testReadFieldHeaderBooleanFalse() throws {
        // Field type=boolFalse (0x02), delta=1
        let data = Data([0x12, 0x00]) // 0x12 = (1 << 4) | 0x02
        let reader = ThriftReader(data: data)
        var lastFieldId: Int16 = 0

        let field = try reader.readFieldHeader(lastFieldId: &lastFieldId)
        XCTAssertEqual(field?.fieldId, 1)
        XCTAssertEqual(field?.type, .boolFalse)
    }

    // MARK: - Collection Reading Tests

    func testReadListHeader() throws {
        // List with 3 i32 elements: size=3 in upper bits, type=i32 (0x05) in lower bits
        let data = Data([0x35]) // (3 << 4) | 0x05
        let reader = ThriftReader(data: data)

        let (elementType, count) = try reader.readListHeader()
        XCTAssertEqual(elementType, .i32)
        XCTAssertEqual(count, 3)
    }

    func testReadListHeaderLarge() throws {
        // List with 16+ elements uses varint for size
        // 0xF5 = (15 << 4) | 0x05, followed by unsigned varint size
        let data = Data([0xF5, 0x10]) // 0xF5 signals varint, 0x10 = 16 (unsigned)
        let reader = ThriftReader(data: data)

        let (elementType, count) = try reader.readListHeader()
        XCTAssertEqual(elementType, .i32)
        XCTAssertEqual(count, 16)
    }

    // MARK: - Error Handling Tests

    func testUnexpectedEndOfData() {
        let data = Data([]) // Empty data
        let reader = ThriftReader(data: data)

        XCTAssertThrowsError(try reader.readByte()) { error in
            guard case ThriftError.unexpectedEndOfData = error else {
                XCTFail("Expected unexpectedEndOfData error")
                return
            }
        }
    }

    func testVarintTooLong() {
        // 10 continuation bytes (invalid varint)
        let data = Data(repeating: 0xFF, count: 10)
        let reader = ThriftReader(data: data)

        XCTAssertThrowsError(try reader.readVarint()) { error in
            guard case ThriftError.protocolError(let msg) = error else {
                XCTFail("Expected protocolError")
                return
            }
            XCTAssertTrue(msg.contains("Varint too long"))
        }
    }

    func testInvalidUTF8String() {
        // Invalid UTF-8 sequence
        let data = Data([0x02, 0xFF, 0xFF]) // length=2 (unsigned), invalid UTF-8
        let reader = ThriftReader(data: data)

        XCTAssertThrowsError(try reader.readString()) { error in
            guard case ThriftError.invalidData(let msg) = error else {
                XCTFail("Expected invalidData error, got \(error)")
                return
            }
            XCTAssertTrue(msg.contains("UTF-8"))
        }
    }

    // MARK: - Integration Tests

    func testReadKeyValue() throws {
        // Simple KeyValue struct: {key: "name", value: "test"}
        // Struct start
        // Field 1 (key): type=binary, delta=1, value="name"
        // Field 2 (value): type=binary, delta=1, value="test"
        // STOP
        let data = Data([
            0x18,  // Field 1: (1 << 4) | 0x08 (binary)
            0x04, 0x6E, 0x61, 0x6D, 0x65,  // length=4 (unsigned), "name"
            0x18,  // Field 2: (1 << 4) | 0x08 (binary)
            0x04, 0x74, 0x65, 0x73, 0x74,  // length=4 (unsigned), "test"
            0x00   // STOP
        ])

        let reader = ThriftReader(data: data)
        let kv = try reader.readKeyValue()

        XCTAssertEqual(kv.key, "name")
        XCTAssertEqual(kv.value, "test")
    }

    func testSkipStruct() throws {
        // Test skipping an unknown struct
        // Struct with 2 i32 fields
        let data = Data([
            0x15, 0x0A,  // Field 1: i32, value=5
            0x15, 0x14,  // Field 2: i32, value=10
            0x00,        // STOP
            0x42         // Extra byte after struct
        ])

        let reader = ThriftReader(data: data)
        try reader.skipStruct()

        // Should have skipped the struct and stopped at the extra byte
        XCTAssertEqual(reader.currentPosition, 5)
        let nextByte = try reader.readByte()
        XCTAssertEqual(nextByte, 0x42)
    }

    func testBytesRemaining() {
        let data = Data([0x01, 0x02, 0x03, 0x04, 0x05])
        let reader = ThriftReader(data: data)

        XCTAssertEqual(reader.bytesRemaining, 5)
        _ = try? reader.readByte()
        XCTAssertEqual(reader.bytesRemaining, 4)
        _ = try? reader.readByte()
        XCTAssertEqual(reader.bytesRemaining, 3)
    }
}
