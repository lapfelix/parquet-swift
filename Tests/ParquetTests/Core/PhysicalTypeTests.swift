// Tests for PhysicalType
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class PhysicalTypeTests: XCTestCase {
    // MARK: - Fixed-Size Types

    func testBooleanType() {
        let type = PhysicalType.boolean
        XCTAssertNil(type.byteSize, "Boolean is bit-packed, not byte-aligned")
        XCTAssertFalse(type.isFixedSize)
        XCTAssertTrue(type.isVariableLength)
        XCTAssertEqual(type.name, "BOOLEAN")
    }

    func testInt32Type() {
        let type = PhysicalType.int32
        XCTAssertEqual(type.byteSize, 4)
        XCTAssertTrue(type.isFixedSize)
        XCTAssertFalse(type.isVariableLength)
        XCTAssertEqual(type.name, "INT32")
    }

    func testInt64Type() {
        let type = PhysicalType.int64
        XCTAssertEqual(type.byteSize, 8)
        XCTAssertTrue(type.isFixedSize)
        XCTAssertEqual(type.name, "INT64")
    }

    func testInt96Type() {
        let type = PhysicalType.int96
        XCTAssertEqual(type.byteSize, 12)
        XCTAssertTrue(type.isFixedSize)
        XCTAssertEqual(type.name, "INT96")
    }

    func testFloatType() {
        let type = PhysicalType.float
        XCTAssertEqual(type.byteSize, 4)
        XCTAssertTrue(type.isFixedSize)
        XCTAssertEqual(type.name, "FLOAT")
    }

    func testDoubleType() {
        let type = PhysicalType.double
        XCTAssertEqual(type.byteSize, 8)
        XCTAssertTrue(type.isFixedSize)
        XCTAssertEqual(type.name, "DOUBLE")
    }

    // MARK: - Variable-Length Types

    func testByteArrayType() {
        let type = PhysicalType.byteArray
        XCTAssertNil(type.byteSize)
        XCTAssertFalse(type.isFixedSize)
        XCTAssertTrue(type.isVariableLength)
        XCTAssertEqual(type.name, "BYTE_ARRAY")
    }

    func testFixedLenByteArrayType() {
        let type = PhysicalType.fixedLenByteArray(length: 16)
        XCTAssertEqual(type.byteSize, 16)
        XCTAssertTrue(type.isFixedSize)
        XCTAssertFalse(type.isVariableLength)
        XCTAssertEqual(type.name, "FIXED_LEN_BYTE_ARRAY(16)")
    }

    // MARK: - Equality

    func testEquality() {
        XCTAssertEqual(PhysicalType.int32, PhysicalType.int32)
        XCTAssertNotEqual(PhysicalType.int32, PhysicalType.int64)

        let fixedLen1 = PhysicalType.fixedLenByteArray(length: 16)
        let fixedLen2 = PhysicalType.fixedLenByteArray(length: 16)
        let fixedLen3 = PhysicalType.fixedLenByteArray(length: 32)

        XCTAssertEqual(fixedLen1, fixedLen2)
        XCTAssertNotEqual(fixedLen1, fixedLen3)
    }

    // MARK: - Description

    func testDescription() {
        XCTAssertEqual(String(describing: PhysicalType.int32), "INT32")
        XCTAssertEqual(String(describing: PhysicalType.byteArray), "BYTE_ARRAY")
        XCTAssertEqual(String(describing: PhysicalType.fixedLenByteArray(length: 10)), "FIXED_LEN_BYTE_ARRAY(10)")
    }
}
