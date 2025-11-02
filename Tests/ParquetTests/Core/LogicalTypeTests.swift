// Tests for LogicalType
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class LogicalTypeTests: XCTestCase {
    // MARK: - Primitive Types

    func testStringType() {
        let logicalType = LogicalType.string
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.byteArray])
        XCTAssertEqual(logicalType.name, "STRING")
    }

    func testEnumType() {
        let logicalType = LogicalType.enum
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.byteArray])
        XCTAssertEqual(logicalType.name, "ENUM")
    }

    func testUUIDType() {
        let logicalType = LogicalType.uuid
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.fixedLenByteArray(length: 16)])
        XCTAssertEqual(logicalType.name, "UUID")
    }

    // MARK: - Temporal Types

    func testDateType() {
        let logicalType = LogicalType.date
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int32])
        XCTAssertEqual(logicalType.name, "DATE")
    }

    func testTimeTypeMillis() {
        let logicalType = LogicalType.time(isAdjustedToUTC: true, unit: .milliseconds)
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int32])
        XCTAssertEqual(logicalType.name, "TIME(isAdjustedToUTC=true, unit=MILLIS)")
    }

    func testTimeTypeMicros() {
        let logicalType = LogicalType.time(isAdjustedToUTC: false, unit: .microseconds)
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int64])
        XCTAssertEqual(logicalType.name, "TIME(isAdjustedToUTC=false, unit=MICROS)")
    }

    func testTimestampType() {
        let logicalType = LogicalType.timestamp(isAdjustedToUTC: true, unit: .microseconds)
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int64])
        XCTAssertEqual(logicalType.name, "TIMESTAMP(isAdjustedToUTC=true, unit=MICROS)")
    }

    // MARK: - Numeric Types

    func testIntegerType8Bit() {
        let logicalType = LogicalType.integer(bitWidth: 8, isSigned: true)
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int32])
        XCTAssertEqual(logicalType.name, "INT(8, signed)")
    }

    func testIntegerType16Bit() {
        let logicalType = LogicalType.integer(bitWidth: 16, isSigned: false)
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int32])
        XCTAssertEqual(logicalType.name, "INT(16, unsigned)")
    }

    func testIntegerType64Bit() {
        let logicalType = LogicalType.integer(bitWidth: 64, isSigned: true)
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.int64])
        XCTAssertEqual(logicalType.name, "INT(64, signed)")
    }

    func testDecimalType() {
        let logicalType = LogicalType.decimal(precision: 10, scale: 2)
        let compatibleTypes = logicalType.compatiblePhysicalTypes
        XCTAssertTrue(compatibleTypes.contains(.int32))
        XCTAssertTrue(compatibleTypes.contains(.int64))
        XCTAssertTrue(compatibleTypes.contains(.byteArray))
        XCTAssertEqual(logicalType.name, "DECIMAL(precision=10, scale=2)")
    }

    // MARK: - Complex Types

    func testJSONType() {
        let logicalType = LogicalType.json
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.byteArray])
        XCTAssertEqual(logicalType.name, "JSON")
    }

    func testBSONType() {
        let logicalType = LogicalType.bson
        XCTAssertEqual(logicalType.compatiblePhysicalTypes, [.byteArray])
        XCTAssertEqual(logicalType.name, "BSON")
    }

    func testListType() {
        let logicalType = LogicalType.list
        XCTAssertTrue(logicalType.compatiblePhysicalTypes.isEmpty, "LIST works with nested structures")
        XCTAssertEqual(logicalType.name, "LIST")
    }

    func testMapType() {
        let logicalType = LogicalType.map
        XCTAssertTrue(logicalType.compatiblePhysicalTypes.isEmpty, "MAP works with nested structures")
        XCTAssertEqual(logicalType.name, "MAP")
    }

    // MARK: - Time Unit

    func testTimeUnitConversions() {
        XCTAssertEqual(TimeUnit.milliseconds.toSeconds, 1e-3, accuracy: 1e-10)
        XCTAssertEqual(TimeUnit.microseconds.toSeconds, 1e-6, accuracy: 1e-10)
        XCTAssertEqual(TimeUnit.nanoseconds.toSeconds, 1e-9, accuracy: 1e-10)
    }

    func testTimeUnitRawValues() {
        XCTAssertEqual(TimeUnit.milliseconds.rawValue, "MILLIS")
        XCTAssertEqual(TimeUnit.microseconds.rawValue, "MICROS")
        XCTAssertEqual(TimeUnit.nanoseconds.rawValue, "NANOS")
    }

    // MARK: - Equality

    func testEquality() {
        let string1 = LogicalType.string
        let string2 = LogicalType.string
        XCTAssertEqual(string1, string2)

        let time1 = LogicalType.time(isAdjustedToUTC: true, unit: .microseconds)
        let time2 = LogicalType.time(isAdjustedToUTC: true, unit: .microseconds)
        let time3 = LogicalType.time(isAdjustedToUTC: false, unit: .microseconds)
        XCTAssertEqual(time1, time2)
        XCTAssertNotEqual(time1, time3)

        let decimal1 = LogicalType.decimal(precision: 10, scale: 2)
        let decimal2 = LogicalType.decimal(precision: 10, scale: 2)
        let decimal3 = LogicalType.decimal(precision: 10, scale: 3)
        XCTAssertEqual(decimal1, decimal2)
        XCTAssertNotEqual(decimal1, decimal3)
    }

    // MARK: - Description

    func testDescription() {
        XCTAssertEqual(String(describing: LogicalType.string), "STRING")
        XCTAssertEqual(String(describing: LogicalType.time(isAdjustedToUTC: true, unit: .milliseconds)),
                       "TIME(isAdjustedToUTC=true, unit=MILLIS)")
        XCTAssertEqual(String(describing: LogicalType.decimal(precision: 18, scale: 6)),
                       "DECIMAL(precision=18, scale=6)")
    }
}
