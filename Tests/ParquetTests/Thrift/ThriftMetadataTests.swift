// Tests for Thrift metadata deserialization
//
// Licensed under the Apache License, Version 2.0
//
// NOTE: These tests use hand-crafted Thrift Compact Binary data.
// For comprehensive testing, real Parquet file footers should be used as fixtures.

import XCTest
@testable import Parquet

final class ThriftMetadataTests: XCTestCase {
    // MARK: - Basic Structure Tests

    func testReadKeyValueSimple() throws {
        // KeyValue with just a key (value is optional)
        // Field 1 (key): binary
        // No field 2 (value is nil)
        // STOP
        let data = Data([
            0x18,  // Field 1: (delta=1 << 4) | 0x08 (binary)
            0x04, 0x74, 0x65, 0x73, 0x74,  // length=4 (unsigned), "test"
            0x00   // STOP
        ])

        let reader = ThriftReader(data: data)
        let kv = try reader.readKeyValue()

        XCTAssertEqual(kv.key, "test")
        XCTAssertNil(kv.value)
    }

    func testReadKeyValueWithValue() throws {
        // KeyValue: {key: "k", value: "v"}
        let data = Data([
            0x18,  // Field 1: binary
            0x01, 0x6B,  // length=1 (unsigned), "k"
            0x18,  // Field 2: binary (delta=1)
            0x01, 0x76,  // length=1 (unsigned), "v"
            0x00   // STOP
        ])

        let reader = ThriftReader(data: data)
        let kv = try reader.readKeyValue()

        XCTAssertEqual(kv.key, "k")
        XCTAssertEqual(kv.value, "v")
    }

    // MARK: - Statistics Tests

    func testReadStatisticsEmpty() throws {
        // Empty statistics (all fields optional)
        let data = Data([0x00])  // Just STOP

        let reader = ThriftReader(data: data)
        let stats = try reader.readStatistics()

        XCTAssertNil(stats.nullCount)
        XCTAssertNil(stats.distinctCount)
        XCTAssertNil(stats.max)
        XCTAssertNil(stats.min)
    }

    func testReadStatisticsWithNullCount() throws {
        // Statistics with just null_count (field 3)
        let data = Data([
            0x36, 0x14,  // Field 3: i64, value=10 (20 zigzag = 0x14)
            0x00  // STOP
        ])

        let reader = ThriftReader(data: data)
        let stats = try reader.readStatistics()

        XCTAssertEqual(stats.nullCount, 10)
        XCTAssertNil(stats.distinctCount)
    }

    // MARK: - Type Enum Tests

    func testThriftTypeRawValues() {
        XCTAssertEqual(ThriftType.boolean.rawValue, 0)
        XCTAssertEqual(ThriftType.int32.rawValue, 1)
        XCTAssertEqual(ThriftType.int64.rawValue, 2)
        XCTAssertEqual(ThriftType.byteArray.rawValue, 6)
    }

    func testThriftEncodingRawValues() {
        XCTAssertEqual(ThriftEncoding.plain.rawValue, 0)
        XCTAssertEqual(ThriftEncoding.rle.rawValue, 3)
        XCTAssertEqual(ThriftEncoding.rleDictionary.rawValue, 8)
    }

    func testThriftCompressionCodecRawValues() {
        XCTAssertEqual(ThriftCompressionCodec.uncompressed.rawValue, 0)
        XCTAssertEqual(ThriftCompressionCodec.snappy.rawValue, 1)
        XCTAssertEqual(ThriftCompressionCodec.gzip.rawValue, 2)
    }

    func testThriftPageTypeRawValues() {
        XCTAssertEqual(ThriftPageType.dataPage.rawValue, 0)
        XCTAssertEqual(ThriftPageType.dictionaryPage.rawValue, 2)
        XCTAssertEqual(ThriftPageType.dataPageV2.rawValue, 3)
    }

    // MARK: - Error Cases

    func testReadKeyValueMissingKey() {
        // KeyValue without key (required field)
        let data = Data([0x00])  // Just STOP

        let reader = ThriftReader(data: data)
        XCTAssertThrowsError(try reader.readKeyValue()) { error in
            guard case ThriftError.invalidData(let msg) = error else {
                XCTFail("Expected invalidData error")
                return
            }
            XCTAssertTrue(msg.contains("key"))
        }
    }

    // MARK: - Struct Type Tests

    func testThriftDecimalType() {
        let decimal = ThriftDecimalType(scale: 2, precision: 10)
        XCTAssertEqual(decimal.scale, 2)
        XCTAssertEqual(decimal.precision, 10)
    }

    func testThriftTimestampType() {
        let ts = ThriftTimestampType(isAdjustedToUTC: true, unit: .micros)
        XCTAssertTrue(ts.isAdjustedToUTC)
        XCTAssertEqual(ts.unit, .micros)
    }

    func testThriftTimeType() {
        let time = ThriftTimeType(isAdjustedToUTC: false, unit: .millis)
        XCTAssertFalse(time.isAdjustedToUTC)
        XCTAssertEqual(time.unit, .millis)
    }

    func testThriftIntType() {
        let intType = ThriftIntType(bitWidth: 32, isSigned: true)
        XCTAssertEqual(intType.bitWidth, 32)
        XCTAssertTrue(intType.isSigned)
    }

    // MARK: - Logical Type Names

    func testLogicalTypeNames() {
        XCTAssertEqual(ThriftLogicalType.string.name, "STRING")
        XCTAssertEqual(ThriftLogicalType.json.name, "JSON")
        XCTAssertEqual(ThriftLogicalType.uuid.name, "UUID")

        let decimal = ThriftLogicalType.decimal(ThriftDecimalType(scale: 2, precision: 10))
        XCTAssertEqual(decimal.name, "DECIMAL(precision=10, scale=2)")
    }

    // MARK: - TimeUnit Tests

    func testTimeUnitNames() {
        XCTAssertEqual(ThriftTimeUnit.millis.name, "MILLIS")
        XCTAssertEqual(ThriftTimeUnit.micros.name, "MICROS")
        XCTAssertEqual(ThriftTimeUnit.nanos.name, "NANOS")
    }

    // MARK: - Integration Note

    /// NOTE: Full integration tests with complete FileMetaData, SchemaElement,
    /// RowGroup, etc. require real Parquet file footers as fixtures.
    ///
    /// The Thrift Compact Binary format is complex with:
    /// - Zigzag varint encoding
    /// - Delta field ID encoding
    /// - Boolean values encoded in field headers
    /// - Nested struct encoding
    ///
    /// Hand-crafting complete metadata structures is error-prone.
    /// Instead, use real Parquet files for end-to-end testing.
    func testIntegrationTestNote() {
        // This test documents the need for real fixtures
        XCTAssertTrue(true, "Real Parquet file footers needed for comprehensive testing")
    }
}
