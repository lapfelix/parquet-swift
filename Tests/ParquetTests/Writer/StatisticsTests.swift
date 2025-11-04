// StatisticsTests.swift - Tests for statistics accumulators
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class StatisticsTests: XCTestCase {

    // MARK: - Int32 Statistics Tests

    func testInt32RequiredStatistics() throws {
        let accumulator = Int32StatisticsAccumulator()

        // Type-safe, zero-copy update
        let values: [Int32] = [10, 5, 20, 15, 8]
        accumulator.update(values)

        let stats = accumulator.build()

        // Verify both modern and legacy fields populated
        XCTAssertNotNil(stats.minValue)
        XCTAssertNotNil(stats.maxValue)
        XCTAssertEqual(decodePlainInt32(stats.minValue!), 5)
        XCTAssertEqual(decodePlainInt32(stats.maxValue!), 20)

        // Verify legacy fields match modern fields
        XCTAssertEqual(stats.min, stats.minValue)
        XCTAssertEqual(stats.max, stats.maxValue)

        // No nulls in required column
        XCTAssertNil(stats.nullCount)
    }

    func testInt32NullableStatistics() throws {
        let accumulator = Int32StatisticsAccumulator()

        let values: [Int32?] = [10, nil, 20, nil, 5]
        accumulator.updateNullable(values)

        let stats = accumulator.build()

        XCTAssertEqual(decodePlainInt32(stats.minValue!), 5)
        XCTAssertEqual(decodePlainInt32(stats.maxValue!), 20)
        XCTAssertEqual(stats.nullCount, 2)

        // Verify legacy fields
        XCTAssertEqual(stats.min, stats.minValue)
        XCTAssertEqual(stats.max, stats.maxValue)
    }

    func testInt32AllNulls() throws {
        let accumulator = Int32StatisticsAccumulator()

        let values: [Int32?] = [nil, nil, nil]
        accumulator.updateNullable(values)

        let stats = accumulator.build()

        // No non-null values, so no min/max
        XCTAssertNil(stats.minValue)
        XCTAssertNil(stats.maxValue)
        XCTAssertNil(stats.min)
        XCTAssertNil(stats.max)
        XCTAssertEqual(stats.nullCount, 3)
    }

    func testInt32NegativeValues() throws {
        let accumulator = Int32StatisticsAccumulator()

        let values: [Int32] = [-10, -5, -20, 0, 5]
        accumulator.update(values)

        let stats = accumulator.build()

        XCTAssertEqual(decodePlainInt32(stats.minValue!), -20)
        XCTAssertEqual(decodePlainInt32(stats.maxValue!), 5)
    }

    // MARK: - Int64 Statistics Tests

    func testInt64RequiredStatistics() throws {
        let accumulator = Int64StatisticsAccumulator()

        let values: [Int64] = [1000, 500, 2000, 1500]
        accumulator.update(values)

        let stats = accumulator.build()

        XCTAssertEqual(decodePlainInt64(stats.minValue!), 500)
        XCTAssertEqual(decodePlainInt64(stats.maxValue!), 2000)
        XCTAssertNil(stats.nullCount)

        // Verify legacy fields
        XCTAssertEqual(stats.min, stats.minValue)
        XCTAssertEqual(stats.max, stats.maxValue)
    }

    func testInt64NullableStatistics() throws {
        let accumulator = Int64StatisticsAccumulator()

        let values: [Int64?] = [1000, nil, 2000, nil, 500]
        accumulator.updateNullable(values)

        let stats = accumulator.build()

        XCTAssertEqual(decodePlainInt64(stats.minValue!), 500)
        XCTAssertEqual(decodePlainInt64(stats.maxValue!), 2000)
        XCTAssertEqual(stats.nullCount, 2)
    }

    // MARK: - Float Statistics Tests (NaN Handling)

    func testFloatNaNExcludedFromMinMax() throws {
        let accumulator = FloatStatisticsAccumulator()

        let values: [Float] = [10.5, Float.nan, 20.3, 5.1, Float.nan]
        accumulator.update(values)

        let stats = accumulator.build()

        // NaN excluded from min/max
        XCTAssertEqual(decodePlainFloat(stats.minValue!), 5.1, accuracy: 0.001)
        XCTAssertEqual(decodePlainFloat(stats.maxValue!), 20.3, accuracy: 0.001)

        // NaN is NOT counted as NULL
        XCTAssertNil(stats.nullCount)

        // Verify legacy fields
        XCTAssertEqual(stats.min, stats.minValue)
        XCTAssertEqual(stats.max, stats.maxValue)
    }

    func testFloatAllNaN() throws {
        let accumulator = FloatStatisticsAccumulator()

        let values: [Float] = [Float.nan, Float.nan, Float.nan]
        accumulator.update(values)

        let stats = accumulator.build()

        // All NaN means no valid min/max
        XCTAssertNil(stats.minValue)
        XCTAssertNil(stats.maxValue)
        XCTAssertNil(stats.nullCount)  // NaN ≠ NULL
    }

    func testFloatInfinityIncluded() throws {
        let accumulator = FloatStatisticsAccumulator()

        let values: [Float] = [10.5, Float.infinity, 20.3, -Float.infinity, 5.1]
        accumulator.update(values)

        let stats = accumulator.build()

        // Infinity IS included in min/max
        XCTAssertEqual(decodePlainFloat(stats.minValue!), -Float.infinity)
        XCTAssertEqual(decodePlainFloat(stats.maxValue!), Float.infinity)
    }

    func testFloatNullableWithNaN() throws {
        let accumulator = FloatStatisticsAccumulator()

        let values: [Float?] = [10.5, nil, Float.nan, 20.3, nil, 5.1]
        accumulator.updateNullable(values)

        let stats = accumulator.build()

        // NaN excluded, nulls counted
        XCTAssertEqual(decodePlainFloat(stats.minValue!), 5.1, accuracy: 0.001)
        XCTAssertEqual(decodePlainFloat(stats.maxValue!), 20.3, accuracy: 0.001)
        XCTAssertEqual(stats.nullCount, 2)  // Only nil, not NaN
    }

    // MARK: - Double Statistics Tests (NaN Handling)

    func testDoubleNaNExcludedFromMinMax() throws {
        let accumulator = DoubleStatisticsAccumulator()

        let values: [Double] = [10.5, Double.nan, 20.3, 5.1]
        accumulator.update(values)

        let stats = accumulator.build()

        XCTAssertEqual(decodePlainDouble(stats.minValue!), 5.1, accuracy: 0.001)
        XCTAssertEqual(decodePlainDouble(stats.maxValue!), 20.3, accuracy: 0.001)
        XCTAssertNil(stats.nullCount)

        // Verify legacy fields
        XCTAssertEqual(stats.min, stats.minValue)
        XCTAssertEqual(stats.max, stats.maxValue)
    }

    func testDoubleInfinityIncluded() throws {
        let accumulator = DoubleStatisticsAccumulator()

        let values: [Double] = [10.5, Double.infinity, -Double.infinity]
        accumulator.update(values)

        let stats = accumulator.build()

        XCTAssertEqual(decodePlainDouble(stats.minValue!), -Double.infinity)
        XCTAssertEqual(decodePlainDouble(stats.maxValue!), Double.infinity)
    }

    // MARK: - String Statistics Tests (Byte-Wise Comparison)

    func testStringByteWiseComparison() throws {
        let accumulator = StringStatisticsAccumulator()

        let values = ["zebra", "apple", "banana", "aardvark"]
        accumulator.update(values)

        let stats = accumulator.build()

        // Byte-wise lexicographic ordering
        XCTAssertEqual(decodePlainByteArray(stats.minValue!), "aardvark")
        XCTAssertEqual(decodePlainByteArray(stats.maxValue!), "zebra")

        // Verify legacy fields
        XCTAssertEqual(stats.min, stats.minValue)
        XCTAssertEqual(stats.max, stats.maxValue)
    }

    func testStringNonASCIIByteWiseComparison() throws {
        let accumulator = StringStatisticsAccumulator()

        // Test with non-ASCII to verify byte-wise (not locale) comparison
        // UTF-8 byte values: 'a'=0x61, 'z'=0x7A, 'É'=0xC3 0x89, 'é'=0xC3 0xA9
        // Byte-wise ordering: 0x61 < 0x7A < 0xC3
        // So: "apple" < "zebra" < "ÉLÈVE" < "élève"
        let values = ["zebra", "élève", "apple", "ÉLÈVE"]
        accumulator.update(values)

        let stats = accumulator.build()

        let minStr = decodePlainByteArray(stats.minValue!)
        let maxStr = decodePlainByteArray(stats.maxValue!)

        // Verify byte-wise ordering (not locale-sensitive)
        XCTAssertEqual(minStr, "apple")   // 0x61... smallest
        XCTAssertEqual(maxStr, "élève")   // 0xC3 0xA9... largest
    }

    func testStringEmptyVsNull() throws {
        let accumulator = StringStatisticsAccumulator()

        let values: [String?] = ["", nil, "abc"]
        accumulator.updateNullable(values)

        let stats = accumulator.build()

        // Empty string is valid, different from NULL
        XCTAssertEqual(decodePlainByteArray(stats.minValue!), "")
        XCTAssertEqual(decodePlainByteArray(stats.maxValue!), "abc")
        XCTAssertEqual(stats.nullCount, 1)  // Only nil
    }

    func testStringAllNulls() throws {
        let accumulator = StringStatisticsAccumulator()

        let values: [String?] = [nil, nil, nil]
        accumulator.updateNullable(values)

        let stats = accumulator.build()

        XCTAssertNil(stats.minValue)
        XCTAssertNil(stats.maxValue)
        XCTAssertEqual(stats.nullCount, 3)
    }

    // MARK: - Helper Decoders

    private func decodePlainInt32(_ data: Data) -> Int32 {
        precondition(data.count == 4)
        var value: Int32 = 0
        value |= Int32(data[0])
        value |= Int32(data[1]) << 8
        value |= Int32(data[2]) << 16
        value |= Int32(data[3]) << 24
        return value
    }

    private func decodePlainInt64(_ data: Data) -> Int64 {
        precondition(data.count == 8)
        var value: Int64 = 0
        for i in 0..<8 {
            value |= Int64(data[i]) << (i * 8)
        }
        return value
    }

    private func decodePlainFloat(_ data: Data) -> Float {
        precondition(data.count == 4)
        var bits: UInt32 = 0
        bits |= UInt32(data[0])
        bits |= UInt32(data[1]) << 8
        bits |= UInt32(data[2]) << 16
        bits |= UInt32(data[3]) << 24
        return Float(bitPattern: bits)
    }

    private func decodePlainDouble(_ data: Data) -> Double {
        precondition(data.count == 8)
        var bits: UInt64 = 0
        for i in 0..<8 {
            bits |= UInt64(data[i]) << (i * 8)
        }
        return Double(bitPattern: bits)
    }

    private func decodePlainByteArray(_ data: Data) -> String {
        precondition(data.count >= 4)

        // Read 4-byte length prefix (little-endian)
        var length: UInt32 = 0
        length |= UInt32(data[0])
        length |= UInt32(data[1]) << 8
        length |= UInt32(data[2]) << 16
        length |= UInt32(data[3]) << 24

        let bytes = data.subdata(in: 4..<(4 + Int(length)))
        return String(data: bytes, encoding: .utf8)!
    }
}
