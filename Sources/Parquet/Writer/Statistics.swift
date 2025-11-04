// Statistics.swift - Type-safe statistics tracking for column writers
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Statistics for a column chunk (matches Thrift ColumnMetaData format)
///
/// Parquet statistics include both modern fields (minValue/maxValue) and
/// legacy fields (min/max) for backward compatibility. Modern readers
/// (Arrow C++, PyArrow, parquet-mr) prefer minValue/maxValue.
public struct ColumnChunkStatistics {
    /// Modern min field (preferred by Arrow/parquet-mr)
    var minValue: Data?

    /// Modern max field (preferred by Arrow/parquet-mr)
    var maxValue: Data?

    /// Legacy min field (populate for backward compatibility)
    var min: Data?

    /// Legacy max field (populate for backward compatibility)
    var max: Data?

    /// Count of NULL values in column chunk
    var nullCount: Int64?

    /// Cardinality estimate (not implemented - future)
    var distinctCount: Int64?

    public init() {
        self.minValue = nil
        self.maxValue = nil
        self.min = nil
        self.max = nil
        self.nullCount = nil
        self.distinctCount = nil
    }
}

/// Base protocol for type-safe statistics accumulators
///
/// Each physical type has its own concrete accumulator that implements
/// type-safe update methods (no Any? boxing).
protocol StatisticsAccumulator {
    /// Build final statistics from accumulated values
    func build() -> ColumnChunkStatistics

    /// Reset statistics for new column chunk
    func reset()
}

// MARK: - Int32 Statistics

/// Statistics accumulator for Int32 columns (type-safe, zero-copy)
final class Int32StatisticsAccumulator: StatisticsAccumulator {
    private var min: Int32?
    private var max: Int32?
    private var nullCount: Int64 = 0

    /// Update with required (non-nullable) values
    func update(_ values: [Int32]) {
        for value in values {
            if let currentMin = min {
                min = Swift.min(currentMin, value)
            } else {
                min = value
            }

            if let currentMax = max {
                max = Swift.max(currentMax, value)
            } else {
                max = value
            }
        }
    }

    /// Update with nullable values
    func updateNullable(_ values: [Int32?]) {
        for value in values {
            if let v = value {
                if let currentMin = min {
                    min = Swift.min(currentMin, v)
                } else {
                    min = v
                }

                if let currentMax = max {
                    max = Swift.max(currentMax, v)
                } else {
                    max = v
                }
            } else {
                nullCount += 1
            }
        }
    }

    func build() -> ColumnChunkStatistics {
        var stats = ColumnChunkStatistics()

        // Encode min/max as PLAIN Int32 (little-endian, 4 bytes)
        if let minValue = min {
            let encoded = encodePlainInt32(minValue)
            stats.minValue = encoded
            stats.min = encoded  // Legacy field
        }

        if let maxValue = max {
            let encoded = encodePlainInt32(maxValue)
            stats.maxValue = encoded
            stats.max = encoded  // Legacy field
        }

        stats.nullCount = nullCount > 0 ? nullCount : nil

        return stats
    }

    func reset() {
        min = nil
        max = nil
        nullCount = 0
    }

    private func encodePlainInt32(_ value: Int32) -> Data {
        var data = Data(count: 4)
        data[0] = UInt8(value & 0xFF)
        data[1] = UInt8((value >> 8) & 0xFF)
        data[2] = UInt8((value >> 16) & 0xFF)
        data[3] = UInt8((value >> 24) & 0xFF)
        return data
    }
}

// MARK: - Int64 Statistics

/// Statistics accumulator for Int64 columns (type-safe, zero-copy)
final class Int64StatisticsAccumulator: StatisticsAccumulator {
    private var min: Int64?
    private var max: Int64?
    private var nullCount: Int64 = 0

    /// Update with required (non-nullable) values
    func update(_ values: [Int64]) {
        for value in values {
            if let currentMin = min {
                min = Swift.min(currentMin, value)
            } else {
                min = value
            }

            if let currentMax = max {
                max = Swift.max(currentMax, value)
            } else {
                max = value
            }
        }
    }

    /// Update with nullable values
    func updateNullable(_ values: [Int64?]) {
        for value in values {
            if let v = value {
                if let currentMin = min {
                    min = Swift.min(currentMin, v)
                } else {
                    min = v
                }

                if let currentMax = max {
                    max = Swift.max(currentMax, v)
                } else {
                    max = v
                }
            } else {
                nullCount += 1
            }
        }
    }

    func build() -> ColumnChunkStatistics {
        var stats = ColumnChunkStatistics()

        // Encode min/max as PLAIN Int64 (little-endian, 8 bytes)
        if let minValue = min {
            let encoded = encodePlainInt64(minValue)
            stats.minValue = encoded
            stats.min = encoded  // Legacy field
        }

        if let maxValue = max {
            let encoded = encodePlainInt64(maxValue)
            stats.maxValue = encoded
            stats.max = encoded  // Legacy field
        }

        stats.nullCount = nullCount > 0 ? nullCount : nil

        return stats
    }

    func reset() {
        min = nil
        max = nil
        nullCount = 0
    }

    private func encodePlainInt64(_ value: Int64) -> Data {
        var data = Data(count: 8)
        for i in 0..<8 {
            data[i] = UInt8((value >> (i * 8)) & 0xFF)
        }
        return data
    }
}

// MARK: - Float Statistics

/// Statistics accumulator for Float columns (type-safe, zero-copy)
///
/// NaN values are excluded from min/max computation but are NOT counted as NULL.
final class FloatStatisticsAccumulator: StatisticsAccumulator {
    private var min: Float?
    private var max: Float?
    private var nullCount: Int64 = 0

    /// Update with required (non-nullable) values
    func update(_ values: [Float]) {
        for value in values {
            // Skip NaN in min/max (not counted as NULL)
            guard !value.isNaN else { continue }

            if let currentMin = min {
                min = Swift.min(currentMin, value)
            } else {
                min = value
            }

            if let currentMax = max {
                max = Swift.max(currentMax, value)
            } else {
                max = value
            }
        }
    }

    /// Update with nullable values
    func updateNullable(_ values: [Float?]) {
        for value in values {
            if let v = value {
                guard !v.isNaN else { continue }

                if let currentMin = min {
                    min = Swift.min(currentMin, v)
                } else {
                    min = v
                }

                if let currentMax = max {
                    max = Swift.max(currentMax, v)
                } else {
                    max = v
                }
            } else {
                nullCount += 1
            }
        }
    }

    func build() -> ColumnChunkStatistics {
        var stats = ColumnChunkStatistics()

        // Encode min/max as PLAIN Float (IEEE 754 single precision, little-endian)
        if let minValue = min {
            let encoded = encodePlainFloat(minValue)
            stats.minValue = encoded
            stats.min = encoded  // Legacy field
        }

        if let maxValue = max {
            let encoded = encodePlainFloat(maxValue)
            stats.maxValue = encoded
            stats.max = encoded  // Legacy field
        }

        stats.nullCount = nullCount > 0 ? nullCount : nil

        return stats
    }

    func reset() {
        min = nil
        max = nil
        nullCount = 0
    }

    private func encodePlainFloat(_ value: Float) -> Data {
        // IEEE 754 single precision, little-endian
        var data = Data(count: 4)
        let bits = value.bitPattern
        data[0] = UInt8(bits & 0xFF)
        data[1] = UInt8((bits >> 8) & 0xFF)
        data[2] = UInt8((bits >> 16) & 0xFF)
        data[3] = UInt8((bits >> 24) & 0xFF)
        return data
    }
}

// MARK: - Double Statistics

/// Statistics accumulator for Double columns (type-safe, zero-copy)
///
/// NaN values are excluded from min/max computation but are NOT counted as NULL.
final class DoubleStatisticsAccumulator: StatisticsAccumulator {
    private var min: Double?
    private var max: Double?
    private var nullCount: Int64 = 0

    /// Update with required (non-nullable) values
    func update(_ values: [Double]) {
        for value in values {
            // Skip NaN in min/max (not counted as NULL)
            guard !value.isNaN else { continue }

            if let currentMin = min {
                min = Swift.min(currentMin, value)
            } else {
                min = value
            }

            if let currentMax = max {
                max = Swift.max(currentMax, value)
            } else {
                max = value
            }
        }
    }

    /// Update with nullable values
    func updateNullable(_ values: [Double?]) {
        for value in values {
            if let v = value {
                guard !v.isNaN else { continue }

                if let currentMin = min {
                    min = Swift.min(currentMin, v)
                } else {
                    min = v
                }

                if let currentMax = max {
                    max = Swift.max(currentMax, v)
                } else {
                    max = v
                }
            } else {
                nullCount += 1
            }
        }
    }

    func build() -> ColumnChunkStatistics {
        var stats = ColumnChunkStatistics()

        // Encode min/max as PLAIN Double (IEEE 754 double precision, little-endian)
        if let minValue = min {
            let encoded = encodePlainDouble(minValue)
            stats.minValue = encoded
            stats.min = encoded  // Legacy field
        }

        if let maxValue = max {
            let encoded = encodePlainDouble(maxValue)
            stats.maxValue = encoded
            stats.max = encoded  // Legacy field
        }

        stats.nullCount = nullCount > 0 ? nullCount : nil

        return stats
    }

    func reset() {
        min = nil
        max = nil
        nullCount = 0
    }

    private func encodePlainDouble(_ value: Double) -> Data {
        // IEEE 754 double precision, little-endian
        var data = Data(count: 8)
        let bits = value.bitPattern
        for i in 0..<8 {
            data[i] = UInt8((bits >> (i * 8)) & 0xFF)
        }
        return data
    }
}

// MARK: - String Statistics

/// Statistics accumulator for String columns (type-safe, byte-wise comparison)
///
/// Uses lexicographical byte-wise comparison (not locale-sensitive) as required
/// by Parquet specification. Strings are stored as UTF-8 bytes internally.
final class StringStatisticsAccumulator: StatisticsAccumulator {
    private var min: Data?      // UTF-8 bytes for byte-wise comparison
    private var max: Data?
    private var nullCount: Int64 = 0

    /// Update with required (non-nullable) values
    func update(_ values: [String]) {
        for value in values {
            let bytes = Data(value.utf8)

            // Byte-wise comparison (not locale-sensitive)
            if let currentMin = min {
                if bytes.lexicographicallyPrecedes(currentMin) {
                    min = bytes
                }
            } else {
                min = bytes
            }

            if let currentMax = max {
                if currentMax.lexicographicallyPrecedes(bytes) {
                    max = bytes
                }
            } else {
                max = bytes
            }
        }
    }

    /// Update with nullable values
    func updateNullable(_ values: [String?]) {
        for value in values {
            if let v = value {
                let bytes = Data(v.utf8)

                if let currentMin = min {
                    if bytes.lexicographicallyPrecedes(currentMin) {
                        min = bytes
                    }
                } else {
                    min = bytes
                }

                if let currentMax = max {
                    if currentMax.lexicographicallyPrecedes(bytes) {
                        max = bytes
                    }
                } else {
                    max = bytes
                }
            } else {
                nullCount += 1
            }
        }
    }

    func build() -> ColumnChunkStatistics {
        var stats = ColumnChunkStatistics()

        // Encode as PLAIN ByteArray: [length: 4 bytes LE] [UTF-8 bytes]
        if let minBytes = min {
            let encoded = encodePlainByteArray(minBytes)
            stats.minValue = encoded
            stats.min = encoded  // Legacy field
        }

        if let maxBytes = max {
            let encoded = encodePlainByteArray(maxBytes)
            stats.maxValue = encoded
            stats.max = encoded  // Legacy field
        }

        stats.nullCount = nullCount > 0 ? nullCount : nil

        return stats
    }

    func reset() {
        min = nil
        max = nil
        nullCount = 0
    }

    private func encodePlainByteArray(_ bytes: Data) -> Data {
        // PLAIN ByteArray: [length: 4 bytes LE] [raw bytes]
        var data = Data(count: 4 + bytes.count)

        let length = UInt32(bytes.count)
        data[0] = UInt8(length & 0xFF)
        data[1] = UInt8((length >> 8) & 0xFF)
        data[2] = UInt8((length >> 16) & 0xFF)
        data[3] = UInt8((length >> 24) & 0xFF)
        data.replaceSubrange(4..<(4 + bytes.count), with: bytes)

        return data
    }
}
