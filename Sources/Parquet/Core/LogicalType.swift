// Logical types in Parquet
//
// Licensed under the Apache License, Version 2.0

/// Logical type annotation for Parquet columns
///
/// Logical types add semantic meaning to physical types.
/// For example, a `STRING` logical type uses `BYTE_ARRAY` physical storage
/// but indicates the bytes should be interpreted as UTF-8 text.
///
/// Corresponds to `LogicalType` in the Parquet Thrift specification.
///
/// # Common Logical Types
///
/// - `string`: UTF-8 encoded text
/// - `date`: Days since Unix epoch (int32)
/// - `timestamp`: Microseconds/milliseconds/nanoseconds since epoch
/// - `decimal`: Fixed-precision decimal numbers
/// - `uuid`: 16-byte universally unique identifier
/// - `json`: UTF-8 encoded JSON
///
/// # Usage
///
/// ```swift
/// let logicalType = LogicalType.string
/// print(logicalType.compatiblePhysicalTypes) // [.byteArray]
/// ```
public enum LogicalType: Equatable, Hashable, Sendable {
    // MARK: - Primitive Logical Types

    /// UTF-8 encoded string
    ///
    /// **Physical type:** BYTE_ARRAY
    case string

    /// Enumeration (UTF-8 encoded)
    ///
    /// **Physical type:** BYTE_ARRAY
    case `enum`

    /// Universally unique identifier (16 bytes)
    ///
    /// **Physical type:** FIXED_LEN_BYTE_ARRAY(16)
    case uuid

    // MARK: - Temporal Types

    /// Date (days since Unix epoch: 1970-01-01)
    ///
    /// **Physical type:** INT32
    case date

    /// Time of day
    ///
    /// **Physical type:** INT32 (milliseconds) or INT64 (micro/nanoseconds)
    ///
    /// - Parameters:
    ///   - isAdjustedToUTC: Whether time is adjusted to UTC
    ///   - unit: Time unit (millis, micros, nanos)
    case time(isAdjustedToUTC: Bool, unit: TimeUnit)

    /// Timestamp (instant in time)
    ///
    /// **Physical type:** INT64
    ///
    /// - Parameters:
    ///   - isAdjustedToUTC: Whether timestamp is in UTC
    ///   - unit: Time unit (millis, micros, nanos)
    case timestamp(isAdjustedToUTC: Bool, unit: TimeUnit)

    // MARK: - Numeric Types

    /// Signed or unsigned integer with specific bit width
    ///
    /// **Physical type:** INT32 or INT64
    ///
    /// - Parameters:
    ///   - bitWidth: Number of bits (8, 16, 32, 64)
    ///   - isSigned: Whether the integer is signed
    case integer(bitWidth: Int, isSigned: Bool)

    /// Arbitrary-precision decimal number
    ///
    /// **Physical type:** INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY
    ///
    /// - Parameters:
    ///   - precision: Total number of digits
    ///   - scale: Number of digits after decimal point
    case decimal(precision: Int, scale: Int)

    // MARK: - Complex Types

    /// UTF-8 encoded JSON
    ///
    /// **Physical type:** BYTE_ARRAY
    case json

    /// Binary JSON (BSON)
    ///
    /// **Physical type:** BYTE_ARRAY
    case bson

    /// List (array) of elements
    ///
    /// Used with Parquet's nested encoding.
    case list

    /// Map (key-value pairs)
    ///
    /// Used with Parquet's nested encoding.
    case map

    // MARK: - Properties

    /// Compatible physical types for this logical type
    public var compatiblePhysicalTypes: [PhysicalType] {
        switch self {
        case .string, .enum, .json, .bson:
            return [.byteArray]

        case .uuid:
            return [.fixedLenByteArray(length: 16)]

        case .date:
            return [.int32]

        case .time(_, let unit):
            switch unit {
            case .milliseconds:
                return [.int32]
            case .microseconds, .nanoseconds:
                return [.int64]
            }

        case .timestamp:
            return [.int64]

        case .integer(let bitWidth, _):
            switch bitWidth {
            case 8, 16, 32:
                return [.int32]
            case 64:
                return [.int64]
            default:
                return []
            }

        case .decimal:
            return [.int32, .int64, .byteArray, .fixedLenByteArray(length: 0)]

        case .list, .map:
            return [] // These work with nested structures, not single physical types
        }
    }

    /// String representation matching Parquet specification
    public var name: String {
        switch self {
        case .string:
            return "STRING"
        case .enum:
            return "ENUM"
        case .uuid:
            return "UUID"
        case .date:
            return "DATE"
        case .time(let isUTC, let unit):
            return "TIME(isAdjustedToUTC=\(isUTC), unit=\(unit.rawValue))"
        case .timestamp(let isUTC, let unit):
            return "TIMESTAMP(isAdjustedToUTC=\(isUTC), unit=\(unit.rawValue))"
        case .integer(let bitWidth, let isSigned):
            let signedness = isSigned ? "signed" : "unsigned"
            return "INT(\(bitWidth), \(signedness))"
        case .decimal(let precision, let scale):
            return "DECIMAL(precision=\(precision), scale=\(scale))"
        case .json:
            return "JSON"
        case .bson:
            return "BSON"
        case .list:
            return "LIST"
        case .map:
            return "MAP"
        }
    }
}

// MARK: - Time Unit

/// Time unit for temporal types
public enum TimeUnit: String, Equatable, Hashable, Sendable {
    /// Milliseconds (10^-3 seconds)
    case milliseconds = "MILLIS"

    /// Microseconds (10^-6 seconds)
    case microseconds = "MICROS"

    /// Nanoseconds (10^-9 seconds)
    case nanoseconds = "NANOS"

    /// Conversion factor to seconds
    public var toSeconds: Double {
        switch self {
        case .milliseconds:
            return 1e-3
        case .microseconds:
            return 1e-6
        case .nanoseconds:
            return 1e-9
        }
    }
}

extension LogicalType: CustomStringConvertible {
    public var description: String {
        name
    }
}

extension LogicalType: CustomDebugStringConvertible {
    public var debugDescription: String {
        name
    }
}
