// Thrift ConvertedType enum - Deprecated logical type annotations
//
// Licensed under the Apache License, Version 2.0

/// DEPRECATED: Common types used by frameworks (e.g. Hive, Pig) using Parquet.
///
/// ConvertedType is superseded by LogicalType. This enum should not be extended.
/// See LogicalTypes.md for conversion between ConvertedType and LogicalType.
///
/// Maps directly to the Thrift `ConvertedType` enum.
public enum ThriftConvertedType: Int32, Sendable {
    /// A BYTE_ARRAY actually contains UTF8 encoded chars
    case utf8 = 0

    /// A map is converted as an optional field containing a repeated key/value pair
    case map = 1

    /// A key/value pair is converted into a group of two fields
    case mapKeyValue = 2

    /// A list is converted into an optional field containing a repeated field
    case list = 3

    /// An enum is converted into a BYTE_ARRAY field
    case `enum` = 4

    /// A decimal value (BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY)
    case decimal = 5

    /// A date (stored as days since Unix epoch, INT32)
    case date = 6

    /// A time (milliseconds since midnight, INT32)
    case timeMillis = 7

    /// A time (microseconds since midnight, INT64)
    case timeMicros = 8

    /// A timestamp (milliseconds since Unix epoch, INT64)
    case timestampMillis = 9

    /// A timestamp (microseconds since Unix epoch, INT64)
    case timestampMicros = 10

    /// Unsigned 8-bit integer (INT32)
    case uint8 = 11

    /// Unsigned 16-bit integer (INT32)
    case uint16 = 12

    /// Unsigned 32-bit integer (INT32)
    case uint32 = 13

    /// Unsigned 64-bit integer (INT64)
    case uint64 = 14

    /// Signed 8-bit integer (INT32)
    case int8 = 15

    /// Signed 16-bit integer (INT32)
    case int16 = 16

    /// Signed 32-bit integer (INT32)
    case int32 = 17

    /// Signed 64-bit integer (INT64)
    case int64 = 18

    /// An embedded JSON document (UTF8 BYTE_ARRAY)
    case json = 19

    /// An embedded BSON document (BYTE_ARRAY)
    case bson = 20

    /// An interval of time (FIXED_LEN_BYTE_ARRAY of length 12)
    case interval = 21

    public var name: String {
        switch self {
        case .utf8: return "UTF8"
        case .map: return "MAP"
        case .mapKeyValue: return "MAP_KEY_VALUE"
        case .list: return "LIST"
        case .enum: return "ENUM"
        case .decimal: return "DECIMAL"
        case .date: return "DATE"
        case .timeMillis: return "TIME_MILLIS"
        case .timeMicros: return "TIME_MICROS"
        case .timestampMillis: return "TIMESTAMP_MILLIS"
        case .timestampMicros: return "TIMESTAMP_MICROS"
        case .uint8: return "UINT_8"
        case .uint16: return "UINT_16"
        case .uint32: return "UINT_32"
        case .uint64: return "UINT_64"
        case .int8: return "INT_8"
        case .int16: return "INT_16"
        case .int32: return "INT_32"
        case .int64: return "INT_64"
        case .json: return "JSON"
        case .bson: return "BSON"
        case .interval: return "INTERVAL"
        }
    }
}
