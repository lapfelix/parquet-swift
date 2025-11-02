// Thrift LogicalType - Logical type annotations for Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Time unit for temporal logical types.
///
/// Maps to Thrift `TimeUnit` union.
public enum ThriftTimeUnit: Sendable {
    case millis
    case micros
    case nanos

    public var name: String {
        switch self {
        case .millis: return "MILLIS"
        case .micros: return "MICROS"
        case .nanos: return "NANOS"
        }
    }
}

/// Decimal logical type annotation.
///
/// Scale must be zero or a positive integer less than or equal to the precision.
/// Precision must be a non-zero positive integer.
///
/// Allowed for physical types: INT32, INT64, FIXED_LEN_BYTE_ARRAY, and BYTE_ARRAY.
public struct ThriftDecimalType: Sendable {
    public let scale: Int32
    public let precision: Int32

    public init(scale: Int32, precision: Int32) {
        self.scale = scale
        self.precision = precision
    }
}

/// Timestamp logical type annotation.
///
/// Allowed for physical types: INT64
public struct ThriftTimestampType: Sendable {
    public let isAdjustedToUTC: Bool
    public let unit: ThriftTimeUnit

    public init(isAdjustedToUTC: Bool, unit: ThriftTimeUnit) {
        self.isAdjustedToUTC = isAdjustedToUTC
        self.unit = unit
    }
}

/// Time logical type annotation.
///
/// Allowed for physical types: INT32 (millis), INT64 (micros, nanos)
public struct ThriftTimeType: Sendable {
    public let isAdjustedToUTC: Bool
    public let unit: ThriftTimeUnit

    public init(isAdjustedToUTC: Bool, unit: ThriftTimeUnit) {
        self.isAdjustedToUTC = isAdjustedToUTC
        self.unit = unit
    }
}

/// Integer logical type annotation.
///
/// bitWidth must be 8, 16, 32, or 64.
///
/// Allowed for physical types: INT32, INT64
public struct ThriftIntType: Sendable {
    public let bitWidth: Int8
    public let isSigned: Bool

    public init(bitWidth: Int8, isSigned: Bool) {
        self.bitWidth = bitWidth
        self.isSigned = isSigned
    }
}

/// LogicalType annotations to replace ConvertedType.
///
/// To maintain compatibility, implementations using LogicalType for a SchemaElement
/// must also set the corresponding ConvertedType (if any).
///
/// Maps to Thrift `LogicalType` union.
public enum ThriftLogicalType: Sendable {
    /// String type - use ConvertedType UTF8
    case string

    /// Map type - use ConvertedType MAP
    case map

    /// List type - use ConvertedType LIST
    case list

    /// Enum type - use ConvertedType ENUM
    case `enum`

    /// Decimal type - use ConvertedType DECIMAL
    case decimal(ThriftDecimalType)

    /// Date type - use ConvertedType DATE
    case date

    /// Time type
    case time(ThriftTimeType)

    /// Timestamp type
    case timestamp(ThriftTimestampType)

    /// Integer type - use ConvertedType INT_* or UINT_*
    case integer(ThriftIntType)

    /// Unknown/Null type - no compatible ConvertedType
    case unknown

    /// JSON type - use ConvertedType JSON
    case json

    /// BSON type - use ConvertedType BSON
    case bson

    /// UUID type - no compatible ConvertedType
    case uuid

    /// Float16 type - no compatible ConvertedType
    case float16

    public var name: String {
        switch self {
        case .string: return "STRING"
        case .map: return "MAP"
        case .list: return "LIST"
        case .enum: return "ENUM"
        case .decimal(let d): return "DECIMAL(precision=\(d.precision), scale=\(d.scale))"
        case .date: return "DATE"
        case .time(let t): return "TIME(isAdjustedToUTC=\(t.isAdjustedToUTC), unit=\(t.unit.name))"
        case .timestamp(let t): return "TIMESTAMP(isAdjustedToUTC=\(t.isAdjustedToUTC), unit=\(t.unit.name))"
        case .integer(let i): return "INTEGER(bitWidth=\(i.bitWidth), signed=\(i.isSigned))"
        case .unknown: return "UNKNOWN"
        case .json: return "JSON"
        case .bson: return "BSON"
        case .uuid: return "UUID"
        case .float16: return "FLOAT16"
        }
    }
}
