// Physical types in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Physical storage types in Parquet files
///
/// These types define how data is physically stored on disk.
/// They correspond to the Thrift `Type` enum in the Parquet specification.
///
/// # Physical Types
///
/// - `boolean`: Single bit, stored in bit-packed format
/// - `int32`: 32-bit signed integer (little-endian)
/// - `int64`: 64-bit signed integer (little-endian)
/// - `int96`: 96-bit value (deprecated, used for legacy timestamps)
/// - `float`: IEEE 32-bit floating point (little-endian)
/// - `double`: IEEE 64-bit floating point (little-endian)
/// - `byteArray`: Variable-length byte array
/// - `fixedLenByteArray`: Fixed-length byte array (length specified in schema)
///
/// # Usage
///
/// ```swift
/// let type = PhysicalType.int32
/// print(type.size) // 4 bytes
/// ```
public enum PhysicalType: Equatable, Hashable, Sendable {
    /// Boolean value (stored as bit-packed)
    case boolean

    /// 32-bit signed integer
    case int32

    /// 64-bit signed integer
    case int64

    /// 96-bit value (deprecated, used for legacy Impala timestamps)
    case int96

    /// IEEE 32-bit floating point
    case float

    /// IEEE 64-bit floating point
    case double

    /// Variable-length byte array
    case byteArray

    /// Fixed-length byte array
    /// - Parameter length: The fixed length in bytes
    case fixedLenByteArray(length: Int)

    /// Size in bytes for fixed-size types (nil for variable-size types)
    public var byteSize: Int? {
        switch self {
        case .boolean:
            return nil // Bit-packed, not byte-aligned
        case .int32, .float:
            return 4
        case .int64, .double:
            return 8
        case .int96:
            return 12
        case .byteArray:
            return nil // Variable length
        case .fixedLenByteArray(let length):
            return length
        }
    }

    /// Whether this type has a fixed size
    public var isFixedSize: Bool {
        byteSize != nil
    }

    /// Whether this type is variable length
    public var isVariableLength: Bool {
        !isFixedSize
    }

    /// String representation matching Parquet specification
    public var name: String {
        switch self {
        case .boolean:
            return "BOOLEAN"
        case .int32:
            return "INT32"
        case .int64:
            return "INT64"
        case .int96:
            return "INT96"
        case .float:
            return "FLOAT"
        case .double:
            return "DOUBLE"
        case .byteArray:
            return "BYTE_ARRAY"
        case .fixedLenByteArray(let length):
            return "FIXED_LEN_BYTE_ARRAY(\(length))"
        }
    }
}

extension PhysicalType: CustomStringConvertible {
    public var description: String {
        name
    }
}

extension PhysicalType: CustomDebugStringConvertible {
    public var debugDescription: String {
        name
    }
}
