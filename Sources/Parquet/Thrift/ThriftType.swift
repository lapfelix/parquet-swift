// Thrift Type enum - Physical types in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Physical types supported by Parquet.
///
/// These types are intended to be used in combination with encodings to control
/// the on-disk storage format. Maps directly to the Thrift `Type` enum.
public enum ThriftType: Int32, Sendable {
    case boolean = 0
    case int32 = 1
    case int64 = 2
    case int96 = 3  // Deprecated, only used by legacy implementations
    case float = 4
    case double = 5
    case byteArray = 6
    case fixedLenByteArray = 7

    public var name: String {
        switch self {
        case .boolean: return "BOOLEAN"
        case .int32: return "INT32"
        case .int64: return "INT64"
        case .int96: return "INT96"
        case .float: return "FLOAT"
        case .double: return "DOUBLE"
        case .byteArray: return "BYTE_ARRAY"
        case .fixedLenByteArray: return "FIXED_LEN_BYTE_ARRAY"
        }
    }
}
