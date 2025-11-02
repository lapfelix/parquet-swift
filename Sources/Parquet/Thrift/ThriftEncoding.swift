// Thrift Encoding enum - Encoding schemes in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Encodings supported by Parquet.
///
/// Not all encodings are valid for all types. These enums are also used to specify
/// the encoding of definition and repetition levels.
///
/// Maps directly to the Thrift `Encoding` enum.
public enum ThriftEncoding: Int32, Sendable {
    /// Default encoding - values stored in their natural format
    case plain = 0

    /// Deprecated: Dictionary encoding (use rleDictionary in data pages)
    case plainDictionary = 2

    /// Group packed run length encoding
    /// Used for definition/repetition levels and booleans
    case rle = 3

    /// Bit packed encoding (deprecated for data values)
    /// Used for definition/repetition levels
    case bitPacked = 4

    /// Delta encoding for integers
    /// Works best on sorted data
    case deltaBinaryPacked = 5

    /// Encoding for byte arrays to separate length values and data
    /// Lengths are encoded using DELTA_BINARY_PACKED
    case deltaLengthByteArray = 6

    /// Incremental-encoded byte array
    /// Prefix lengths are encoded using DELTA_BINARY_PACKED
    case deltaByteArray = 7

    /// Dictionary encoding: the ids are encoded using RLE
    case rleDictionary = 8

    /// Byte stream split encoding
    /// Added in Parquet 2.8 for FLOAT and DOUBLE
    /// Support for INT32, INT64, FIXED_LEN_BYTE_ARRAY added in 2.11
    case byteStreamSplit = 9

    public var name: String {
        switch self {
        case .plain: return "PLAIN"
        case .plainDictionary: return "PLAIN_DICTIONARY"
        case .rle: return "RLE"
        case .bitPacked: return "BIT_PACKED"
        case .deltaBinaryPacked: return "DELTA_BINARY_PACKED"
        case .deltaLengthByteArray: return "DELTA_LENGTH_BYTE_ARRAY"
        case .deltaByteArray: return "DELTA_BYTE_ARRAY"
        case .rleDictionary: return "RLE_DICTIONARY"
        case .byteStreamSplit: return "BYTE_STREAM_SPLIT"
        }
    }
}
