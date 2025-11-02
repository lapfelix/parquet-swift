// Encoding types in Parquet
//
// Licensed under the Apache License, Version 2.0

/// Data encoding schemes in Parquet
///
/// Defines how values are encoded on disk for compression and efficiency.
/// Corresponds to the Thrift `Encoding` enum in the Parquet specification.
///
/// # Common Encodings
///
/// - `plain`: Raw values with no encoding (baseline)
/// - `plainDictionary`: Dictionary encoding (Parquet v1)
/// - `rle`: Run-length encoding (for booleans and levels)
/// - `rleDictionary`: Dictionary indices encoded with RLE (Parquet v2)
///
/// # Advanced Encodings
///
/// - `deltaBinaryPacked`: Delta encoding + bit packing (integers)
/// - `deltaLengthByteArray`: Delta encoding for lengths + raw data (byte arrays)
/// - `deltaByteArray`: Delta encoding for prefixes (byte arrays)
/// - `byteStreamSplit`: Byte stream splitting (floating point)
///
/// # Phase 1 Support
///
/// Phase 1 implements:
/// - PLAIN (all types)
/// - RLE_DICTIONARY (for dictionary-encoded columns)
/// - RLE (minimal, for definition levels and dictionary indices)
///
/// Remaining encodings deferred to Phase 2.
public enum Encoding: String, Equatable, Hashable, Sendable {
    // MARK: - Basic Encodings (Phase 1)

    /// Plain encoding: raw values, no compression
    ///
    /// Values are stored in their native format:
    /// - Fixed-width types: little-endian byte order
    /// - Variable-length: 4-byte length prefix + data
    case plain = "PLAIN"

    /// Plain dictionary encoding (Parquet v1, deprecated)
    ///
    /// Dictionary values followed by indices.
    /// Superseded by `rleDictionary` in Parquet v2.
    @available(*, deprecated, message: "Use rleDictionary instead")
    case plainDictionary = "PLAIN_DICTIONARY"

    /// Run-length encoding + bit packing
    ///
    /// Used for:
    /// - Boolean columns
    /// - Definition/repetition levels
    /// - Dictionary indices (in RLE_DICTIONARY)
    case rle = "RLE"

    /// Dictionary encoding with RLE indices (Parquet v2)
    ///
    /// Dictionary page with values (PLAIN encoded)
    /// + data pages with indices (RLE encoded)
    case rleDictionary = "RLE_DICTIONARY"

    // MARK: - Delta Encodings (Phase 2)

    /// Delta encoding with binary packing
    ///
    /// Efficient for sorted or monotonic integers.
    /// Stores first value + deltas, bit-packed.
    case deltaBinaryPacked = "DELTA_BINARY_PACKED"

    /// Delta encoding for byte array lengths
    ///
    /// Encodes lengths with delta encoding,
    /// concatenates raw data.
    case deltaLengthByteArray = "DELTA_LENGTH_BYTE_ARRAY"

    /// Delta encoding for byte array prefixes
    ///
    /// Encodes suffix lengths + suffixes.
    /// Efficient for strings with common prefixes.
    case deltaByteArray = "DELTA_BYTE_ARRAY"

    // MARK: - Advanced Encodings (Phase 2)

    /// Byte stream split encoding
    ///
    /// Splits floating-point values into separate streams
    /// for each byte position. Improves compression.
    case byteStreamSplit = "BYTE_STREAM_SPLIT"

    // MARK: - Legacy Encodings

    /// Deprecated: bit-packed encoding (Parquet v1)
    @available(*, deprecated, message: "Legacy encoding, use RLE instead")
    case bitPacked = "BIT_PACKED"

    // MARK: - Properties

    /// Whether this encoding is supported in Phase 1
    public var isPhase1Supported: Bool {
        switch self {
        case .plain, .rleDictionary, .rle:
            return true
        case .plainDictionary, .deltaBinaryPacked, .deltaLengthByteArray,
             .deltaByteArray, .byteStreamSplit, .bitPacked:
            return false
        }
    }

    /// Whether this is a dictionary encoding
    public var isDictionary: Bool {
        switch self {
        case .plainDictionary, .rleDictionary:
            return true
        default:
            return false
        }
    }

    /// Whether this is a delta encoding
    public var isDelta: Bool {
        switch self {
        case .deltaBinaryPacked, .deltaLengthByteArray, .deltaByteArray:
            return true
        default:
            return false
        }
    }

    /// Whether this encoding is deprecated
    public var isDeprecated: Bool {
        switch self {
        case .plainDictionary, .bitPacked:
            return true
        default:
            return false
        }
    }
}

extension Encoding: CustomStringConvertible {
    public var description: String {
        rawValue
    }
}

extension Encoding: CustomDebugStringConvertible {
    public var debugDescription: String {
        rawValue
    }
}
