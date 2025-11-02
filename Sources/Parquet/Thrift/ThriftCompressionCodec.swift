// Thrift CompressionCodec enum - Compression codecs in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Supported compression algorithms.
///
/// Codecs added in format version X.Y can be read by readers based on X.Y and later.
/// Codec support may vary between readers based on the format version and libraries
/// available at runtime.
///
/// Maps directly to the Thrift `CompressionCodec` enum.
public enum ThriftCompressionCodec: Int32, Sendable {
    case uncompressed = 0
    case snappy = 1
    case gzip = 2
    case lzo = 3
    case brotli = 4  // Added in Parquet 2.4
    case lz4 = 5     // Added in Parquet 2.4 (deprecated)
    case zstd = 6    // Added in Parquet 2.4
    case lz4Raw = 7  // Added in Parquet 2.9

    public var name: String {
        switch self {
        case .uncompressed: return "UNCOMPRESSED"
        case .snappy: return "SNAPPY"
        case .gzip: return "GZIP"
        case .lzo: return "LZO"
        case .brotli: return "BROTLI"
        case .lz4: return "LZ4"
        case .zstd: return "ZSTD"
        case .lz4Raw: return "LZ4_RAW"
        }
    }
}
