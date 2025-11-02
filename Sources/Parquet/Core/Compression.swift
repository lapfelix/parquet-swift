// Compression codecs in Parquet
//
// Licensed under the Apache License, Version 2.0

/// Compression codec types in Parquet
///
/// Defines the compression algorithms available for compressing data pages.
/// Corresponds to the Thrift `CompressionCodec` enum.
///
/// # Supported Codecs
///
/// ## Phase 1
/// - `uncompressed`: No compression
/// - `gzip`: GZIP/DEFLATE (via Foundation)
/// - `snappy`: Snappy compression (best-effort, via C library)
///
/// ## Phase 2+
/// - `lz4`: LZ4 compression
/// - `zstd`: Zstandard compression
/// - `brotli`: Brotli compression
/// - `lzo`: LZO compression (rare)
///
/// # Usage
///
/// ```swift
/// let codec = Compression.snappy
/// if codec.isSupported {
///     let compressed = try codec.compress(data)
/// }
/// ```
public enum Compression: String, Equatable, Hashable, Sendable, CaseIterable {
    // MARK: - Phase 1 Codecs

    /// No compression
    case uncompressed = "UNCOMPRESSED"

    /// GZIP/DEFLATE compression
    ///
    /// Uses Foundation's `Compression` framework.
    /// Widely supported, moderate compression ratio, slower than Snappy.
    case gzip = "GZIP"

    /// Snappy compression
    ///
    /// Fast compression/decompression, lower ratio than GZIP.
    /// Most common in production Parquet files.
    /// Requires C library or Swift wrapper.
    case snappy = "SNAPPY"

    // MARK: - Phase 2+ Codecs

    /// LZ4 compression
    ///
    /// Very fast, similar to Snappy.
    /// Requires C library.
    case lz4 = "LZ4"

    /// LZ4_RAW compression (without frame header)
    ///
    /// Parquet v2.9.0+ uses raw LZ4 format.
    case lz4Raw = "LZ4_RAW"

    /// Zstandard compression
    ///
    /// Best compression ratio, good speed.
    /// Requires C library.
    case zstd = "ZSTD"

    /// Brotli compression
    ///
    /// High compression ratio, slower.
    /// Requires C library.
    case brotli = "BROTLI"

    /// LZO compression (legacy, rare)
    case lzo = "LZO"

    // MARK: - Properties

    /// Whether this codec is supported in Phase 1
    public var isPhase1Supported: Bool {
        switch self {
        case .uncompressed, .gzip:
            return true
        case .snappy:
            return true // Best-effort
        case .lz4, .lz4Raw, .zstd, .brotli, .lzo:
            return false
        }
    }

    /// Whether this codec is implemented (Phase 1)
    ///
    /// Note: Snappy depends on C library availability
    public var isImplemented: Bool {
        switch self {
        case .uncompressed, .gzip:
            return true
        case .snappy, .lz4, .lz4Raw, .zstd, .brotli, .lzo:
            return false // Will be implemented during M1.8
        }
    }

    /// Typical compression ratio (compressed / original)
    ///
    /// Approximate values for reference:
    /// - Uncompressed: 1.0
    /// - Snappy: 0.5-0.7
    /// - LZ4: 0.5-0.7
    /// - GZIP: 0.3-0.5
    /// - ZSTD: 0.3-0.5
    /// - Brotli: 0.2-0.4
    public var typicalRatio: ClosedRange<Double> {
        switch self {
        case .uncompressed:
            return 1.0...1.0
        case .snappy, .lz4, .lz4Raw:
            return 0.5...0.7
        case .gzip, .zstd:
            return 0.3...0.5
        case .brotli:
            return 0.2...0.4
        case .lzo:
            return 0.4...0.6
        }
    }

    /// Relative speed (1 = fastest, 5 = slowest)
    public var relativeSpeed: Int {
        switch self {
        case .uncompressed:
            return 0 // No computation
        case .snappy, .lz4, .lz4Raw, .lzo:
            return 1 // Very fast
        case .zstd:
            return 2 // Fast
        case .gzip:
            return 3 // Moderate
        case .brotli:
            return 4 // Slower
        }
    }

    /// Whether this codec requires an external library
    public var requiresExternalLibrary: Bool {
        switch self {
        case .uncompressed, .gzip:
            return false // Foundation built-in
        case .snappy, .lz4, .lz4Raw, .zstd, .brotli, .lzo:
            return true
        }
    }
}

extension Compression: CustomStringConvertible {
    public var description: String {
        rawValue
    }
}

extension Compression: CustomDebugStringConvertible {
    public var debugDescription: String {
        rawValue
    }
}
