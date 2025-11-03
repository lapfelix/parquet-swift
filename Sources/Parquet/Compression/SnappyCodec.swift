// Snappy compression codec
//
// Licensed under the Apache License, Version 2.0

import Foundation
import SnappySwift

/// Snappy compression codec
///
/// Snappy is a fast compression algorithm focused on speed rather than
/// maximum compression. It's the most common compression in production Parquet files.
///
/// # Implementation Status
///
/// **Phase 2**: Pure Swift implementation ✅
///
/// # Implementation
///
/// Uses [snappy-swift](https://github.com/codelynx/snappy-swift), a pure Swift
/// implementation with zero dependencies. No system libraries required!
///
/// # Format
///
/// Parquet uses raw Snappy format (not framed). Each compressed block
/// is independent and must be decompressed separately.
///
/// # Performance
///
/// - Compression ratio: Moderate (50-70% of original)
/// - Compression speed: 64-128 MB/s (depending on data size)
/// - Decompression speed: 203-261 MB/s (depending on data size)
/// - CPU usage: Low
/// - 100% compatible with C++ reference implementation
///
/// # Usage
///
/// ```swift
/// let codec = SnappyCodec()
/// let decompressed = try codec.decompress(compressed, uncompressedSize: 1024)
/// ```
struct SnappyCodec: Codec {
    var compressionType: Compression { .snappy }

    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
        guard !data.isEmpty else {
            throw CodecError.invalidData("Empty compressed data")
        }

        // Always delegate to SnappySwift for proper validation,
        // even when uncompressedSize is 0 (validates corrupted data)
        do {
            let decompressed = try data.snappyDecompressed()

            // Verify size matches expectation
            guard decompressed.count == uncompressedSize else {
                throw CodecError.sizeMismatch(expected: uncompressedSize, actual: decompressed.count)
            }

            return decompressed
        } catch {
            throw CodecError.decompressionFailed("Snappy decompression failed: \(error)")
        }
    }

    func compress(_ data: Data) throws -> Data {
        // Always delegate to SnappySwift for proper wire format,
        // even for empty data (emits valid Snappy header)
        do {
            return try data.snappyCompressed()
        } catch {
            throw CodecError.compressionFailed("Snappy compression failed: \(error)")
        }
    }
}
