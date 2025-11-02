// Snappy compression codec
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Snappy compression codec
///
/// Snappy is a fast compression algorithm focused on speed rather than
/// maximum compression. It's the most common compression in production Parquet files.
///
/// # Implementation Status
///
/// **Phase 1**: Stub implementation (throws unavailable error)
/// **Phase 2**: Full implementation with C library binding
///
/// # Adding Snappy Support
///
/// To enable Snappy compression, add a system library target to Package.swift:
///
/// ```swift
/// .systemLibrary(
///     name: "CSnappy",
///     pkgConfig: "snappy",
///     providers: [
///         .brew(["snappy"]),
///         .apt(["libsnappy-dev"])
///     ]
/// )
/// ```
///
/// Then install the library:
/// - macOS: `brew install snappy`
/// - Linux: `apt-get install libsnappy-dev`
///
/// # Format
///
/// Parquet uses raw Snappy format (not framed). Each compressed block
/// is independent and must be decompressed separately.
///
/// # Performance
///
/// - Compression ratio: Moderate (50-70% of original)
/// - Speed: Very fast (faster than GZIP)
/// - CPU usage: Low
struct SnappyCodec: Codec {
    var compressionType: Compression { .snappy }

    func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
        // Phase 1: Snappy not yet implemented
        // This will be implemented in Phase 2 when C library binding is added
        throw CodecError.unavailable(
            "Snappy codec not yet implemented. " +
            "Install libsnappy and rebuild to enable Snappy support."
        )
    }

    func compress(_ data: Data) throws -> Data {
        // Phase 1: Snappy not yet implemented
        throw CodecError.unavailable(
            "Snappy codec not yet implemented. " +
            "Install libsnappy and rebuild to enable Snappy support."
        )
    }
}

// MARK: - Future Implementation Notes

/*
 When implementing Snappy support, use these C functions:

 // Decompression
 snappy_status snappy_uncompress(
     const char* compressed,
     size_t compressed_length,
     char* uncompressed,
     size_t* uncompressed_length
 );

 // Compression
 snappy_status snappy_compress(
     const char* input,
     size_t input_length,
     char* compressed,
     size_t* compressed_length
 );

 // Get uncompressed length
 snappy_status snappy_uncompressed_length(
     const char* compressed,
     size_t compressed_length,
     size_t* result
 );

 Example implementation:

 func decompress(_ data: Data, uncompressedSize: Int) throws -> Data {
     var outputBuffer = Data(count: uncompressedSize)
     var actualSize = uncompressedSize

     let status = data.withUnsafeBytes { inputPtr in
         outputBuffer.withUnsafeMutableBytes { outputPtr in
             snappy_uncompress(
                 inputPtr.baseAddress!.assumingMemoryBound(to: Int8.self),
                 data.count,
                 outputPtr.baseAddress!.assumingMemoryBound(to: Int8.self),
                 &actualSize
             )
         }
     }

     guard status == SNAPPY_OK else {
         throw CodecError.decompressionFailed("Snappy error: \(status)")
     }

     guard actualSize == uncompressedSize else {
         throw CodecError.sizeMismatch(expected: uncompressedSize, actual: actualSize)
     }

     return outputBuffer
 }
 */
