// Tests for Codec implementations
//
// Licensed under the Apache License, Version 2.0

import XCTest
@testable import Parquet

final class CodecTests: XCTestCase {
    // MARK: - Codec Factory Tests

    func testCodecFactoryUncompressed() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        XCTAssertEqual(codec.compressionType, .uncompressed)
    }

    func testCodecFactoryGzip() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        XCTAssertEqual(codec.compressionType, .gzip)
    }

    func testCodecFactorySnappy() throws {
        // Snappy is now available in Phase 2
        let codec = try CodecFactory.codec(for: .snappy)
        XCTAssertEqual(codec.compressionType, .snappy)
    }

    func testCodecFactoryUnsupportedCodecs() {
        let unsupported: [Compression] = [.lz4, .lz4Raw, .zstd, .brotli, .lzo]

        for compression in unsupported {
            XCTAssertThrowsError(try CodecFactory.codec(for: compression)) { error in
                guard case CodecError.unavailable = error else {
                    XCTFail("Expected unavailable error for \(compression)")
                    return
                }
            }
        }
    }

    func testCodecFactoryIsAvailable() {
        XCTAssertTrue(CodecFactory.isAvailable(.uncompressed))
        XCTAssertTrue(CodecFactory.isAvailable(.gzip))
        XCTAssertTrue(CodecFactory.isAvailable(.snappy)) // Now available!
        XCTAssertFalse(CodecFactory.isAvailable(.lz4))
    }

    // MARK: - Uncompressed Codec Tests

    func testUncompressedDecompress() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        let data = Data("Hello, World!".utf8)

        let decompressed = try codec.decompress(data, uncompressedSize: data.count)
        XCTAssertEqual(decompressed, data)
    }

    func testUncompressedCompress() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        let data = Data("Hello, World!".utf8)

        let compressed = try codec.compress(data)
        XCTAssertEqual(compressed, data)
    }

    func testUncompressedSizeMismatch() throws {
        let codec = try CodecFactory.codec(for: .uncompressed)
        let data = Data("Hello, World!".utf8)

        // Wrong uncompressed size should fail
        XCTAssertThrowsError(try codec.decompress(data, uncompressedSize: 100)) { error in
            guard case CodecError.sizeMismatch(let expected, let actual) = error else {
                XCTFail("Expected sizeMismatch error")
                return
            }
            XCTAssertEqual(expected, 100)
            XCTAssertEqual(actual, data.count)
        }
    }

    // MARK: - GZIP Codec Tests

    func testGzipRoundTrip() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        let original = Data("Hello, World! This is a test string for compression.".utf8)

        // Compress
        let compressed = try codec.compress(original)

        // Should be smaller (or at least different)
        print("Original: \(original.count) bytes, Compressed: \(compressed.count) bytes")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        // Should match original
        XCTAssertEqual(decompressed, original)
    }

    func testGzipLargeData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Create 10KB of repeated data (compresses well)
        let pattern = Data("ABCDEFGHIJ".utf8)
        var original = Data()
        for _ in 0..<1000 {
            original.append(pattern)
        }

        // Compress
        let compressed = try codec.compress(original)

        // Should achieve good compression
        let ratio = Double(compressed.count) / Double(original.count)
        print("Compression ratio: \(ratio)")
        XCTAssertLessThan(compressed.count, original.count, "Compressed should be smaller")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testGzipEmptyData() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        let empty = Data()

        let compressed = try codec.compress(empty)
        let decompressed = try codec.decompress(compressed, uncompressedSize: 0)

        XCTAssertEqual(decompressed, empty)
    }

    func testGzipBinaryData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Create binary data (0-255 repeated)
        var original = Data()
        for i in 0..<256 {
            original.append(UInt8(i))
        }
        original.append(contentsOf: original) // 512 bytes

        let compressed = try codec.compress(original)
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        XCTAssertEqual(decompressed, original)
    }

    func testGzipInvalidCompressedData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Invalid GZIP data
        let invalid = Data([0xFF, 0xFF, 0xFF, 0xFF])

        XCTAssertThrowsError(try codec.decompress(invalid, uncompressedSize: 100)) { error in
            guard case CodecError.decompressionFailed = error else {
                XCTFail("Expected decompressionFailed error")
                return
            }
        }
    }

    func testGzipSizeMismatch() throws {
        let codec = try CodecFactory.codec(for: .gzip)
        let original = Data("Hello, World!".utf8)

        let compressed = try codec.compress(original)

        // Wrong uncompressed size
        XCTAssertThrowsError(try codec.decompress(compressed, uncompressedSize: 1000)) { error in
            guard case CodecError.sizeMismatch = error else {
                XCTFail("Expected sizeMismatch error")
                return
            }
        }
    }

    // MARK: - Snappy Codec Tests

    func testSnappyRoundTrip() throws {
        let codec = try CodecFactory.codec(for: .snappy)
        let original = Data("Hello, World! This is a test string for Snappy compression.".utf8)

        // Compress
        let compressed = try codec.compress(original)

        // Should be smaller (or at least different)
        print("Snappy Original: \(original.count) bytes, Compressed: \(compressed.count) bytes")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        // Should match original
        XCTAssertEqual(decompressed, original)
    }

    func testSnappyLargeData() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Create 10KB of repeated data (compresses well)
        let pattern = Data("ABCDEFGHIJ".utf8)
        var original = Data()
        for _ in 0..<1000 {
            original.append(pattern)
        }

        // Compress
        let compressed = try codec.compress(original)

        // Should achieve compression
        let ratio = Double(compressed.count) / Double(original.count)
        print("Snappy compression ratio: \(ratio)")
        XCTAssertLessThan(compressed.count, original.count, "Compressed should be smaller")

        // Decompress
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testSnappyEmptyData() throws {
        let codec = try CodecFactory.codec(for: .snappy)
        let empty = Data()

        let compressed = try codec.compress(empty)
        let decompressed = try codec.decompress(compressed, uncompressedSize: 0)

        XCTAssertEqual(decompressed, empty)
    }

    func testSnappyBinaryData() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Create binary data (0-255 repeated)
        var original = Data()
        for i in 0..<256 {
            original.append(UInt8(i))
        }
        original.append(contentsOf: original) // 512 bytes

        let compressed = try codec.compress(original)
        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)

        XCTAssertEqual(decompressed, original)
    }

    func testSnappyRepeatingPattern() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Highly compressible data
        let original = Data(repeating: 0x42, count: 1000)

        let compressed = try codec.compress(original)

        // Should compress very well
        let ratio = Double(compressed.count) / Double(original.count)
        print("Snappy repeating pattern compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.5, "Repeating data should compress significantly")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testSnappyTextData() throws {
        let codec = try CodecFactory.codec(for: .snappy)

        // Realistic text data
        let text = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        """

        let original = Data(text.utf8)
        let compressed = try codec.compress(original)

        let ratio = Double(compressed.count) / Double(original.count)
        print("Snappy text data compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 1.0, "Text should compress")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    // MARK: - Real-World Data Tests

    func testGzipRepeatingPattern() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Highly compressible data
        let original = Data(repeating: 0x42, count: 1000)

        let compressed = try codec.compress(original)

        // Should compress very well
        let ratio = Double(compressed.count) / Double(original.count)
        print("Repeating pattern compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.1, "Repeating data should compress to < 10%")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testGzipRandomData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Random data (doesn't compress well)
        var original = Data(count: 1000)
        for i in 0..<1000 {
            original[i] = UInt8.random(in: 0...255)
        }

        let compressed = try codec.compress(original)

        // May not compress much (random data)
        let ratio = Double(compressed.count) / Double(original.count)
        print("Random data compression ratio: \(ratio)")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    func testGzipTextData() throws {
        let codec = try CodecFactory.codec(for: .gzip)

        // Realistic text data
        let text = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.
        Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
        """

        let original = Data(text.utf8)
        let compressed = try codec.compress(original)

        let ratio = Double(compressed.count) / Double(original.count)
        print("Text data compression ratio: \(ratio)")
        XCTAssertLessThan(ratio, 0.8, "Text should compress reasonably")

        let decompressed = try codec.decompress(compressed, uncompressedSize: original.count)
        XCTAssertEqual(decompressed, original)
    }

    // MARK: - Error Description Tests

    func testCodecErrorDescriptions() {
        let unavailable = CodecError.unavailable("Test message")
        XCTAssertTrue(unavailable.description.contains("unavailable"))

        let decompFailed = CodecError.decompressionFailed("Test")
        XCTAssertTrue(decompFailed.description.contains("Decompression failed"))

        let compFailed = CodecError.compressionFailed("Test")
        XCTAssertTrue(compFailed.description.contains("Compression failed"))

        let sizeMismatch = CodecError.sizeMismatch(expected: 100, actual: 50)
        XCTAssertTrue(sizeMismatch.description.contains("100"))
        XCTAssertTrue(sizeMismatch.description.contains("50"))

        let invalid = CodecError.invalidData("Test")
        XCTAssertTrue(invalid.description.contains("Invalid data"))
    }
}
