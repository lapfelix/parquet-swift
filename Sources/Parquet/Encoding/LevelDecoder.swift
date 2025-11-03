// LevelDecoder - Decode definition and repetition levels
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Decodes definition and repetition levels from RLE/bit-packed hybrid encoding
///
/// # Format (Data Page V1)
///
/// ```
/// <length: 4 bytes LE> <varint-encoded runs>
/// ```
///
/// Each run is either:
/// - **Bit-packed**: Header `varint((numGroups << 1) | 1)`, followed by bit-packed values in groups of 8
/// - **RLE**: Header `varint(count << 1)`, followed by repeated value (padded to byte boundary)
///
/// # Varint Encoding
///
/// Uses ULEB-128 (unsigned Little Endian Base 128):
/// - Each byte encodes 7 bits of data
/// - High bit (bit 7) = 1 means more bytes follow
/// - Values are little-endian
///
/// # Reference
///
/// Based on Apache Arrow/Parquet RLE encoding:
/// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/rle_encoding_internal.h
public final class LevelDecoder {

    public init() {}

    // MARK: - Public API

    /// Decode definition or repetition levels
    ///
    /// - Parameters:
    ///   - data: Complete level stream (4-byte length + runs)
    ///   - numValues: Expected number of level values
    ///   - maxLevel: Maximum level value (determines bit-width)
    /// - Returns: Array of level values (0 to maxLevel)
    /// - Throws: `LevelError` if decoding fails
    public func decodeLevels(from data: Data, numValues: Int, maxLevel: Int) throws -> [UInt8] {
        // 1. Extract 4-byte length prefix (little-endian)
        guard data.count >= 4 else {
            throw LevelError.missingLengthPrefix
        }

        let runDataLength = Int(UInt32(data[0])
            | (UInt32(data[1]) << 8)
            | (UInt32(data[2]) << 16)
            | (UInt32(data[3]) << 24))

        // 2. Validate buffer size
        let expectedSize = 4 + runDataLength
        guard data.count == expectedSize else {
            throw LevelError.invalidSize(expected: expectedSize, got: data.count)
        }

        // 3. Calculate bit-width from maxLevel
        let bitWidth = maxLevel == 0 ? 0 : (64 - (maxLevel).leadingZeroBitCount)

        // 4. Decode runs
        let runData = data.subdata(in: 4..<expectedSize)
        return try decodeRuns(
            data: runData,
            numValues: numValues,
            bitWidth: bitWidth
        )
    }

    // MARK: - Private Helpers

    /// Decode RLE/bit-packed runs
    private func decodeRuns(data: Data, numValues: Int, bitWidth: Int) throws -> [UInt8] {
        var levels: [UInt8] = []
        levels.reserveCapacity(numValues)

        var offset = 0

        while levels.count < numValues {
            // Read varint run header
            guard offset < data.count else {
                throw LevelError.truncatedRuns
            }

            let (header, headerBytes) = try readVarint(data: data, offset: offset)
            offset += headerBytes

            let isLiteral = (header & 1) == 1

            if isLiteral {
                // Bit-packed run
                let numGroups = Int(header >> 1)
                let numValues = numGroups * 8  // Always in groups of 8

                let numBytes = (numValues * bitWidth + 7) / 8
                guard offset + numBytes <= data.count else {
                    throw LevelError.truncatedRuns
                }

                let packedData = data.subdata(in: offset..<(offset + numBytes))
                let values = try unpackBits(data: packedData, numValues: numValues, bitWidth: bitWidth)
                levels.append(contentsOf: values)

                offset += numBytes
            } else {
                // RLE run
                let count = Int(header >> 1)
                let valueBytes = (bitWidth + 7) / 8

                guard offset + valueBytes <= data.count else {
                    throw LevelError.truncatedRuns
                }

                // Read repeated value
                var value: UInt8 = 0
                if bitWidth > 0 {
                    value = data[offset]
                }

                levels.append(contentsOf: Array(repeating: value, count: count))
                offset += valueBytes
            }
        }

        // Trim to exact count (bit-packed runs may have padding)
        return Array(levels.prefix(numValues))
    }

    /// Read ULEB-128 varint
    private func readVarint(data: Data, offset: Int) throws -> (value: UInt32, bytes: Int) {
        var result: UInt32 = 0
        var shift: UInt32 = 0
        var bytesRead = 0

        while bytesRead < 5 {  // Max 5 bytes for UInt32
            guard offset + bytesRead < data.count else {
                throw LevelError.truncatedVarint
            }

            let byte = data[offset + bytesRead]
            bytesRead += 1

            result |= UInt32(byte & 0x7F) << shift

            if (byte & 0x80) == 0 {
                // High bit not set - this is the last byte
                return (result, bytesRead)
            }

            shift += 7
        }

        throw LevelError.varIntTooLong
    }

    /// Unpack bit-packed values
    private func unpackBits(data: Data, numValues: Int, bitWidth: Int) throws -> [UInt8] {
        guard bitWidth > 0 else {
            return Array(repeating: 0, count: numValues)
        }

        var values: [UInt8] = []
        values.reserveCapacity(numValues)

        var bitOffset = 0

        for _ in 0..<numValues {
            let byteOffset = bitOffset / 8
            let bitInByte = bitOffset % 8

            guard byteOffset < data.count else {
                throw LevelError.truncatedRuns
            }

            // Read value spanning up to 2 bytes
            var value: UInt16 = 0
            value |= UInt16(data[byteOffset])
            if byteOffset + 1 < data.count {
                value |= UInt16(data[byteOffset + 1]) << 8
            }

            // Extract bitWidth bits starting at bitInByte
            value = (value >> bitInByte) & ((1 << bitWidth) - 1)
            values.append(UInt8(value))

            bitOffset += bitWidth
        }

        return values
    }
}

// MARK: - Errors

/// Errors that can occur during level decoding
public enum LevelError: Error, Equatable {
    case missingLengthPrefix
    case invalidSize(expected: Int, got: Int)
    case truncatedRuns
    case truncatedVarint
    case varIntTooLong
}

extension LevelError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .missingLengthPrefix:
            return "Missing 4-byte length prefix"
        case .invalidSize(let expected, let got):
            return "Invalid size: expected \(expected) bytes, got \(got)"
        case .truncatedRuns:
            return "Truncated run data"
        case .truncatedVarint:
            return "Truncated varint"
        case .varIntTooLong:
            return "Varint exceeds maximum length"
        }
    }
}
