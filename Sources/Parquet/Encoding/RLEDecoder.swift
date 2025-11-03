// RLEDecoder - Hybrid RLE/Bit-Packing decoder for Parquet
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// RLE/Bit-Packing Hybrid decoder for Parquet dictionary indices
///
/// This decoder implements the hybrid RLE/bit-packing encoding used for:
/// - Dictionary indices in dictionary-encoded columns
/// - Definition and repetition levels (future use)
/// - Boolean values (future use)
///
/// # Data Page Format (Primary Use)
///
/// Dictionary index streams in **data pages** follow this format:
/// ```
/// <bit-width: 1 byte>
/// <runs... (rest of buffer)>
/// ```
///
/// **No length prefix** is present in data pages. Use `decodeIndices()` for this format.
///
/// # Legacy Format (Not Used in Data Pages)
///
/// Some contexts may use a length-prefixed format:
/// ```
/// <bit-width: 1 byte>
/// <length: 4 bytes little-endian>
/// <runs: exactly 'length' bytes>
/// ```
///
/// Use `decodeIndicesWithLengthPrefix()` for this format.
///
/// # Run Encoding
///
/// Each run is either:
/// - **Bit-packed**: Header `(count << 1) | 1`, followed by bit-packed values
///   - Values come in groups of 8, packed with minimal bit width
///   - Padding may exist at the end (trimmed to exact count)
/// - **RLE**: Header `(count << 1)`, followed by single repeated value
///
/// # Special Cases
///
/// - **Bit-width 0**: Single-value dictionary (all indices are 0)
/// - **Empty runs**: Legal when numValues matches the encoding
///
/// # Reference
///
/// Based on Apache Arrow/Parquet C++ implementation:
/// https://github.com/apache/arrow/blob/main/cpp/src/parquet/encoding.h
public final class RLEDecoder {

    public init() {}

    // MARK: - Public API

    /// Decode dictionary indices from encoded data (data page format)
    ///
    /// **Data Page Format** (no length prefix):
    /// ```
    /// <bit-width: 1 byte>
    /// <runs... (rest of buffer)>
    /// ```
    ///
    /// - Parameters:
    ///   - data: Complete encoded stream (bit-width + runs, NO length prefix)
    ///   - numValues: Exact number of indices to decode
    /// - Returns: Exactly `numValues` unsigned dictionary indices
    /// - Throws: `RLEError` if data is malformed or truncated
    public func decodeIndices(from data: Data, numValues: Int) throws -> [UInt32] {
        // Format: <bit-width: 1> <runs... (rest of buffer)>
        // NOTE: No 4-byte length prefix for dictionary indices in data pages!

        // 1. Extract bit-width (byte 0)
        guard data.count >= 1 else {
            throw RLEError.missingBitWidth
        }
        let bitWidth = Int(data[0])

        guard bitWidth <= 32 else {
            throw RLEError.invalidBitWidth(bitWidth)
        }

        // 2. Extract run data (everything after bit-width)
        let runData = data.subdata(in: 1..<data.count)

        // 3. Decode based on bit-width
        if bitWidth == 0 {
            // Single-value dictionary: all indices are 0
            return try decodeRunsZeroBitWidth(data: runData, numValues: numValues)
        } else {
            // Normal RLE/bit-packed decoding
            return try decodeRuns(
                bitWidth: bitWidth,
                data: runData,
                numValues: numValues
            )
        }
    }

    /// Decode dictionary indices from encoded data with length prefix (legacy format)
    ///
    /// **Legacy Format** (with length prefix):
    /// ```
    /// <bit-width: 1 byte>
    /// <length: 4 bytes LE>
    /// <runs: exactly 'length' bytes>
    /// ```
    ///
    /// This format is NOT used in modern Parquet data pages - use `decodeIndices()` instead.
    ///
    /// - Parameters:
    ///   - data: Complete encoded stream (bit-width + length + runs)
    ///   - numValues: Exact number of indices to decode
    /// - Returns: Exactly `numValues` unsigned dictionary indices
    /// - Throws: `RLEError` if data is malformed, truncated, or has wrong size
    public func decodeIndicesWithLengthPrefix(from data: Data, numValues: Int) throws -> [UInt32] {
        // Format: <bit-width: 1> <length: 4 LE> <runs>

        // 1. Extract bit-width (byte 0)
        guard data.count >= 1 else {
            throw RLEError.missingBitWidth
        }
        let bitWidth = Int(data[0])

        guard bitWidth <= 32 else {
            throw RLEError.invalidBitWidth(bitWidth)
        }

        // 2. Extract length prefix (bytes 1-4, little-endian)
        guard data.count >= 5 else {
            throw RLEError.missingLengthPrefix
        }

        // Read 4-byte length (little-endian) safely without alignment issues
        let runDataLength = Int(UInt32(data[1])
            | (UInt32(data[2]) << 8)
            | (UInt32(data[3]) << 16)
            | (UInt32(data[4]) << 24))

        // 3. Validate buffer size is EXACT (detects corruption/misalignment)
        let expectedSize = 5 + runDataLength
        guard data.count == expectedSize else {
            if data.count < expectedSize {
                throw RLEError.truncatedData(expected: expectedSize, got: data.count)
            } else {
                throw RLEError.extraneousData(expected: expectedSize, got: data.count)
            }
        }

        // 4. Extract run data (exactly runDataLength bytes)
        let runData = data.subdata(in: 5..<expectedSize)

        // 5. Decode based on bit-width
        if bitWidth == 0 {
            // Single-value dictionary: all indices are 0
            return try decodeRunsZeroBitWidth(data: runData, numValues: numValues)
        } else {
            // Normal RLE/bit-packed decoding
            return try decodeRuns(
                bitWidth: bitWidth,
                data: runData,
                numValues: numValues
            )
        }
    }

    // MARK: - Zero Bit-Width Decoding

    /// Decode runs with bit-width 0 (all values are 0)
    ///
    /// Even though all values are 0, we still parse the run structure
    /// to validate the encoding and detect corruption.
    private func decodeRunsZeroBitWidth(
        data: Data,
        numValues: Int
    ) throws -> [UInt32] {
        // Empty payload is legal (no runs needed for implicit zeros)
        if data.isEmpty {
            return Array(repeating: 0, count: numValues)
        }

        // Parse runs to validate structure
        var result: [UInt32] = []
        result.reserveCapacity(numValues)
        var offset = 0

        while result.count < numValues {
            guard offset < data.count else {
                throw RLEError.truncatedRuns(expected: numValues, got: result.count)
            }

            // Read run header (varint)
            let (header, headerBytes) = try readVarint(from: data, offset: offset)
            offset += headerBytes

            if header & 1 == 1 {
                // Bit-packed run: (header >> 1) groups of 8
                let groupCount = header >> 1

                // Check for overflow before converting to Int
                guard groupCount <= UInt64(Int.max / 8) else {
                    throw RLEError.invalidRunHeader("Bit-packed run group count too large: \(groupCount)")
                }

                let runLength = Int(groupCount * 8)
                let needed = min(runLength, numValues - result.count)
                result.append(contentsOf: repeatElement(0, count: needed))
            } else {
                // RLE run: (header >> 1) repetitions
                let count = header >> 1

                // Check for overflow before converting to Int
                guard count <= UInt64(Int.max) else {
                    throw RLEError.invalidRunHeader("RLE run length too large: \(count)")
                }

                let runLength = Int(count)
                let needed = min(runLength, numValues - result.count)
                result.append(contentsOf: repeatElement(0, count: needed))
            }
        }

        // Must decode exactly numValues
        guard result.count == numValues else {
            throw RLEError.valueMismatch(expected: numValues, got: result.count)
        }

        // Must consume EXACTLY all run data bytes
        guard offset == data.count else {
            throw RLEError.unconsumedData(
                expectedBytes: data.count,
                consumedBytes: offset
            )
        }

        return result
    }

    // MARK: - Normal Decoding (Bit-Width 1-32)

    /// Decode runs with normal bit-width (1-32)
    private func decodeRuns(
        bitWidth: Int,
        data: Data,
        numValues: Int
    ) throws -> [UInt32] {
        var result: [UInt32] = []
        result.reserveCapacity(numValues)
        var offset = 0

        while result.count < numValues {
            guard offset < data.count else {
                throw RLEError.truncatedRuns(expected: numValues, got: result.count)
            }

            // Read run header (varint)
            let (header, headerBytes) = try readVarint(from: data, offset: offset)
            offset += headerBytes

            if header & 1 == 1 {
                // Bit-packed run: (header >> 1) groups of 8 values
                let rawGroupCount = header >> 1

                // Check for overflow before converting to Int and multiplying by 8
                // We need groupCount * 8 to fit in Int
                guard rawGroupCount <= UInt64(Int.max / 8) else {
                    throw RLEError.invalidRunHeader("Bit-packed run group count too large: \(rawGroupCount)")
                }

                let groupCount = Int(rawGroupCount)
                let values = try decodeBitPackedRun(
                    from: data,
                    offset: &offset,
                    bitWidth: bitWidth,
                    groupCount: groupCount
                )

                // Trim padding (groups of 8 may exceed numValues)
                let needed = min(values.count, numValues - result.count)
                result.append(contentsOf: values.prefix(needed))

            } else {
                // RLE run: (header >> 1) repetitions of a single value
                let rawRunLength = header >> 1

                // Check for overflow before converting to Int
                guard rawRunLength <= UInt64(Int.max) else {
                    throw RLEError.invalidRunHeader("RLE run length too large: \(rawRunLength)")
                }
                guard rawRunLength > 0 else {
                    throw RLEError.invalidRunHeader("RLE run length is zero")
                }

                let runLength = Int(rawRunLength)

                let value = try readValue(
                    from: data,
                    offset: &offset,
                    bitWidth: bitWidth
                )

                let needed = min(runLength, numValues - result.count)
                result.append(contentsOf: repeatElement(value, count: needed))
            }
        }

        // Must decode exactly numValues
        guard result.count == numValues else {
            throw RLEError.valueMismatch(expected: numValues, got: result.count)
        }

        // Must consume EXACTLY all run data bytes
        guard offset == data.count else {
            throw RLEError.unconsumedData(
                expectedBytes: data.count,
                consumedBytes: offset
            )
        }

        return result
    }

    // MARK: - Helper Methods

    /// Read a variable-length integer (varint)
    ///
    /// Varints encode integers using 7 bits per byte, with the MSB
    /// indicating continuation. Used for run headers.
    private func readVarint(from data: Data, offset: Int) throws -> (value: UInt64, bytes: Int) {
        var result: UInt64 = 0
        var shift: UInt64 = 0
        var bytesRead = 0

        while true {
            guard offset + bytesRead < data.count else {
                throw RLEError.malformedVarint("Unexpected end of data")
            }

            let byte = data[offset + bytesRead]
            bytesRead += 1

            result |= UInt64(byte & 0x7F) << shift

            // MSB clear means this is the last byte
            if (byte & 0x80) == 0 {
                break
            }

            shift += 7
            guard shift < 64 else {
                throw RLEError.malformedVarint("Varint too long")
            }
        }

        return (result, bytesRead)
    }

    /// Decode a bit-packed run
    ///
    /// Bit-packed runs store values with minimal bit-width, packed into bytes.
    /// Values come in groups of 8 for alignment.
    ///
    /// - Parameters:
    ///   - data: Source data
    ///   - offset: Current offset (will be advanced)
    ///   - bitWidth: Bits per value (1-32)
    ///   - groupCount: Number of 8-value groups
    /// - Returns: Decoded values (groupCount × 8 values)
    private func decodeBitPackedRun(
        from data: Data,
        offset: inout Int,
        bitWidth: Int,
        groupCount: Int
    ) throws -> [UInt32] {
        let valuesPerGroup = 8
        let totalValues = groupCount * valuesPerGroup
        let bytesPerGroup = (bitWidth * valuesPerGroup + 7) / 8

        // Check for overflow before multiplying groupCount * bytesPerGroup
        // When bitWidth = 32, bytesPerGroup = 32, so this can overflow even
        // though groupCount was bounded to Int.max / 8
        guard groupCount <= Int.max / bytesPerGroup else {
            throw RLEError.invalidRunHeader("Bit-packed run would require too many bytes")
        }

        let totalBytes = groupCount * bytesPerGroup

        guard offset + totalBytes <= data.count else {
            throw RLEError.truncatedRuns(
                expected: totalBytes,
                got: data.count - offset
            )
        }

        var result: [UInt32] = []
        result.reserveCapacity(totalValues)

        // Decode each group of 8 values
        for _ in 0..<groupCount {
            let groupValues = try decodeBitPackedGroup(
                from: data,
                offset: &offset,
                bitWidth: bitWidth
            )
            result.append(contentsOf: groupValues)
        }

        return result
    }

    /// Decode one bit-packed group (8 values)
    private func decodeBitPackedGroup(
        from data: Data,
        offset: inout Int,
        bitWidth: Int
    ) throws -> [UInt32] {
        let valuesPerGroup = 8
        var values: [UInt32] = []
        values.reserveCapacity(valuesPerGroup)

        var bitBuffer: UInt64 = 0
        var bitsInBuffer = 0

        for _ in 0..<valuesPerGroup {
            // Ensure we have enough bits in the buffer
            while bitsInBuffer < bitWidth {
                guard offset < data.count else {
                    throw RLEError.truncatedRuns(expected: 1, got: 0)
                }
                let byte = data[offset]
                offset += 1

                bitBuffer |= UInt64(byte) << bitsInBuffer
                bitsInBuffer += 8
            }

            // Extract value (bitWidth bits)
            let mask = (UInt64(1) << bitWidth) - 1
            let value = UInt32(bitBuffer & mask)
            values.append(value)

            // Shift buffer
            bitBuffer >>= bitWidth
            bitsInBuffer -= bitWidth
        }

        return values
    }

    /// Read a single value with given bit-width
    ///
    /// Used for RLE runs (repeated value) and single values.
    /// Rounded up to next byte boundary.
    private func readValue(
        from data: Data,
        offset: inout Int,
        bitWidth: Int
    ) throws -> UInt32 {
        // Round up to bytes
        let bytes = (bitWidth + 7) / 8

        guard offset + bytes <= data.count else {
            throw RLEError.truncatedRuns(expected: bytes, got: data.count - offset)
        }

        var value: UInt32 = 0
        for i in 0..<bytes {
            value |= UInt32(data[offset + i]) << (i * 8)
        }
        offset += bytes

        // Mask to bit-width (value may have extra bits from padding)
        if bitWidth < 32 {
            let mask = (UInt32(1) << bitWidth) - 1
            value &= mask
        }

        return value
    }
}

// MARK: - Errors

/// Errors that can occur during RLE decoding
public enum RLEError: Error, Equatable {
    /// Bit-width byte is missing
    case missingBitWidth

    /// Length prefix (4 bytes) is missing
    case missingLengthPrefix

    /// Bit-width is out of valid range (0-32)
    case invalidBitWidth(Int)

    /// Buffer is too short (expected vs. actual size)
    case truncatedData(expected: Int, got: Int)

    /// Buffer has extra bytes beyond declared length
    case extraneousData(expected: Int, got: Int)

    /// Run data ended before decoding numValues indices
    case truncatedRuns(expected: Int, got: Int)

    /// Decoded wrong number of values (should match numValues exactly)
    case valueMismatch(expected: Int, got: Int)

    /// Not all run data bytes were consumed
    case unconsumedData(expectedBytes: Int, consumedBytes: Int)

    /// Invalid run header
    case invalidRunHeader(String)

    /// Malformed varint encoding
    case malformedVarint(String)
}

extension RLEError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .missingBitWidth:
            return "RLE: Missing bit-width byte"
        case .missingLengthPrefix:
            return "RLE: Missing 4-byte length prefix"
        case .invalidBitWidth(let width):
            return "RLE: Invalid bit-width \(width) (must be 0-32)"
        case .truncatedData(let expected, let got):
            return "RLE: Buffer truncated (expected \(expected) bytes, got \(got))"
        case .extraneousData(let expected, let got):
            return "RLE: Buffer has extraneous data (expected \(expected) bytes, got \(got))"
        case .truncatedRuns(let expected, let got):
            return "RLE: Runs truncated (expected \(expected) values, got \(got))"
        case .valueMismatch(let expected, let got):
            return "RLE: Value count mismatch (expected \(expected), got \(got))"
        case .unconsumedData(let expectedBytes, let consumedBytes):
            return "RLE: Unconsumed data (\(expectedBytes) bytes declared, only \(consumedBytes) consumed)"
        case .invalidRunHeader(let msg):
            return "RLE: Invalid run header: \(msg)"
        case .malformedVarint(let msg):
            return "RLE: Malformed varint: \(msg)"
        }
    }
}
