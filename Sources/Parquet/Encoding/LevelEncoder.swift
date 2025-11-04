// LevelEncoder - Encode definition and repetition levels
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Encodes definition and repetition levels using RLE/bit-packed hybrid encoding
///
/// # Format (Data Page V1)
///
/// ```
/// <length: 4 bytes LE> <RLE/bit-packed runs>
/// ```
///
/// Each run is either:
/// - **RLE run**: Header `varint(count << 1)`, followed by repeated value
/// - **Bit-packed run**: Header `varint((numGroups << 1) | 1)`, followed by bit-packed values
///
/// # Usage
///
/// ```swift
/// var encoder = LevelEncoder(maxLevel: 1)  // maxLevel 1 for optional columns
/// encoder.encode([1, 1, 0, 1, 1])  // 1 = present, 0 = null
/// let data = encoder.flush()
/// ```
///
/// # Relationship to RLEEncoder
///
/// LevelEncoder wraps RLEEncoder and adds the 4-byte length prefix required
/// for definition/repetition levels in data pages.
public final class LevelEncoder {
    private let maxLevel: Int
    private let bitWidth: Int
    private var rleEncoder: RLEEncoder

    /// Initialize level encoder
    /// - Parameter maxLevel: Maximum level value (determines bit-width)
    public init(maxLevel: Int) {
        self.maxLevel = maxLevel
        self.bitWidth = maxLevel == 0 ? 0 : (64 - maxLevel.leadingZeroBitCount)
        self.rleEncoder = RLEEncoder(bitWidth: bitWidth)
    }

    /// Encode a sequence of level values
    /// - Parameter levels: Array of level values (0 to maxLevel)
    public func encode(_ levels: [UInt16]) {
        let values = levels.map { UInt32($0) }
        rleEncoder.encode(values)
    }

    /// Encode a single level value
    /// - Parameter level: Level value (0 to maxLevel)
    public func encodeOne(_ level: UInt16) {
        rleEncoder.encodeOne(UInt32(level))
    }

    /// Flush and return encoded levels with 4-byte length prefix
    /// - Returns: Complete level data (4-byte length + RLE-encoded runs)
    public func flush() -> Data {
        var rleData = rleEncoder.flush()

        // Remove bit-width byte from RLE encoder output
        // (RLEEncoder includes bit-width as first byte, but for levels it's implicit from maxLevel)
        if !rleData.isEmpty {
            rleData = rleData.dropFirst()  // Remove bit-width byte
        }

        // Build final output: 4-byte length + runs
        var result = Data()
        result.reserveCapacity(4 + rleData.count)

        // Write 4-byte length (little-endian)
        let length = UInt32(rleData.count)
        result.append(UInt8(length & 0xFF))
        result.append(UInt8((length >> 8) & 0xFF))
        result.append(UInt8((length >> 16) & 0xFF))
        result.append(UInt8((length >> 24) & 0xFF))

        // Append run data
        result.append(rleData)

        return result
    }
}
