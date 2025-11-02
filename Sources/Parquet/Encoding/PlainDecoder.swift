// PLAIN encoding decoder
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Decoder for PLAIN encoding.
///
/// PLAIN is the simplest encoding where values are stored in their native format
/// with little-endian byte order.
///
/// # Format by Type
///
/// - **BOOLEAN**: Bit-packed, LSB first
/// - **INT32**: 4 bytes little-endian
/// - **INT64**: 8 bytes little-endian
/// - **INT96**: 12 bytes little-endian
/// - **FLOAT**: 4 bytes IEEE little-endian
/// - **DOUBLE**: 8 bytes IEEE little-endian
/// - **BYTE_ARRAY**: 4-byte length (little-endian) + raw bytes
/// - **FIXED_LEN_BYTE_ARRAY**: Raw bytes (length from schema)
///
/// # Usage
///
/// ```swift
/// let decoder = PlainDecoder.int32(data: encodedData)
/// let values = try decoder.decode(count: 100)
/// ```
public final class PlainDecoder<T> {
    /// The encoded data
    private let data: Data

    /// Current read position in the data
    private var offset: Int = 0

    /// Fixed length for FIXED_LEN_BYTE_ARRAY (nil for other types)
    private let fixedLength: Int?

    /// Initialize a PLAIN decoder
    ///
    /// - Parameters:
    ///   - data: The encoded data
    ///   - fixedLength: Fixed length for FIXED_LEN_BYTE_ARRAY types. Required when
    ///                  decoding FIXED_LEN_BYTE_ARRAY, must be omitted for BYTE_ARRAY.
    ///                  The data length should be a multiple of this fixed length.
    ///
    /// - Note: For FIXED_LEN_BYTE_ARRAY, each value is exactly `fixedLength` bytes
    ///         with no length prefix. For BYTE_ARRAY, each value has a 4-byte length
    ///         prefix followed by the data.
    public init(data: Data, fixedLength: Int? = nil) {
        self.data = data
        self.fixedLength = fixedLength
    }
}

// MARK: - Boolean Decoder

extension PlainDecoder where T == Bool {
    /// Current bit position within the current byte (0-7)
    private var bitOffset: Int {
        get { offset & 0x7 }
        set {
            // If we've moved to next byte, advance offset
            if newValue == 0 && bitOffset != 0 {
                offset = (offset & ~0x7) + 8
            }
        }
    }

    /// Decode a single boolean value
    ///
    /// Booleans are bit-packed with LSB first
    public func decodeOne() throws -> Bool {
        let byteIndex = offset >> 3 // offset / 8
        let bitIndex = offset & 0x7  // offset % 8

        guard byteIndex < data.count else {
            throw DecoderError.unexpectedEOF
        }

        let byte = data[byteIndex]
        let value = (byte & (1 << bitIndex)) != 0

        offset += 1 // Advance by 1 bit
        return value
    }

    /// Decode multiple boolean values
    public func decode(count: Int) throws -> [Bool] {
        let bitsNeeded = count
        let bytesNeeded = (bitsNeeded + 7) / 8 // Round up
        let byteIndex = offset >> 3

        guard byteIndex + bytesNeeded <= data.count else {
            throw DecoderError.insufficientData("Need \(bytesNeeded) bytes for \(count) booleans")
        }

        var values = [Bool]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - Int32 Decoder

extension PlainDecoder where T == Int32 {
    /// Decode a single Int32 value (4 bytes, little-endian)
    public func decodeOne() throws -> Int32 {
        guard offset + 4 <= data.count else {
            throw DecoderError.insufficientData("Need 4 bytes for Int32, have \(data.count - offset)")
        }

        let value = data.withUnsafeBytes { ptr in
            ptr.loadUnaligned(fromByteOffset: offset, as: Int32.self).littleEndian
        }

        offset += 4
        return value
    }

    /// Decode multiple Int32 values
    public func decode(count: Int) throws -> [Int32] {
        let bytesNeeded = count * 4
        guard offset + bytesNeeded <= data.count else {
            throw DecoderError.insufficientData("Need \(bytesNeeded) bytes, have \(data.count - offset)")
        }

        var values = [Int32]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - Int64 Decoder

extension PlainDecoder where T == Int64 {
    /// Decode a single Int64 value (8 bytes, little-endian)
    public func decodeOne() throws -> Int64 {
        guard offset + 8 <= data.count else {
            throw DecoderError.insufficientData("Need 8 bytes for Int64, have \(data.count - offset)")
        }

        let value = data.withUnsafeBytes { ptr in
            ptr.loadUnaligned(fromByteOffset: offset, as: Int64.self).littleEndian
        }

        offset += 8
        return value
    }

    /// Decode multiple Int64 values
    public func decode(count: Int) throws -> [Int64] {
        let bytesNeeded = count * 8
        guard offset + bytesNeeded <= data.count else {
            throw DecoderError.insufficientData("Need \(bytesNeeded) bytes, have \(data.count - offset)")
        }

        var values = [Int64]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - Int96 Decoder

/// Represents a 96-bit value (deprecated, used for legacy timestamps)
public struct Int96: Equatable, Hashable {
    /// Raw bytes (12 bytes, little-endian)
    public let bytes: Data

    public init(bytes: Data) {
        precondition(bytes.count == 12, "Int96 must be 12 bytes")
        self.bytes = bytes
    }
}

extension PlainDecoder where T == Int96 {
    /// Decode a single Int96 value (12 bytes, little-endian)
    public func decodeOne() throws -> Int96 {
        guard offset + 12 <= data.count else {
            throw DecoderError.insufficientData("Need 12 bytes for Int96, have \(data.count - offset)")
        }

        let bytes = data[offset..<(offset + 12)]
        offset += 12
        return Int96(bytes: Data(bytes))
    }

    /// Decode multiple Int96 values
    public func decode(count: Int) throws -> [Int96] {
        let bytesNeeded = count * 12
        guard offset + bytesNeeded <= data.count else {
            throw DecoderError.insufficientData("Need \(bytesNeeded) bytes, have \(data.count - offset)")
        }

        var values = [Int96]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - Float Decoder

extension PlainDecoder where T == Float {
    /// Decode a single Float value (4 bytes, IEEE little-endian)
    public func decodeOne() throws -> Float {
        guard offset + 4 <= data.count else {
            throw DecoderError.insufficientData("Need 4 bytes for Float, have \(data.count - offset)")
        }

        let value = data.withUnsafeBytes { ptr in
            // Read as UInt32 then bitcast to Float
            let bits = ptr.loadUnaligned(fromByteOffset: offset, as: UInt32.self).littleEndian
            return Float(bitPattern: bits)
        }

        offset += 4
        return value
    }

    /// Decode multiple Float values
    public func decode(count: Int) throws -> [Float] {
        let bytesNeeded = count * 4
        guard offset + bytesNeeded <= data.count else {
            throw DecoderError.insufficientData("Need \(bytesNeeded) bytes, have \(data.count - offset)")
        }

        var values = [Float]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - Double Decoder

extension PlainDecoder where T == Double {
    /// Decode a single Double value (8 bytes, IEEE little-endian)
    public func decodeOne() throws -> Double {
        guard offset + 8 <= data.count else {
            throw DecoderError.insufficientData("Need 8 bytes for Double, have \(data.count - offset)")
        }

        let value = data.withUnsafeBytes { ptr in
            // Read as UInt64 then bitcast to Double
            let bits = ptr.loadUnaligned(fromByteOffset: offset, as: UInt64.self).littleEndian
            return Double(bitPattern: bits)
        }

        offset += 8
        return value
    }

    /// Decode multiple Double values
    public func decode(count: Int) throws -> [Double] {
        let bytesNeeded = count * 8
        guard offset + bytesNeeded <= data.count else {
            throw DecoderError.insufficientData("Need \(bytesNeeded) bytes, have \(data.count - offset)")
        }

        var values = [Double]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - ByteArray Decoder

extension PlainDecoder where T == Data {
    /// Decode a single ByteArray value (4-byte length + data)
    ///
    /// For BYTE_ARRAY: reads 4-byte length then that many bytes
    /// For FIXED_LEN_BYTE_ARRAY: reads fixed length bytes
    public func decodeOne() throws -> Data {
        if let fixedLen = fixedLength {
            // FIXED_LEN_BYTE_ARRAY: just read the bytes
            guard offset + fixedLen <= data.count else {
                throw DecoderError.insufficientData("Need \(fixedLen) bytes for FIXED_LEN_BYTE_ARRAY, have \(data.count - offset)")
            }

            let bytes = data[offset..<(offset + fixedLen)]
            offset += fixedLen
            return Data(bytes)
        } else {
            // BYTE_ARRAY: read 4-byte length + data
            guard offset + 4 <= data.count else {
                throw DecoderError.insufficientData("Need 4 bytes for length, have \(data.count - offset)")
            }

            let lengthU32 = data.withUnsafeBytes { ptr in
                ptr.loadUnaligned(fromByteOffset: offset, as: UInt32.self).littleEndian
            }
            offset += 4

            // Check for valid length (max 2GB for safety)
            guard lengthU32 <= Int32.max else {
                throw DecoderError.invalidData("Byte array length too large: \(lengthU32)")
            }

            let length = Int(lengthU32)

            guard offset + length <= data.count else {
                throw DecoderError.insufficientData("Need \(length) bytes for array, have \(data.count - offset)")
            }

            let bytes = data[offset..<(offset + length)]
            offset += length
            return Data(bytes)
        }
    }

    /// Decode multiple ByteArray values
    public func decode(count: Int) throws -> [Data] {
        var values = [Data]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

// MARK: - String Decoder (convenience for UTF-8 byte arrays)

extension PlainDecoder where T == String {
    /// Decode a single String value (UTF-8 byte array)
    public func decodeOne() throws -> String {
        let byteDecoder = PlainDecoder<Data>(data: data, fixedLength: fixedLength)
        byteDecoder.offset = self.offset

        let bytes = try byteDecoder.decodeOne()
        self.offset = byteDecoder.offset

        guard let string = String(data: bytes, encoding: .utf8) else {
            throw DecoderError.invalidData("Invalid UTF-8 data")
        }

        return string
    }

    /// Decode multiple String values
    public func decode(count: Int) throws -> [String] {
        var values = [String]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}

