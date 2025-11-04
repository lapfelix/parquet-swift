// PLAIN encoding encoder
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Encoder for PLAIN encoding.
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
/// var encoder = PlainEncoder.int32()
/// try encoder.encode([1, 2, 3, 4, 5])
/// let data = encoder.data
/// ```
public final class PlainEncoder<T> {
    /// The encoded data buffer
    private(set) var data: Data

    /// Fixed length for FIXED_LEN_BYTE_ARRAY (nil for other types)
    private let fixedLength: Int?

    /// Track bits written for boolean encoding (only used when T == Bool)
    private var bitsWritten: Int = 0

    /// Initialize a PLAIN encoder
    ///
    /// - Parameter fixedLength: Fixed length for FIXED_LEN_BYTE_ARRAY types
    public init(fixedLength: Int? = nil) {
        self.data = Data()
        self.fixedLength = fixedLength
    }
}

// MARK: - Boolean Encoder

extension PlainEncoder where T == Bool {
    /// Encode a single boolean value
    ///
    /// Booleans are bit-packed with LSB first
    public func encodeOne(_ value: Bool) {
        let bitIndex = bitsWritten % 8

        // If starting a new byte, append it
        if bitIndex == 0 {
            data.append(0)
        }

        // Set the bit in the last byte
        if value {
            let byteIndex = data.count - 1
            data[byteIndex] |= (1 << bitIndex)
        }

        bitsWritten += 1
    }

    /// Encode multiple boolean values
    public func encode(_ values: [Bool]) {
        for value in values {
            encodeOne(value)
        }
    }
}

// MARK: - Int32 Encoder

extension PlainEncoder where T == Int32 {
    /// Encode a single Int32 value (4 bytes, little-endian)
    public func encodeOne(_ value: Int32) {
        var leValue = value.littleEndian
        withUnsafeBytes(of: &leValue) { ptr in
            data.append(contentsOf: ptr)
        }
    }

    /// Encode multiple Int32 values
    public func encode(_ values: [Int32]) {
        data.reserveCapacity(data.count + values.count * 4)
        for value in values {
            encodeOne(value)
        }
    }
}

// MARK: - Int64 Encoder

extension PlainEncoder where T == Int64 {
    /// Encode a single Int64 value (8 bytes, little-endian)
    public func encodeOne(_ value: Int64) {
        var leValue = value.littleEndian
        withUnsafeBytes(of: &leValue) { ptr in
            data.append(contentsOf: ptr)
        }
    }

    /// Encode multiple Int64 values
    public func encode(_ values: [Int64]) {
        data.reserveCapacity(data.count + values.count * 8)
        for value in values {
            encodeOne(value)
        }
    }
}

// MARK: - Int96 Encoder

extension PlainEncoder where T == Int96 {
    /// Encode a single Int96 value (12 bytes, little-endian)
    public func encodeOne(_ value: Int96) {
        data.append(value.bytes)
    }

    /// Encode multiple Int96 values
    public func encode(_ values: [Int96]) {
        data.reserveCapacity(data.count + values.count * 12)
        for value in values {
            encodeOne(value)
        }
    }
}

// MARK: - Float Encoder

extension PlainEncoder where T == Float {
    /// Encode a single Float value (4 bytes, IEEE little-endian)
    public func encodeOne(_ value: Float) {
        var bits = value.bitPattern.littleEndian
        withUnsafeBytes(of: &bits) { ptr in
            data.append(contentsOf: ptr)
        }
    }

    /// Encode multiple Float values
    public func encode(_ values: [Float]) {
        data.reserveCapacity(data.count + values.count * 4)
        for value in values {
            encodeOne(value)
        }
    }
}

// MARK: - Double Encoder

extension PlainEncoder where T == Double {
    /// Encode a single Double value (8 bytes, IEEE little-endian)
    public func encodeOne(_ value: Double) {
        var bits = value.bitPattern.littleEndian
        withUnsafeBytes(of: &bits) { ptr in
            data.append(contentsOf: ptr)
        }
    }

    /// Encode multiple Double values
    public func encode(_ values: [Double]) {
        data.reserveCapacity(data.count + values.count * 8)
        for value in values {
            encodeOne(value)
        }
    }
}

// MARK: - ByteArray Encoder

extension PlainEncoder where T == Data {
    /// Encode a single ByteArray value (4-byte length + data)
    ///
    /// For BYTE_ARRAY: writes 4-byte length then data
    /// For FIXED_LEN_BYTE_ARRAY: writes fixed length bytes only
    public func encodeOne(_ value: Data) throws {
        if let fixedLen = fixedLength {
            // FIXED_LEN_BYTE_ARRAY: just write the bytes
            guard value.count == fixedLen else {
                throw EncoderError.valueSizeMismatch(expected: fixedLen, actual: value.count)
            }
            data.append(value)
        } else {
            // BYTE_ARRAY: write 4-byte length + data
            guard value.count <= Int32.max else {
                throw EncoderError.invalidData("Byte array too large: \(value.count)")
            }

            var length = UInt32(value.count).littleEndian
            withUnsafeBytes(of: &length) { ptr in
                data.append(contentsOf: ptr)
            }
            data.append(value)
        }
    }

    /// Encode multiple ByteArray values
    public func encode(_ values: [Data]) throws {
        for value in values {
            try encodeOne(value)
        }
    }
}

// MARK: - String Encoder (convenience for UTF-8 byte arrays)

extension PlainEncoder where T == String {
    /// Encode a single String value (UTF-8 byte array)
    public func encodeOne(_ value: String) throws {
        let bytes = Data(value.utf8)

        guard bytes.count <= Int32.max else {
            throw EncoderError.invalidData("String too large: \(bytes.count)")
        }

        // Write 4-byte length + UTF-8 bytes
        var length = UInt32(bytes.count).littleEndian
        withUnsafeBytes(of: &length) { ptr in
            data.append(contentsOf: ptr)
        }
        data.append(bytes)
    }

    /// Encode multiple String values
    public func encode(_ values: [String]) throws {
        for value in values {
            try encodeOne(value)
        }
    }
}

// MARK: - Encoder Errors

enum EncoderError: Error, LocalizedError {
    case valueSizeMismatch(expected: Int, actual: Int)
    case invalidData(String)

    var errorDescription: String? {
        switch self {
        case .valueSizeMismatch(let expected, let actual):
            return "Value size mismatch: expected \(expected), got \(actual)"
        case .invalidData(let message):
            return "Invalid data: \(message)"
        }
    }
}
