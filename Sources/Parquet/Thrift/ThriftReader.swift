// ThriftReader - Compact Binary Protocol deserializer for Parquet
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Errors that can occur during Thrift deserialization.
public enum ThriftError: Error {
    case invalidData(String)
    case unexpectedEndOfData
    case unsupportedType(String)
    case protocolError(String)
}

/// Thrift data types in Compact Binary Protocol.
enum ThriftCompactType: UInt8 {
    case stop = 0x00
    case boolTrue = 0x01
    case boolFalse = 0x02
    case byte = 0x03
    case i16 = 0x04
    case i32 = 0x05
    case i64 = 0x06
    case double = 0x07
    case binary = 0x08
    case list = 0x09
    case set = 0x0A
    case map = 0x0B
    case `struct` = 0x0C
}

/// Reader for Thrift Compact Binary Protocol.
///
/// Implements deserialization of Parquet metadata using Thrift's Compact Binary Protocol.
/// This is a read-only implementation focused on the structures needed for Parquet Phase 1.
public final class ThriftReader {
    private var data: Data
    private var position: Int

    /// Creates a new reader from Data.
    public init(data: Data) {
        self.data = data
        self.position = 0
    }

    /// Returns the current position in the data.
    public var currentPosition: Int {
        return position
    }

    /// Returns the number of bytes remaining.
    public var bytesRemaining: Int {
        return data.count - position
    }

    // MARK: - Basic Type Reading

    /// Reads a single byte.
    func readByte() throws -> UInt8 {
        guard position < data.count else {
            throw ThriftError.unexpectedEndOfData
        }
        let byte = data[position]
        position += 1
        return byte
    }

    /// Reads multiple bytes into Data.
    func readBytes(count: Int) throws -> Data {
        guard position + count <= data.count else {
            throw ThriftError.unexpectedEndOfData
        }
        let bytes = data.subdata(in: position..<(position + count))
        position += count
        return bytes
    }

    /// Reads an unsigned variable-length integer.
    /// Used for: collection sizes, binary lengths, field IDs.
    func readUnsignedVarint() throws -> UInt64 {
        var result: UInt64 = 0
        var shift: UInt64 = 0

        while true {
            let byte = try readByte()
            result |= UInt64(byte & 0x7F) << shift

            if (byte & 0x80) == 0 {
                break
            }

            shift += 7
            guard shift < 64 else {
                throw ThriftError.protocolError("Varint too long")
            }
        }

        return result
    }

    /// Reads a signed variable-length integer (zigzag encoded).
    /// Used for: i16, i32, i64 field types.
    func readVarint() throws -> Int64 {
        let unsignedValue = try readUnsignedVarint()

        // Zigzag decode
        return Int64(unsignedValue >> 1) ^ -(Int64(unsignedValue & 1))
    }

    /// Reads a 32-bit integer (zigzag encoded).
    func readVarint32() throws -> Int32 {
        let value = try readVarint()
        guard value >= Int64(Int32.min) && value <= Int64(Int32.max) else {
            throw ThriftError.protocolError("Varint32 out of range")
        }
        return Int32(value)
    }

    /// Reads a 16-bit integer (zigzag encoded).
    func readVarint16() throws -> Int16 {
        let value = try readVarint()
        guard value >= Int64(Int16.min) && value <= Int64(Int16.max) else {
            throw ThriftError.protocolError("Varint16 out of range: \(value)")
        }
        return Int16(value)
    }

    /// Reads an 8-bit integer.
    func readI8() throws -> Int8 {
        let byte = try readByte()
        return Int8(bitPattern: byte)
    }

    /// Reads a double (8 bytes, little-endian).
    func readDouble() throws -> Double {
        let bytes = try readBytes(count: 8)
        let value = bytes.withUnsafeBytes { $0.load(as: UInt64.self) }
        return Double(bitPattern: value.littleEndian)
    }

    /// Reads a binary field (length-prefixed bytes).
    func readBinary() throws -> Data {
        let length = try readUnsignedVarint()
        guard length <= Int.max else {
            throw ThriftError.protocolError("Binary length too large")
        }
        return try readBytes(count: Int(length))
    }

    /// Reads a string (UTF-8 encoded binary).
    func readString() throws -> String {
        let binary = try readBinary()
        guard let string = String(data: binary, encoding: .utf8) else {
            throw ThriftError.invalidData("Invalid UTF-8 string")
        }
        return string
    }

    // MARK: - Field Reading

    /// Reads a field header and returns the field ID and type.
    ///
    /// Returns nil if STOP field is encountered.
    func readFieldHeader(lastFieldId: inout Int16) throws -> (fieldId: Int16, type: ThriftCompactType)? {
        let byte = try readByte()

        // Check for STOP
        if byte == 0 {
            return nil
        }

        // Extract type (lower 4 bits) and delta (upper 4 bits)
        let typeBits = byte & 0x0F
        let delta = Int16(byte >> 4)

        // Calculate field ID first (before type validation)
        let fieldId: Int16
        if delta == 0 {
            // Read zigzag varint for field ID
            fieldId = try readVarint16()
        } else {
            // Field ID is lastFieldId + delta
            fieldId = lastFieldId + delta
        }

        lastFieldId = fieldId

        // Try to map to known type, or use a sentinel for unknown types
        guard let type = ThriftCompactType(rawValue: typeBits) else {
            // Unknown type - we'll treat it as a field to skip
            // This can happen with newer Thrift versions or extensions
            throw ThriftError.unsupportedType("Unknown field type: \(typeBits) for field \(fieldId)")
        }

        return (fieldId: fieldId, type: type)
    }

    /// Skips a field of the given type.
    func skipField(type: ThriftCompactType) throws {
        switch type {
        case .stop:
            break
        case .boolTrue, .boolFalse:
            break
        case .byte:
            _ = try readByte()
        case .i16:
            _ = try readVarint16()
        case .i32:
            _ = try readVarint32()
        case .i64:
            _ = try readVarint()
        case .double:
            _ = try readDouble()
        case .binary:
            _ = try readBinary()
        case .list, .set:
            try skipList()
        case .map:
            try skipMap()
        case .struct:
            try skipStruct()
        }
    }

    /// Skips a list/set.
    private func skipList() throws {
        let sizeAndType = try readByte()
        let size: Int

        if (sizeAndType >> 4) == 0x0F {
            // Size is unsigned varint
            size = Int(try readUnsignedVarint())
        } else {
            // Size is in upper 4 bits
            size = Int(sizeAndType >> 4)
        }

        let typeBits = sizeAndType & 0x0F
        guard let elementType = ThriftCompactType(rawValue: typeBits) else {
            throw ThriftError.unsupportedType("Unknown list element type: \(typeBits)")
        }

        for _ in 0..<size {
            try skipField(type: elementType)
        }
    }

    /// Skips a map.
    private func skipMap() throws {
        let size = Int(try readUnsignedVarint())
        if size == 0 {
            return
        }

        let keyAndValueType = try readByte()
        let keyTypeBits = keyAndValueType >> 4
        let valueTypeBits = keyAndValueType & 0x0F

        guard let keyType = ThriftCompactType(rawValue: keyTypeBits),
              let valueType = ThriftCompactType(rawValue: valueTypeBits) else {
            throw ThriftError.unsupportedType("Unknown map key/value type")
        }

        for _ in 0..<size {
            try skipField(type: keyType)
            try skipField(type: valueType)
        }
    }

    /// Skips a struct.
    func skipStruct() throws {
        var lastFieldId: Int16 = 0
        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            try skipField(type: field.type)
        }
    }

    // MARK: - Collection Reading Helpers

    /// Reads a list header and returns the element type and count.
    func readListHeader() throws -> (elementType: ThriftCompactType, count: Int) {
        let sizeAndType = try readByte()
        let size: Int

        if (sizeAndType >> 4) == 0x0F {
            // Size is unsigned varint
            size = Int(try readUnsignedVarint())
        } else {
            // Size is in upper 4 bits
            size = Int(sizeAndType >> 4)
        }

        let typeBits = sizeAndType & 0x0F
        guard let elementType = ThriftCompactType(rawValue: typeBits) else {
            throw ThriftError.unsupportedType("Unknown list element type: \(typeBits)")
        }

        return (elementType: elementType, count: size)
    }
}
