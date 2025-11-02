// Decoder protocol for value decoding
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Protocol for decoding Parquet values from encoded data.
///
/// Decoders handle the conversion from encoded byte representation
/// to Swift value types. Different encodings (PLAIN, RLE, DELTA, etc.)
/// implement this protocol.
///
/// # Usage
///
/// ```swift
/// let decoder = PlainDecoder<Int32>(data: encodedData)
/// let values = try decoder.decode(count: 100)
/// ```
///
/// # Thread Safety
///
/// Decoders are not thread-safe. Each decoder maintains internal state
/// and should only be used from a single thread.
public protocol Decoder {
    /// The physical type this decoder handles
    associatedtype PhysicalType

    /// The Swift type that values decode to
    associatedtype ValueType

    /// Initialize a decoder with encoded data
    ///
    /// - Parameter data: The encoded data to decode
    init(data: Data)

    /// Decode a single value
    ///
    /// - Returns: The decoded value
    /// - Throws: `DecoderError` if decoding fails or no more data available
    func decodeOne() throws -> ValueType

    /// Decode multiple values
    ///
    /// - Parameter count: Number of values to decode
    /// - Returns: Array of decoded values
    /// - Throws: `DecoderError` if decoding fails or insufficient data
    func decode(count: Int) throws -> [ValueType]

    /// Number of values remaining in the encoded data
    ///
    /// Returns `nil` if the count cannot be determined (e.g., for variable-length types)
    var valuesRemaining: Int? { get }
}

/// Errors that can occur during decoding
public enum DecoderError: Error, Equatable {
    /// Not enough data to decode the requested number of values
    case insufficientData(String)

    /// Invalid encoded data format
    case invalidData(String)

    /// Unsupported encoding for this type
    case unsupportedEncoding(String)

    /// Unexpected end of data
    case unexpectedEOF
}

extension DecoderError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .insufficientData(let msg):
            return "Insufficient data: \(msg)"
        case .invalidData(let msg):
            return "Invalid data: \(msg)"
        case .unsupportedEncoding(let msg):
            return "Unsupported encoding: \(msg)"
        case .unexpectedEOF:
            return "Unexpected end of file"
        }
    }
}

// MARK: - Default Implementations

extension Decoder {
    /// Default implementation: decode values one at a time
    public func decode(count: Int) throws -> [ValueType] {
        var values = [ValueType]()
        values.reserveCapacity(count)

        for _ in 0..<count {
            values.append(try decodeOne())
        }

        return values
    }
}
