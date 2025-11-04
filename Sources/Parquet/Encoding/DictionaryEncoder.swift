// DictionaryEncoder - Dictionary encoding with fallback to PLAIN
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Dictionary encoder that builds a dictionary and encodes values as indices
///
/// # Strategy
///
/// 1. **Dictionary Building**: Track unique values and assign sequential indices
/// 2. **Index Encoding**: Use RLE/bit-packing hybrid for indices
/// 3. **Fallback**: Switch to PLAIN encoding if dictionary exceeds threshold
///
/// # Usage
///
/// ```swift
/// var encoder = DictionaryEncoder<String>()
/// try encoder.encode(["foo", "bar", "foo", "baz"])
///
/// if encoder.shouldUseDictionary {
///     let dictData = try encoder.dictionaryData()  // PLAIN-encoded dictionary
///     let indicesData = encoder.indicesData()       // RLE-encoded indices
/// } else {
///     let plainData = try encoder.fallbackToPlain()
/// }
/// ```
public final class DictionaryEncoder<T: Hashable> {
    // Configuration
    private let maxDictionarySize: Int
    private let maxDictionaryBytes: Int

    // Dictionary state
    private var dictionary: [T] = []              // Index → Value
    private var valueToIndex: [T: Int] = [:]      // Value → Index
    private var indices: [UInt32] = []            // Current page indices (cleared after flush)
    private var hasFallenBack: Bool = false

    // Statistics
    private var dictionaryByteSize: Int = 0
    private var totalValueCount: Int = 0          // Total values encoded across all pages

    /// Initialize dictionary encoder
    /// - Parameters:
    ///   - maxDictionarySize: Maximum number of unique values (default: 10000)
    ///   - maxDictionaryBytes: Maximum dictionary size in bytes (default: 1MB)
    public init(maxDictionarySize: Int = 10000, maxDictionaryBytes: Int = 1024 * 1024) {
        self.maxDictionarySize = maxDictionarySize
        self.maxDictionaryBytes = maxDictionaryBytes
    }

    /// Whether dictionary encoding should be used
    public var shouldUseDictionary: Bool {
        !hasFallenBack && !dictionary.isEmpty
    }

    /// Number of unique values in dictionary
    public var dictionaryCount: Int {
        dictionary.count
    }

    /// Number of values encoded (total across all pages)
    public var valueCount: Int {
        totalValueCount
    }

    /// Encode a batch of values
    /// - Parameter values: Values to encode
    /// - Throws: EncoderError if encoding fails
    public func encode(_ values: [T]) throws {
        for value in values {
            try encodeOne(value)
        }
    }

    /// Encode a single value
    /// - Parameter value: Value to encode
    /// - Throws: EncoderError if encoding fails
    public func encodeOne(_ value: T) throws {
        totalValueCount += 1

        guard !hasFallenBack else {
            // Already fallen back, no need to track indices
            return
        }

        // Check if value exists in dictionary
        if let index = valueToIndex[value] {
            indices.append(UInt32(index))
            return
        }

        // New value - add to dictionary
        let newIndex = dictionary.count
        let estimatedSize = estimateValueSize(value)

        // Check if adding this would exceed limits
        if newIndex >= maxDictionarySize || dictionaryByteSize + estimatedSize > maxDictionaryBytes {
            // Fall back to PLAIN encoding
            hasFallenBack = true
            indices.removeAll()  // Clear indices as they won't be used
            return
        }

        // Add to dictionary
        dictionary.append(value)
        valueToIndex[value] = newIndex
        dictionaryByteSize += estimatedSize
        indices.append(UInt32(newIndex))
    }

    // MARK: - Output Methods

    /// Get RLE-encoded indices data for current page
    /// - Returns: RLE-encoded indices for values since last clearPageIndices()
    /// - Note: Does NOT clear indices; caller must call clearPageIndices() after successful flush
    public func indicesData() -> Data {
        guard !hasFallenBack else { return Data() }

        let bitWidth = calculateBitWidth(maxValue: UInt32(dictionary.count - 1))
        let encoder = RLEEncoder(bitWidth: bitWidth)
        encoder.encode(indices)
        return encoder.flush()
    }

    /// Clear page indices after successful flush
    /// - Note: Dictionary and totalValueCount are preserved across pages
    public func clearPageIndices() {
        indices.removeAll(keepingCapacity: true)
    }

    /// Get dictionary values for dictionary page
    /// - Returns: Array of dictionary values in index order
    public func dictionaryValues() -> [T] {
        dictionary
    }

    // MARK: - Private Methods

    private func calculateBitWidth(maxValue: UInt32) -> Int {
        guard maxValue > 0 else { return 0 }
        return Int(32 - maxValue.leadingZeroBitCount)
    }

    private func estimateValueSize(_ value: T) -> Int {
        // Conservative estimate
        if let str = value as? String {
            return 4 + str.utf8.count  // Length + data
        } else if value is Int32 {
            return 4
        } else if value is Int64 {
            return 8
        } else if value is Float {
            return 4
        } else if value is Double {
            return 8
        } else if let data = value as? Data {
            return 4 + data.count
        }
        return 16  // Default estimate
    }
}

// MARK: - String Specialization

extension DictionaryEncoder where T == String {
    /// Get PLAIN-encoded dictionary data for String dictionary
    /// - Returns: PLAIN-encoded dictionary values
    /// - Throws: EncoderError if encoding fails
    public func dictionaryData() throws -> Data {
        let encoder = PlainEncoder<String>()
        try encoder.encode(dictionary)
        return encoder.data
    }
}

// MARK: - Int32 Specialization

extension DictionaryEncoder where T == Int32 {
    /// Get PLAIN-encoded dictionary data for Int32 dictionary
    /// - Returns: PLAIN-encoded dictionary values
    public func dictionaryData() -> Data {
        let encoder = PlainEncoder<Int32>()
        encoder.encode(dictionary)
        return encoder.data
    }
}

// MARK: - Int64 Specialization

extension DictionaryEncoder where T == Int64 {
    /// Get PLAIN-encoded dictionary data for Int64 dictionary
    /// - Returns: PLAIN-encoded dictionary values
    public func dictionaryData() -> Data {
        let encoder = PlainEncoder<Int64>()
        encoder.encode(dictionary)
        return encoder.data
    }
}

// MARK: - Float Specialization

extension DictionaryEncoder where T == Float {
    /// Get PLAIN-encoded dictionary data for Float dictionary
    /// - Returns: PLAIN-encoded dictionary values
    public func dictionaryData() -> Data {
        let encoder = PlainEncoder<Float>()
        encoder.encode(dictionary)
        return encoder.data
    }
}

// MARK: - Double Specialization

extension DictionaryEncoder where T == Double {
    /// Get PLAIN-encoded dictionary data for Double dictionary
    /// - Returns: PLAIN-encoded dictionary values
    public func dictionaryData() -> Data {
        let encoder = PlainEncoder<Double>()
        encoder.encode(dictionary)
        return encoder.data
    }
}
