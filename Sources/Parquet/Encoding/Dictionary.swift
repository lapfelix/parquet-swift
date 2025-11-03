// Dictionary - Parquet dictionary encoding support
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// A dictionary that maps indices to values for dictionary-encoded columns.
///
/// Dictionary encoding stores a dictionary of unique values in a dictionary page,
/// then stores indices to those values in the data pages. This is efficient for
/// columns with low cardinality (many repeated values).
///
/// # Format
///
/// - **Dictionary Page**: Contains PLAIN-encoded dictionary values
/// - **Data Pages**: Contains RLE/Bit-Packing Hybrid encoded indices
///
/// # Usage
///
/// ```swift
/// // Read dictionary page
/// let dictPage = try pageReader.readDictionaryPage()
///
/// // Decode dictionary values
/// let dictionary = try Dictionary<Int32>(page: dictPage)
///
/// // Read data page with indices
/// let dataPage = try pageReader.readDataPage()
/// let indices = try RLEDecoder().decodeIndices(from: dataPage.data, numValues: 1000)
///
/// // Look up values
/// let values = try dictionary.values(at: indices)
/// ```
///
/// # Thread Safety
///
/// Dictionaries are immutable and thread-safe after initialization.
public struct Dictionary<T> {
    /// The dictionary values (index → value mapping)
    private let values: [T]

    /// Number of values in the dictionary
    public var count: Int { values.count }

    /// Private initializer (use factory methods instead)
    private init(values: [T]) {
        self.values = values
    }

    /// Look up a value by index
    ///
    /// - Parameter index: Dictionary index (0-based)
    /// - Returns: The value at that index
    /// - Throws: `DictionaryError.indexOutOfBounds` if index is invalid
    public func value(at index: Int) throws -> T {
        guard index >= 0 && index < values.count else {
            throw DictionaryError.indexOutOfBounds(index: index, max: values.count - 1)
        }
        return values[index]
    }

    /// Look up a value by UInt32 index (common from RLE decoder)
    ///
    /// - Parameter index: Dictionary index (0-based)
    /// - Returns: The value at that index
    /// - Throws: `DictionaryError.indexOutOfBounds` if index is invalid
    public func value(at index: UInt32) throws -> T {
        guard index < values.count else {
            throw DictionaryError.indexOutOfBounds(index: Int(index), max: values.count - 1)
        }
        return values[Int(index)]
    }

    /// Look up multiple values by indices
    ///
    /// - Parameter indices: Array of dictionary indices
    /// - Returns: Array of values corresponding to those indices
    /// - Throws: `DictionaryError.indexOutOfBounds` if any index is invalid
    public func values(at indices: [UInt32]) throws -> [T] {
        var result = [T]()
        result.reserveCapacity(indices.count)

        for index in indices {
            result.append(try value(at: index))
        }

        return result
    }
}

// MARK: - Factory Methods

extension Dictionary where T == Int32 {
    /// Create an Int32 dictionary from a dictionary page
    ///
    /// - Parameter page: The dictionary page
    /// - Returns: A dictionary mapping indices to Int32 values
    /// - Throws: `DictionaryError` if page is invalid or encoding is unsupported
    public static func int32(page: DictionaryPage) throws -> Dictionary<Int32> {
        // Validate encoding is PLAIN or PLAIN_DICTIONARY
        // (older writers use PLAIN_DICTIONARY for dictionary pages)
        guard page.encoding == .plain || page.encoding == .plainDictionary else {
            throw DictionaryError.unsupportedEncoding(page.encoding)
        }

        // Decode values using PlainDecoder
        let decoder = PlainDecoder<Int32>(data: page.data)
        let values = try decoder.decode(count: page.numValues)

        // Verify we decoded the right number of values
        guard values.count == page.numValues else {
            throw DictionaryError.valueMismatch(expected: page.numValues, got: values.count)
        }

        return Dictionary(values: values)
    }
}

extension Dictionary where T == Int64 {
    /// Create an Int64 dictionary from a dictionary page
    ///
    /// - Parameter page: The dictionary page
    /// - Returns: A dictionary mapping indices to Int64 values
    /// - Throws: `DictionaryError` if page is invalid or encoding is unsupported
    public static func int64(page: DictionaryPage) throws -> Dictionary<Int64> {
        // Validate encoding is PLAIN or PLAIN_DICTIONARY
        // (older writers use PLAIN_DICTIONARY for dictionary pages)
        guard page.encoding == .plain || page.encoding == .plainDictionary else {
            throw DictionaryError.unsupportedEncoding(page.encoding)
        }

        // Decode values using PlainDecoder
        let decoder = PlainDecoder<Int64>(data: page.data)
        let values = try decoder.decode(count: page.numValues)

        // Verify we decoded the right number of values
        guard values.count == page.numValues else {
            throw DictionaryError.valueMismatch(expected: page.numValues, got: values.count)
        }

        return Dictionary(values: values)
    }
}

extension Dictionary where T == Float {
    /// Create a Float dictionary from a dictionary page
    ///
    /// - Parameter page: The dictionary page
    /// - Returns: A dictionary mapping indices to Float values
    /// - Throws: `DictionaryError` if page is invalid or encoding is unsupported
    public static func float(page: DictionaryPage) throws -> Dictionary<Float> {
        // Validate encoding is PLAIN or PLAIN_DICTIONARY
        // (older writers use PLAIN_DICTIONARY for dictionary pages)
        guard page.encoding == .plain || page.encoding == .plainDictionary else {
            throw DictionaryError.unsupportedEncoding(page.encoding)
        }

        // Decode values using PlainDecoder
        let decoder = PlainDecoder<Float>(data: page.data)
        let values = try decoder.decode(count: page.numValues)

        // Verify we decoded the right number of values
        guard values.count == page.numValues else {
            throw DictionaryError.valueMismatch(expected: page.numValues, got: values.count)
        }

        return Dictionary(values: values)
    }
}

extension Dictionary where T == Double {
    /// Create a Double dictionary from a dictionary page
    ///
    /// - Parameter page: The dictionary page
    /// - Returns: A dictionary mapping indices to Double values
    /// - Throws: `DictionaryError` if page is invalid or encoding is unsupported
    public static func double(page: DictionaryPage) throws -> Dictionary<Double> {
        // Validate encoding is PLAIN or PLAIN_DICTIONARY
        // (older writers use PLAIN_DICTIONARY for dictionary pages)
        guard page.encoding == .plain || page.encoding == .plainDictionary else {
            throw DictionaryError.unsupportedEncoding(page.encoding)
        }

        // Decode values using PlainDecoder
        let decoder = PlainDecoder<Double>(data: page.data)
        let values = try decoder.decode(count: page.numValues)

        // Verify we decoded the right number of values
        guard values.count == page.numValues else {
            throw DictionaryError.valueMismatch(expected: page.numValues, got: values.count)
        }

        return Dictionary(values: values)
    }
}

extension Dictionary where T == String {
    /// Create a String dictionary from a dictionary page
    ///
    /// - Parameter page: The dictionary page
    /// - Returns: A dictionary mapping indices to String values
    /// - Throws: `DictionaryError` if page is invalid or encoding is unsupported
    public static func string(page: DictionaryPage) throws -> Dictionary<String> {
        // Validate encoding is PLAIN or PLAIN_DICTIONARY
        // (older writers use PLAIN_DICTIONARY for dictionary pages)
        guard page.encoding == .plain || page.encoding == .plainDictionary else {
            throw DictionaryError.unsupportedEncoding(page.encoding)
        }

        // Decode values using PlainDecoder
        let decoder = PlainDecoder<String>(data: page.data)
        let values = try decoder.decode(count: page.numValues)

        // Verify we decoded the right number of values
        guard values.count == page.numValues else {
            throw DictionaryError.valueMismatch(expected: page.numValues, got: values.count)
        }

        return Dictionary(values: values)
    }
}

// MARK: - Errors

/// Errors that can occur during dictionary operations
public enum DictionaryError: Error, Equatable {
    /// Dictionary encoding is not supported
    case unsupportedEncoding(ThriftEncoding)

    /// Dictionary index is out of bounds
    case indexOutOfBounds(index: Int, max: Int)

    /// Number of decoded values doesn't match page header
    case valueMismatch(expected: Int, got: Int)
}

extension DictionaryError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .unsupportedEncoding(let encoding):
            return "Dictionary: Unsupported encoding \(encoding) (only PLAIN is supported)"
        case .indexOutOfBounds(let index, let max):
            return "Dictionary: Index \(index) out of bounds (max: \(max))"
        case .valueMismatch(let expected, let got):
            return "Dictionary: Value count mismatch (expected \(expected), got \(got))"
        }
    }
}
