// ColumnReader - Read column values from Parquet files
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Generic column reader interface.
///
/// **NOTE**: This generic implementation is not usable directly in Phase 1 due to
/// Swift's type system limitations with PlainDecoder's type-specific extensions.
///
/// **Use concrete readers instead**:
/// - `Int32ColumnReader` for Int32 columns
/// - `Int64ColumnReader` for Int64 columns
/// - `FloatColumnReader` for Float columns
/// - `DoubleColumnReader` for Double columns
/// - `StringColumnReader` for String (UTF-8) columns
///
/// This class is kept as documentation of the intended API and may be made
/// functional in a future phase with a different decoder architecture.
///
/// # Intended Usage (not working in Phase 1)
///
/// ```swift
/// let reader = try ColumnReader<Int32>(
///     file: file,
///     columnMetadata: metadata,
///     codec: codec
/// )
///
/// let values = try reader.readBatch(count: 1000)
/// ```
public final class ColumnReader<T> {
    /// Page reader for this column
    private let pageReader: PageReader

    /// Current page being read
    private var currentPage: DataPage?

    /// Current page data and decoding position
    private var currentPageData: Data?
    private var currentPageOffset: Int = 0

    /// Number of values read from current page
    private var valuesReadFromPage: Int = 0

    /// Total values read so far
    private var totalValuesRead: Int = 0

    /// Physical type for this column
    private let physicalType: PhysicalType

    /// Initialize a column reader
    ///
    /// - Parameters:
    ///   - file: The file to read from
    ///   - columnMetadata: Metadata for the column chunk
    ///   - codec: Compression codec
    ///   - physicalType: Physical type of the column
    public init(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec,
        physicalType: PhysicalType
    ) throws {
        self.pageReader = try PageReader(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec
        )
        self.physicalType = physicalType
    }

    // MARK: - Batch Reading

    /// Read a batch of values
    ///
    /// - Parameter count: Maximum number of values to read
    /// - Returns: Array of values (may be fewer than requested if end of column)
    /// - Throws: `ColumnReaderError` if reading fails
    public func readBatch(count: Int) throws -> [T] {
        var values: [T] = []
        values.reserveCapacity(count)

        var remaining = count

        while remaining > 0 {
            // Ensure we have a page loaded
            guard try ensurePageLoaded() else {
                break // No more pages
            }

            // Read from current page
            let toRead = min(remaining, remainingInPage())

            for _ in 0..<toRead {
                let value = try decodeOneValue()
                values.append(value)
                valuesReadFromPage += 1
                totalValuesRead += 1
            }

            remaining -= toRead

            // Check if page is exhausted
            if valuesReadFromPage >= currentPage!.numValues {
                currentPage = nil
                currentPageData = nil
                currentPageOffset = 0
                valuesReadFromPage = 0
            }
        }

        return values
    }

    /// Read a single value
    ///
    /// - Returns: The value, or nil if no more values
    /// - Throws: `ColumnReaderError` if reading fails
    public func readOne() throws -> T? {
        guard try ensurePageLoaded() else {
            return nil
        }

        let value = try decodeOneValue()
        valuesReadFromPage += 1
        totalValuesRead += 1

        // Check if page is exhausted
        if valuesReadFromPage >= currentPage!.numValues {
            currentPage = nil
            currentPageData = nil
            currentPageOffset = 0
            valuesReadFromPage = 0
        }

        return value
    }

    /// Read all remaining values
    ///
    /// - Returns: Array of all remaining values
    /// - Throws: `ColumnReaderError` if reading fails
    public func readAll() throws -> [T] {
        var values: [T] = []

        while let value = try readOne() {
            values.append(value)
        }

        return values
    }

    // MARK: - Private Helpers

    /// Ensure a page is loaded, loading next page if needed
    private func ensurePageLoaded() throws -> Bool {
        // Return true if page still has values
        if currentPageData != nil && valuesReadFromPage < currentPage!.numValues {
            return true
        }

        // Load next page
        guard let page = try pageReader.readDataPage() else {
            return false // No more pages
        }

        // Validate encoding
        guard page.encoding == .plain else {
            throw ColumnReaderError.unsupportedEncoding(
                "Only PLAIN encoding supported in M1.9b, got \(page.encoding)"
            )
        }

        currentPage = page
        currentPageData = page.data
        currentPageOffset = 0
        valuesReadFromPage = 0

        return true
    }

    /// Decode one value from current page data
    ///
    /// **IMPORTANT**: This generic ColumnReader is not usable directly due to PlainDecoder's
    /// type-specific extensions. Use concrete type-specific readers like Int32ColumnReader instead.
    ///
    /// The factory methods below (`.int32()`, `.int64()`, etc.) will also fail at runtime
    /// because this method cannot be implemented generically with Swift's current type system.
    private func decodeOneValue() throws -> T {
        fatalError("""
            ColumnReader<T> cannot be used directly because PlainDecoder uses type-specific extensions.
            Use concrete readers instead:
            - Int32ColumnReader for Int32 columns
            - Int64ColumnReader for Int64 columns
            - FloatColumnReader for Float columns
            - DoubleColumnReader for Double columns
            - StringColumnReader for String (UTF-8) columns
            """)
    }

    /// Get remaining values in current page
    private func remainingInPage() -> Int {
        guard let page = currentPage else { return 0 }
        return page.numValues - valuesReadFromPage
    }
}

// MARK: - Errors

/// Errors that can occur during column reading
public enum ColumnReaderError: Error, Equatable {
    /// Unsupported encoding
    case unsupportedEncoding(String)

    /// Page reading error
    case pageReadError(String)

    /// Decoding error
    case decodingError(String)

    /// Type mismatch
    case typeMismatch(String)

    /// Missing dictionary
    case missingDictionary(String)

    /// Unsupported feature for this column type
    case unsupportedFeature(String)

    /// Internal error (shouldn't happen)
    case internalError(String)
}

extension ColumnReaderError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .unsupportedEncoding(let msg):
            return "Unsupported encoding: \(msg)"
        case .pageReadError(let msg):
            return "Page read error: \(msg)"
        case .decodingError(let msg):
            return "Decoding error: \(msg)"
        case .typeMismatch(let msg):
            return "Type mismatch: \(msg)"
        case .missingDictionary(let msg):
            return "Missing dictionary: \(msg)"
        case .unsupportedFeature(let msg):
            return "Unsupported feature: \(msg)"
        case .internalError(let msg):
            return "Internal error: \(msg)"
        }
    }
}

// MARK: - Type-Specific Initializers

extension ColumnReader where T == Bool {
    /// Create a column reader for Boolean type
    public static func boolean(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<Bool> {
        try ColumnReader<Bool>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .boolean
        )
    }
}

extension ColumnReader where T == Int32 {
    /// Create a column reader for Int32 type
    public static func int32(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<Int32> {
        try ColumnReader<Int32>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .int32
        )
    }
}

extension ColumnReader where T == Int64 {
    /// Create a column reader for Int64 type
    public static func int64(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<Int64> {
        try ColumnReader<Int64>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .int64
        )
    }
}

extension ColumnReader where T == Float {
    /// Create a column reader for Float type
    public static func float(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<Float> {
        try ColumnReader<Float>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .float
        )
    }
}

extension ColumnReader where T == Double {
    /// Create a column reader for Double type
    public static func double(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<Double> {
        try ColumnReader<Double>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .double
        )
    }
}

extension ColumnReader where T == Data {
    /// Create a column reader for ByteArray type
    public static func byteArray(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<Data> {
        try ColumnReader<Data>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .byteArray
        )
    }

    /// Create a column reader for FixedLenByteArray type
    public static func fixedLenByteArray(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec,
        length: Int
    ) throws -> ColumnReader<Data> {
        try ColumnReader<Data>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .fixedLenByteArray(length: length)
        )
    }
}

extension ColumnReader where T == String {
    /// Create a column reader for String (UTF-8) type
    public static func string(
        file: RandomAccessFile,
        columnMetadata: ColumnMetadata,
        codec: Codec
    ) throws -> ColumnReader<String> {
        try ColumnReader<String>(
            file: file,
            columnMetadata: columnMetadata,
            codec: codec,
            physicalType: .byteArray
        )
    }
}
