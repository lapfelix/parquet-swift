// Thrift Statistics - Column statistics in Parquet format
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Statistics per row group and per page.
///
/// All fields are optional.
///
/// Maps to Thrift `Statistics` struct.
public struct ThriftStatistics: Sendable {
    /// DEPRECATED: Max value of the column (use maxValue instead)
    ///
    /// Values are encoded using PLAIN encoding, except that variable-length byte
    /// arrays do not include a length prefix.
    ///
    /// These fields encode min and max values determined by signed comparison only.
    public let max: Data?

    /// DEPRECATED: Min value of the column (use minValue instead)
    ///
    /// Values are encoded using PLAIN encoding, except that variable-length byte
    /// arrays do not include a length prefix.
    public let min: Data?

    /// Count of null values in the column.
    ///
    /// Writers SHOULD always write this field even if it is zero.
    /// Readers MUST distinguish between null_count not being present and null_count == 0.
    public let nullCount: Int64?

    /// Count of distinct values occurring
    public let distinctCount: Int64?

    /// Lower and upper bound values for the column, determined by its ColumnOrder.
    ///
    /// These may be the actual minimum and maximum values found on a page or column chunk,
    /// but can also be (more compact) values that do not exist on a page or column chunk.
    ///
    /// Values are encoded using PLAIN encoding, except that variable-length byte
    /// arrays do not include a length prefix.
    public let maxValue: Data?
    public let minValue: Data?

    /// If true, maxValue is the actual maximum value for a column
    public let isMaxValueExact: Bool?

    /// If true, minValue is the actual minimum value for a column
    public let isMinValueExact: Bool?

    public init(
        max: Data? = nil,
        min: Data? = nil,
        nullCount: Int64? = nil,
        distinctCount: Int64? = nil,
        maxValue: Data? = nil,
        minValue: Data? = nil,
        isMaxValueExact: Bool? = nil,
        isMinValueExact: Bool? = nil
    ) {
        self.max = max
        self.min = min
        self.nullCount = nullCount
        self.distinctCount = distinctCount
        self.maxValue = maxValue
        self.minValue = minValue
        self.isMaxValueExact = isMaxValueExact
        self.isMinValueExact = isMinValueExact
    }
}

/// Wrapper struct to store key-value pairs.
///
/// Maps to Thrift `KeyValue` struct.
public struct ThriftKeyValue: Sendable {
    public let key: String
    public let value: String?

    public init(key: String, value: String? = nil) {
        self.key = key
        self.value = value
    }
}
