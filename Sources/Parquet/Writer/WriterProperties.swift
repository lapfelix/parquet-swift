// WriterProperties.swift - Configuration properties for Parquet file writing
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Configuration properties for writing Parquet files
public struct WriterProperties {
    // MARK: - Compression

    /// Default compression codec for all columns
    public var compression: Compression

    /// Compression level (codec-specific, if supported)
    public var compressionLevel: Int?

    // MARK: - Encoding

    /// Whether to enable dictionary encoding by default
    public var dictionaryEnabled: Bool

    /// Dictionary page size limit (bytes)
    /// When dictionary exceeds this size, fall back to PLAIN encoding
    public var dictionaryPageSizeLimit: Int64

    // MARK: - Page Sizes

    /// Target data page size (bytes)
    public var dataPageSize: Int64

    /// Maximum number of rows per page
    public var maxRowsPerPage: Int64

    // MARK: - Statistics

    /// Whether to generate statistics by default
    public var statisticsEnabled: Bool

    /// Maximum statistics size (bytes)
    /// Statistics larger than this are dropped
    public var maxStatisticsSize: Int64

    // MARK: - Version

    /// Parquet format version to write
    public var version: ParquetVersion

    // MARK: - Per-Column Overrides

    /// Per-column property overrides (keyed by column path)
    public var columnProperties: [String: ColumnProperties]

    // MARK: - Defaults

    /// Default writer properties
    public static let `default` = WriterProperties(
        compression: .uncompressed,
        compressionLevel: nil,
        dictionaryEnabled: true,
        dictionaryPageSizeLimit: 1024 * 1024,  // 1MB
        dataPageSize: 1024 * 1024,              // 1MB
        maxRowsPerPage: 20_000,
        statisticsEnabled: true,
        maxStatisticsSize: 4096,                // 4KB
        version: .v1,
        columnProperties: [:]
    )

    /// Create writer properties with custom configuration
    public init(
        compression: Compression = .uncompressed,
        compressionLevel: Int? = nil,
        dictionaryEnabled: Bool = true,
        dictionaryPageSizeLimit: Int64 = 1024 * 1024,
        dataPageSize: Int64 = 1024 * 1024,
        maxRowsPerPage: Int64 = 20_000,
        statisticsEnabled: Bool = true,
        maxStatisticsSize: Int64 = 4096,
        version: ParquetVersion = .v1,
        columnProperties: [String: ColumnProperties] = [:]
    ) {
        self.compression = compression
        self.compressionLevel = compressionLevel
        self.dictionaryEnabled = dictionaryEnabled
        self.dictionaryPageSizeLimit = dictionaryPageSizeLimit
        self.dataPageSize = dataPageSize
        self.maxRowsPerPage = maxRowsPerPage
        self.statisticsEnabled = statisticsEnabled
        self.maxStatisticsSize = maxStatisticsSize
        self.version = version
        self.columnProperties = columnProperties
    }

    // MARK: - Column-Specific Properties

    /// Get effective compression for a column (checks overrides)
    public func compression(for columnPath: String) -> Compression {
        columnProperties[columnPath]?.compression ?? compression
    }

    /// Get effective dictionary enabled setting for a column
    public func dictionaryEnabled(for columnPath: String) -> Bool {
        columnProperties[columnPath]?.dictionaryEnabled ?? dictionaryEnabled
    }

    /// Get effective statistics enabled setting for a column
    public func statisticsEnabled(for columnPath: String) -> Bool {
        columnProperties[columnPath]?.statisticsEnabled ?? statisticsEnabled
    }
}

// MARK: - Column Properties

/// Per-column property overrides
public struct ColumnProperties {
    /// Compression codec for this column
    public var compression: Compression?

    /// Whether dictionary encoding is enabled for this column
    public var dictionaryEnabled: Bool?

    /// Whether statistics are enabled for this column
    public var statisticsEnabled: Bool?

    public init(
        compression: Compression? = nil,
        dictionaryEnabled: Bool? = nil,
        statisticsEnabled: Bool? = nil
    ) {
        self.compression = compression
        self.dictionaryEnabled = dictionaryEnabled
        self.statisticsEnabled = statisticsEnabled
    }
}

// MARK: - Parquet Version

/// Parquet format version
public enum ParquetVersion {
    /// Parquet format version 1.0
    case v1

    /// Parquet format version 2.0 (with additional features)
    case v2

    /// Default version for writing
    public static let `default`: ParquetVersion = .v1
}
