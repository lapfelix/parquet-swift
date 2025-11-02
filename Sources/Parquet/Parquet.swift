// Parquet-Swift
// Native Swift implementation of Apache Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Main Parquet module
///
/// This module provides native Swift APIs for reading and writing Apache Parquet files.
///
/// # Overview
///
/// Parquet is a columnar storage format designed for efficient data processing.
/// This library provides:
/// - Reading Parquet files (Phase 1)
/// - Writing Parquet files (Phase 3)
/// - Support for all standard encodings
/// - Nested type support (Phase 2)
///
/// # Getting Started
///
/// ```swift
/// import Parquet
///
/// // Read a Parquet file
/// let reader = try ParquetFileReader(path: "data.parquet")
/// let metadata = reader.metadata
/// print("Rows: \(metadata.numRows)")
/// ```
///
/// # Current Status
///
/// Phase 1 (In Progress): Practical Reader
/// - ✅ Project setup
/// - 🚧 Core types
/// - 🚧 Thrift integration
/// - 🚧 PLAIN + DICTIONARY encoding
/// - 🚧 Optional column support
public enum Parquet {
    /// Library version
    public static let version = "0.1.0-alpha"

    /// Library name
    public static let name = "Parquet-Swift"
}
