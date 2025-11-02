// ParquetFileReader - Read Parquet file metadata
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Errors that can occur during file reading.
public enum ParquetFileError: Error {
    case invalidFile(String)
    case ioError(String)
}

/// Basic Parquet file reader (Phase 1 - metadata only).
///
/// Parquet file format:
/// - Header: "PAR1" magic (4 bytes)
/// - Data: Row groups and pages
/// - Footer: FileMetaData (Thrift Compact Binary)
/// - Trailer: footer_length (4 bytes, little-endian) + "PAR1" magic (4 bytes)
///
/// Example usage:
/// ```swift
/// let metadata = try ParquetFileReader.readMetadata(from: fileURL)
/// print("Rows: \(metadata.numRows)")
/// print("Row groups: \(metadata.numRowGroups)")
/// for rowGroup in metadata.rowGroups {
///     print("  Row group: \(rowGroup.numRows) rows")
/// }
/// ```
public struct ParquetFileReader {
    /// The magic bytes that identify a Parquet file.
    public static let magic: Data = Data([0x50, 0x41, 0x52, 0x31]) // "PAR1"

    // MARK: - Public API (returns wrapper types)

    /// Reads file metadata from a Parquet file.
    ///
    /// - Parameter url: The URL of the Parquet file
    /// - Returns: The file metadata
    /// - Throws: ParquetFileError or IOError if the file is invalid or cannot be read
    public static func readMetadata(from url: URL) throws -> FileMetadata {
        let thrift = try readThriftMetadata(from: url)
        return try FileMetadata(thrift: thrift)
    }

    /// Reads file metadata from in-memory Parquet data.
    ///
    /// For files on disk, use `readMetadata(from: URL)` instead for better performance.
    ///
    /// - Parameter data: The complete file data
    /// - Returns: The file metadata
    /// - Throws: ParquetFileError if the data is invalid
    public static func readMetadata(from data: Data) throws -> FileMetadata {
        let thrift = try readThriftMetadata(from: data)
        return try FileMetadata(thrift: thrift)
    }

    // MARK: - Internal API (returns Thrift types)

    /// Reads Thrift metadata from a Parquet file using efficient I/O.
    internal static func readThriftMetadata(from url: URL) throws -> ThriftFileMetaData {
        let file = try FileRandomAccessFile(url: url)
        defer { try? file.close() }

        let reader = BufferedReader(file: file)
        return try readThriftMetadata(from: reader)
    }

    /// Reads Thrift metadata from a BufferedReader.
    internal static func readThriftMetadata(from reader: BufferedReader) throws -> ThriftFileMetaData {
        let fileSize = try reader.fileSize

        // Minimum Parquet file size: 12 bytes (header + footer length + magic)
        guard fileSize >= 12 else {
            throw ParquetFileError.invalidFile("File too small to be a Parquet file (\(fileSize) bytes)")
        }

        // Check header magic
        let headerMagic = try reader.read(at: 0, count: 4)
        guard headerMagic == magic else {
            throw ParquetFileError.invalidFile("Invalid header magic bytes")
        }

        // Check footer magic
        let footerMagic = try reader.read(at: fileSize - 4, count: 4)
        guard footerMagic == magic else {
            throw ParquetFileError.invalidFile("Invalid footer magic bytes")
        }

        // Read footer length (4 bytes before the trailing magic)
        let footerLength = try reader.readUInt32LE(at: fileSize - 8)

        guard footerLength > 0 else {
            throw ParquetFileError.invalidFile("Footer length is zero")
        }

        // Calculate metadata offset
        let metadataOffset = fileSize - 8 - Int(footerLength)
        guard metadataOffset >= 4 else {
            throw ParquetFileError.invalidFile("Invalid footer length: \(footerLength)")
        }

        // Read metadata bytes
        let metadataBytes = try reader.read(at: metadataOffset, count: Int(footerLength))

        // Deserialize using ThriftReader
        let thriftReader = ThriftReader(data: metadataBytes)
        do {
            return try thriftReader.readFileMetaData()
        } catch {
            throw ParquetFileError.invalidFile("Failed to parse FileMetaData: \(error)")
        }
    }

    /// Reads Thrift metadata from in-memory Parquet file data.
    internal static func readThriftMetadata(from data: Data) throws -> ThriftFileMetaData {
        let memFile = MemoryRandomAccessFile(data: data)
        let reader = BufferedReader(file: memFile)
        return try readThriftMetadata(from: reader)
    }

    // MARK: - Schema Reading

    /// Reads and builds the schema from a Parquet file.
    ///
    /// - Parameter url: The URL of the Parquet file
    /// - Returns: The reconstructed schema
    /// - Throws: ParquetFileError or SchemaError
    public static func readSchema(from url: URL) throws -> Schema {
        let metadata = try readMetadata(from: url)
        return metadata.schema
    }

    /// Reads and builds the schema from Parquet file data.
    ///
    /// - Parameter data: The complete file data
    /// - Returns: The reconstructed schema
    /// - Throws: ParquetFileError or SchemaError
    public static func readSchema(from data: Data) throws -> Schema {
        let metadata = try readMetadata(from: data)
        return metadata.schema
    }
}
