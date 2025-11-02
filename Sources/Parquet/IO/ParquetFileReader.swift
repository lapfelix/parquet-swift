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
public struct ParquetFileReader {
    /// The magic bytes that identify a Parquet file.
    public static let magic: Data = Data([0x50, 0x41, 0x52, 0x31]) // "PAR1"

    /// Reads the FileMetaData from a Parquet file.
    ///
    /// - Parameter url: The URL of the Parquet file
    /// - Returns: The deserialized FileMetaData
    /// - Throws: ParquetFileError if the file is invalid or cannot be read
    public static func readMetadata(from url: URL) throws -> ThriftFileMetaData {
        let data = try Data(contentsOf: url)
        return try readMetadata(from: data)
    }

    /// Reads the FileMetaData from Parquet file data.
    ///
    /// - Parameter data: The complete file data
    /// - Returns: The deserialized FileMetaData
    /// - Throws: ParquetFileError if the data is invalid
    public static func readMetadata(from data: Data) throws -> ThriftFileMetaData {
        // Minimum Parquet file size: 12 bytes (header + footer length + magic)
        guard data.count >= 12 else {
            throw ParquetFileError.invalidFile("File too small to be a Parquet file (\(data.count) bytes)")
        }

        // Check header magic
        let headerMagic = data.prefix(4)
        guard headerMagic == magic else {
            throw ParquetFileError.invalidFile("Invalid header magic bytes")
        }

        // Check footer magic
        let footerMagic = data.suffix(4)
        guard footerMagic == magic else {
            throw ParquetFileError.invalidFile("Invalid footer magic bytes")
        }

        // Read footer length (4 bytes before the trailing magic)
        let footerLengthOffset = data.count - 8
        let footerLengthBytes = data.subdata(in: footerLengthOffset..<(footerLengthOffset + 4))
        let footerLength = footerLengthBytes.withUnsafeBytes { $0.load(as: UInt32.self).littleEndian }

        guard footerLength > 0 else {
            throw ParquetFileError.invalidFile("Footer length is zero")
        }

        // Calculate metadata offset
        let metadataOffset = data.count - 8 - Int(footerLength)
        guard metadataOffset >= 4 else {
            throw ParquetFileError.invalidFile("Invalid footer length: \(footerLength)")
        }

        // Extract metadata bytes
        let metadataBytes = data.subdata(in: metadataOffset..<footerLengthOffset)

        // Deserialize using ThriftReader
        let reader = ThriftReader(data: metadataBytes)
        do {
            return try reader.readFileMetaData()
        } catch {
            throw ParquetFileError.invalidFile("Failed to parse FileMetaData: \(error)")
        }
    }

    /// Reads and builds the schema from a Parquet file.
    ///
    /// - Parameter url: The URL of the Parquet file
    /// - Returns: The reconstructed schema
    /// - Throws: ParquetFileError or SchemaError
    public static func readSchema(from url: URL) throws -> Schema {
        let metadata = try readMetadata(from: url)
        return try SchemaBuilder.buildSchema(from: metadata.schema)
    }

    /// Reads and builds the schema from Parquet file data.
    ///
    /// - Parameter data: The complete file data
    /// - Returns: The reconstructed schema
    /// - Throws: ParquetFileError or SchemaError
    public static func readSchema(from data: Data) throws -> Schema {
        let metadata = try readMetadata(from: data)
        return try SchemaBuilder.buildSchema(from: metadata.schema)
    }
}
