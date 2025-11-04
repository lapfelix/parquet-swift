// ColumnWriter.swift - Column writers for primitive types
//
// Licensed under the Apache License, Version 2.0

import Foundation

// MARK: - Int32 Column Writer

/// Writer for Int32 columns
///
/// Buffers values and writes data pages when the buffer is full
public final class Int32ColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Int32] = []
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64

    // Track metadata for column chunk
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
    }

    /// Write a batch of Int32 values
    /// - Parameter values: Values to write
    /// - Throws: WriterError if write fails
    public func writeValues(_ values: [Int32]) throws {
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        // Flush if buffer is large enough
        if shouldFlush() {
            try flush()
        }
    }

    /// Flush any buffered values to disk
    /// - Throws: WriterError if flush fails
    func flush() throws {
        guard !valueBuffer.isEmpty else {
            return
        }

        // Encode values using PLAIN encoding
        let encoder = PlainEncoder<Int32>()
        encoder.encode(valueBuffer)

        // Write data page and capture result
        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: Int32(valueBuffer.count),
            encoding: .plain
        )

        // Track first data page offset
        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        // Accumulate sizes
        totalCompressedSize += Int64(result.compressedSize)
        totalUncompressedSize += Int64(result.uncompressedSize)

        // Clear buffer
        valueBuffer.removeAll(keepingCapacity: true)
    }

    /// Close the column writer (flush any remaining data)
    /// - Returns: Metadata for this column chunk
    /// - Throws: WriterError if close fails
    func close() throws -> WriterColumnChunkMetadata {
        // Flush any remaining values
        try flush()

        // dataPageOffset should be set if we wrote any data
        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: columnStartOffset,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: [.plain],
            codec: properties.compression(for: column.name)
        )
    }

    private func shouldFlush() -> Bool {
        let estimatedSize = valueBuffer.count * 4  // 4 bytes per Int32
        return estimatedSize >= properties.dataPageSize
    }
}

// MARK: - Int64 Column Writer

/// Writer for Int64 columns
public final class Int64ColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Int64] = []
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
    }

    /// Write a batch of Int64 values
    public func writeValues(_ values: [Int64]) throws {
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty else {
            return
        }

        let encoder = PlainEncoder<Int64>()
        encoder.encode(valueBuffer)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: Int32(valueBuffer.count),
            encoding: .plain
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        totalCompressedSize += Int64(result.compressedSize)
        totalUncompressedSize += Int64(result.uncompressedSize)

        valueBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: columnStartOffset,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: [.plain],
            codec: properties.compression(for: column.name)
        )
    }

    private func shouldFlush() -> Bool {
        let estimatedSize = valueBuffer.count * 8  // 8 bytes per Int64
        return estimatedSize >= properties.dataPageSize
    }
}

// MARK: - Float Column Writer

/// Writer for Float columns
public final class FloatColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Float] = []
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
    }

    /// Write a batch of Float values
    public func writeValues(_ values: [Float]) throws {
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty else {
            return
        }

        let encoder = PlainEncoder<Float>()
        encoder.encode(valueBuffer)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: Int32(valueBuffer.count),
            encoding: .plain
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        totalCompressedSize += Int64(result.compressedSize)
        totalUncompressedSize += Int64(result.uncompressedSize)

        valueBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: columnStartOffset,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: [.plain],
            codec: properties.compression(for: column.name)
        )
    }

    private func shouldFlush() -> Bool {
        let estimatedSize = valueBuffer.count * 4  // 4 bytes per Float
        return estimatedSize >= properties.dataPageSize
    }
}

// MARK: - Double Column Writer

/// Writer for Double columns
public final class DoubleColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Double] = []
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
    }

    /// Write a batch of Double values
    public func writeValues(_ values: [Double]) throws {
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty else {
            return
        }

        let encoder = PlainEncoder<Double>()
        encoder.encode(valueBuffer)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: Int32(valueBuffer.count),
            encoding: .plain
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        totalCompressedSize += Int64(result.compressedSize)
        totalUncompressedSize += Int64(result.uncompressedSize)

        valueBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: columnStartOffset,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: [.plain],
            codec: properties.compression(for: column.name)
        )
    }

    private func shouldFlush() -> Bool {
        let estimatedSize = valueBuffer.count * 8  // 8 bytes per Double
        return estimatedSize >= properties.dataPageSize
    }
}

// MARK: - String Column Writer

/// Writer for String columns (UTF-8 byte arrays)
public final class StringColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [String] = []
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
    }

    /// Write a batch of String values
    public func writeValues(_ values: [String]) throws {
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty else {
            return
        }

        let encoder = PlainEncoder<String>()
        try encoder.encode(valueBuffer)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: Int32(valueBuffer.count),
            encoding: .plain
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        totalCompressedSize += Int64(result.compressedSize)
        totalUncompressedSize += Int64(result.uncompressedSize)

        valueBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: columnStartOffset,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: [.plain],
            codec: properties.compression(for: column.name)
        )
    }

    private func shouldFlush() -> Bool {
        // Estimate size: 4-byte length + string bytes
        let estimatedSize = valueBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
        return estimatedSize >= properties.dataPageSize
    }
}
