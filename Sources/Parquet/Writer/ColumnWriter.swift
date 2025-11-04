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
    private var definitionLevelBuffer: [UInt16] = []  // W5: Track nulls (0=null, 1=present)
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private let isNullable: Bool

    // Track metadata for column chunk
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []

    // W6: Statistics tracking
    private var statisticsAccumulator: Int32StatisticsAccumulator?

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.isNullable = !column.isRequired

        // W6: Initialize statistics accumulator if enabled
        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = Int32StatisticsAccumulator()
        }
    }

    /// Write a batch of Int32 values (for required columns)
    /// - Parameter values: Values to write
    /// - Throws: WriterError if write fails
    public func writeValues(_ values: [Int32]) throws {
        guard !isNullable else {
            throw WriterError.invalidState("Column \(column.name) is nullable, use writeOptionalValues()")
        }

        // W6: Update statistics (zero-copy, type-safe)
        statisticsAccumulator?.update(values)

        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        // Flush if buffer is large enough
        if shouldFlush() {
            try flush()
        }
    }

    /// Write a batch of optional Int32 values (for nullable columns)
    /// - Parameter values: Array of optional Int32s (nil = null)
    /// - Throws: WriterError if write fails
    public func writeOptionalValues(_ values: [Int32?]) throws {
        guard isNullable else {
            throw WriterError.invalidState("Column \(column.name) is required, use writeValues()")
        }

        // W6: Update statistics (zero-copy, type-safe)
        statisticsAccumulator?.updateNullable(values)

        // Encode definition levels and non-null values
        for value in values {
            if let nonNullValue = value {
                // Present value: def level = 1
                definitionLevelBuffer.append(1)
                valueBuffer.append(nonNullValue)
            } else {
                // Null value: def level = 0
                definitionLevelBuffer.append(0)
                // No value added to valueBuffer
            }
        }

        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    /// Flush any buffered values to disk
    /// - Throws: WriterError if flush fails
    func flush() throws {
        // For nullable columns, allow flush if we have definition levels (even with no values)
        // For required columns, need at least one value
        guard !valueBuffer.isEmpty || (isNullable && !definitionLevelBuffer.isEmpty) else {
            return
        }

        // Encode definition levels if column is nullable
        let definitionLevelsData: Data?
        if isNullable {
            let encoder = LevelEncoder(maxLevel: 1)  // maxLevel=1 for optional columns
            encoder.encode(definitionLevelBuffer)
            definitionLevelsData = encoder.flush()
            usedEncodings.insert(.rle)  // Track RLE encoding used for definition levels
        } else {
            definitionLevelsData = nil
        }

        // Encode values using PLAIN encoding
        let encoder = PlainEncoder<Int32>()
        encoder.encode(valueBuffer)

        // Number of values = total rows (including nulls for nullable columns)
        let numValues = Int32(isNullable ? definitionLevelBuffer.count : valueBuffer.count)

        // Write data page and capture result
        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData
        )

        // Track first data page offset
        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        // Accumulate sizes (including page headers per Parquet spec)
        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)

        // Clear buffers
        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
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

        // W6: Build statistics if enabled
        let statistics = statisticsAccumulator?.build()

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: 0,  // Per spec: 0 when metadata is in footer (deprecated field)
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: Array(usedEncodings),
            codec: properties.compression(for: column.name),
            statistics: statistics
        )
    }

    private func shouldFlush() -> Bool {
        if isNullable {
            // For nullable columns, check both value buffer and definition level buffer
            let valueSize = valueBuffer.count * 4  // 4 bytes per Int32
            // Definition levels: conservative estimate (4-byte length + ~1 bit per level)
            let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
            let estimatedSize = valueSize + defLevelSize
            return estimatedSize >= properties.dataPageSize
        } else {
            let estimatedSize = valueBuffer.count * 4  // 4 bytes per Int32
            return estimatedSize >= properties.dataPageSize
        }
    }
}

// MARK: - Int64 Column Writer

/// Writer for Int64 columns
public final class Int64ColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Int64] = []
    private var definitionLevelBuffer: [UInt16] = []  // W5: Track nulls
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private let isNullable: Bool
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []
    private var statisticsAccumulator: Int64StatisticsAccumulator?  // W6

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.isNullable = !column.isRequired
        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = Int64StatisticsAccumulator()
        }
    }

    /// Write a batch of Int64 values (for required columns)
    public func writeValues(_ values: [Int64]) throws {
        guard !isNullable else {
            throw WriterError.invalidState("Column \(column.name) is nullable, use writeOptionalValues()")
        }

        statisticsAccumulator?.update(values)  // W6
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    /// Write a batch of optional Int64 values (for nullable columns)
    public func writeOptionalValues(_ values: [Int64?]) throws {
        guard isNullable else {
            throw WriterError.invalidState("Column \(column.name) is required, use writeValues()")
        }

        statisticsAccumulator?.updateNullable(values)  // W6
        for value in values {
            if let nonNullValue = value {
                definitionLevelBuffer.append(1)
                valueBuffer.append(nonNullValue)
            } else {
                definitionLevelBuffer.append(0)
            }
        }

        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty || (isNullable && !definitionLevelBuffer.isEmpty) else {
            return
        }

        // Encode definition levels if column is nullable
        let definitionLevelsData: Data?
        if isNullable {
            let encoder = LevelEncoder(maxLevel: 1)
            encoder.encode(definitionLevelBuffer)
            definitionLevelsData = encoder.flush()
            usedEncodings.insert(.rle)
        } else {
            definitionLevelsData = nil
        }

        let encoder = PlainEncoder<Int64>()
        encoder.encode(valueBuffer)

        let numValues = Int32(isNullable ? definitionLevelBuffer.count : valueBuffer.count)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)

        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        let statistics = statisticsAccumulator?.build()  // W6

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: 0,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: Array(usedEncodings),
            codec: properties.compression(for: column.name),
            statistics: statistics
        )
    }

    private func shouldFlush() -> Bool {
        if isNullable {
            // For nullable columns, check both value buffer and definition level buffer
            let valueSize = valueBuffer.count * 8  // 8 bytes per Int64
            // Definition levels: conservative estimate (4-byte length + ~1 bit per level)
            let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
            let estimatedSize = valueSize + defLevelSize
            return estimatedSize >= properties.dataPageSize
        } else {
            let estimatedSize = valueBuffer.count * 8  // 8 bytes per Int64
            return estimatedSize >= properties.dataPageSize
        }
    }
}

// MARK: - Float Column Writer

/// Writer for Float columns
public final class FloatColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Float] = []
    private var definitionLevelBuffer: [UInt16] = []  // W5: Track nulls
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private let isNullable: Bool
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []
    private var statisticsAccumulator: FloatStatisticsAccumulator?  // W6

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.isNullable = !column.isRequired
        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = FloatStatisticsAccumulator()
        }
    }

    /// Write a batch of Float values (for required columns)
    public func writeValues(_ values: [Float]) throws {
        guard !isNullable else {
            throw WriterError.invalidState("Column \(column.name) is nullable, use writeOptionalValues()")
        }

        statisticsAccumulator?.update(values)  // W6
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    /// Write a batch of optional Float values (for nullable columns)
    public func writeOptionalValues(_ values: [Float?]) throws {
        guard isNullable else {
            throw WriterError.invalidState("Column \(column.name) is required, use writeValues()")
        }

        statisticsAccumulator?.updateNullable(values)  // W6
        for value in values {
            if let nonNullValue = value {
                definitionLevelBuffer.append(1)
                valueBuffer.append(nonNullValue)
            } else {
                definitionLevelBuffer.append(0)
            }
        }

        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty || (isNullable && !definitionLevelBuffer.isEmpty) else {
            return
        }

        // Encode definition levels if column is nullable
        let definitionLevelsData: Data?
        if isNullable {
            let encoder = LevelEncoder(maxLevel: 1)
            encoder.encode(definitionLevelBuffer)
            definitionLevelsData = encoder.flush()
            usedEncodings.insert(.rle)
        } else {
            definitionLevelsData = nil
        }

        let encoder = PlainEncoder<Float>()
        encoder.encode(valueBuffer)

        let numValues = Int32(isNullable ? definitionLevelBuffer.count : valueBuffer.count)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)

        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        let statistics = statisticsAccumulator?.build()  // W6

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: 0,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: Array(usedEncodings),
            codec: properties.compression(for: column.name),
            statistics: statistics
        )
    }

    private func shouldFlush() -> Bool {
        if isNullable {
            // For nullable columns, check both value buffer and definition level buffer
            let valueSize = valueBuffer.count * 4  // 4 bytes per Float
            // Definition levels: conservative estimate (4-byte length + ~1 bit per level)
            let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
            let estimatedSize = valueSize + defLevelSize
            return estimatedSize >= properties.dataPageSize
        } else {
            let estimatedSize = valueBuffer.count * 4  // 4 bytes per Float
            return estimatedSize >= properties.dataPageSize
        }
    }
}

// MARK: - Double Column Writer

/// Writer for Double columns
public final class DoubleColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Double] = []
    private var definitionLevelBuffer: [UInt16] = []  // W5: Track nulls
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private let isNullable: Bool
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []
    private var statisticsAccumulator: DoubleStatisticsAccumulator?  // W6

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.isNullable = !column.isRequired
        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = DoubleStatisticsAccumulator()
        }
    }

    /// Write a batch of Double values (for required columns)
    public func writeValues(_ values: [Double]) throws {
        guard !isNullable else {
            throw WriterError.invalidState("Column \(column.name) is nullable, use writeOptionalValues()")
        }

        statisticsAccumulator?.update(values)  // W6
        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    /// Write a batch of optional Double values (for nullable columns)
    public func writeOptionalValues(_ values: [Double?]) throws {
        guard isNullable else {
            throw WriterError.invalidState("Column \(column.name) is required, use writeValues()")
        }

        statisticsAccumulator?.updateNullable(values)  // W6
        for value in values {
            if let nonNullValue = value {
                definitionLevelBuffer.append(1)
                valueBuffer.append(nonNullValue)
            } else {
                definitionLevelBuffer.append(0)
            }
        }

        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        guard !valueBuffer.isEmpty || (isNullable && !definitionLevelBuffer.isEmpty) else {
            return
        }

        // Encode definition levels if column is nullable
        let definitionLevelsData: Data?
        if isNullable {
            let encoder = LevelEncoder(maxLevel: 1)
            encoder.encode(definitionLevelBuffer)
            definitionLevelsData = encoder.flush()
            usedEncodings.insert(.rle)
        } else {
            definitionLevelsData = nil
        }

        let encoder = PlainEncoder<Double>()
        encoder.encode(valueBuffer)

        let numValues = Int32(isNullable ? definitionLevelBuffer.count : valueBuffer.count)

        let result = try pageWriter.writeDataPage(
            values: encoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)

        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        let statistics = statisticsAccumulator?.build()  // W6

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: 0,
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: Array(usedEncodings),
            codec: properties.compression(for: column.name),
            statistics: statistics
        )
    }

    private func shouldFlush() -> Bool {
        if isNullable {
            // For nullable columns, check both value buffer and definition level buffer
            let valueSize = valueBuffer.count * 8  // 8 bytes per Double
            // Definition levels: conservative estimate (4-byte length + ~1 bit per level)
            let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
            let estimatedSize = valueSize + defLevelSize
            return estimatedSize >= properties.dataPageSize
        } else {
            let estimatedSize = valueBuffer.count * 8  // 8 bytes per Double
            return estimatedSize >= properties.dataPageSize
        }
    }
}

// MARK: - String Column Writer

/// Writer for String columns (UTF-8 byte arrays)
public final class StringColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [String] = []
    private var definitionLevelBuffer: [UInt16] = []  // W4: Track nulls (0=null, 1=present)
    private var totalValues: Int64 = 0
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var dictionaryPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private let isNullable: Bool

    // Dictionary encoding support
    private var dictionaryEncoder: DictionaryEncoder<String>?
    private var usedEncodings: Set<Encoding> = []

    // W6: Statistics tracking
    private var statisticsAccumulator: StringStatisticsAccumulator?

    init(column: Column, properties: WriterProperties, pageWriter: PageWriter, startOffset: Int64) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.isNullable = !column.isRequired

        // Enable dictionary encoding if configured
        if properties.dictionaryEnabled(for: column.name) {
            self.dictionaryEncoder = DictionaryEncoder<String>()
        }

        // W6: Enable statistics if configured
        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = StringStatisticsAccumulator()
        }
    }

    /// Write a batch of String values (for required columns)
    public func writeValues(_ values: [String]) throws {
        guard !isNullable else {
            throw WriterError.invalidState("Column \(column.name) is nullable, use writeOptionalValues()")
        }

        statisticsAccumulator?.update(values)  // W6

        // Feed values to dictionary encoder if enabled
        if let dictEncoder = dictionaryEncoder {
            try dictEncoder.encode(values)
        }

        valueBuffer.append(contentsOf: values)
        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    /// Write a batch of optional String values (for nullable columns)
    /// - Parameter values: Array of optional strings (nil = null)
    public func writeOptionalValues(_ values: [String?]) throws {
        guard isNullable else {
            throw WriterError.invalidState("Column \(column.name) is required, use writeValues()")
        }

        statisticsAccumulator?.updateNullable(values)  // W6

        // Encode definition levels and non-null values
        for value in values {
            if let nonNullValue = value {
                // Present value: def level = 1
                definitionLevelBuffer.append(1)
                valueBuffer.append(nonNullValue)

                // Feed to dictionary encoder if enabled
                if let dictEncoder = dictionaryEncoder {
                    try dictEncoder.encodeOne(nonNullValue)
                }
            } else {
                // Null value: def level = 0
                definitionLevelBuffer.append(0)
                // No value added to valueBuffer
            }
        }

        totalValues += Int64(values.count)

        if shouldFlush() {
            try flush()
        }
    }

    func flush() throws {
        // For nullable columns, allow flush if we have definition levels (even with no values)
        // For required columns, need at least one value
        guard !valueBuffer.isEmpty || (isNullable && !definitionLevelBuffer.isEmpty) else {
            return
        }

        // Write dictionary page before first data page if using dictionary encoding
        if let dictEncoder = dictionaryEncoder, dictionaryPageOffset == nil, dictEncoder.shouldUseDictionary {
            try writeDictionaryPage(dictEncoder)
        }

        // Encode definition levels if column is nullable
        let definitionLevelsData: Data?
        if isNullable {
            let encoder = LevelEncoder(maxLevel: 1)  // maxLevel=1 for optional columns
            encoder.encode(definitionLevelBuffer)
            definitionLevelsData = encoder.flush()
            usedEncodings.insert(.rle)  // Track RLE encoding used for definition levels
        } else {
            definitionLevelsData = nil
        }

        // Encode data page values
        let (encodedData, encoding) = try encodeDataPage()

        // Number of values = total rows (including nulls for nullable columns)
        let numValues = Int32(isNullable ? definitionLevelBuffer.count : valueBuffer.count)

        let result = try pageWriter.writeDataPage(
            values: encodedData,
            numValues: numValues,
            encoding: encoding,
            definitionLevels: definitionLevelsData
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        // Accumulate sizes (including page headers per Parquet spec)
        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(encoding)

        // Clear page indices after successful flush (Bug fix: prevents duplicate rows)
        if let dictEncoder = dictionaryEncoder, dictEncoder.shouldUseDictionary {
            dictEncoder.clearPageIndices()
        }

        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    private func writeDictionaryPage(_ dictEncoder: DictionaryEncoder<String>) throws {
        let dictData = try dictEncoder.dictionaryData()
        let numDictValues = Int32(dictEncoder.dictionaryCount)

        let result = try pageWriter.writeDictionaryPage(
            dictionary: dictData,
            numValues: numDictValues,
            encoding: .plain
        )

        dictionaryPageOffset = result.startOffset

        // Accumulate dictionary page size
        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)  // Dictionary uses PLAIN
        usedEncodings.insert(.rleDictionary)  // Data pages use RLE_DICTIONARY
    }

    private func encodeDataPage() throws -> (Data, Encoding) {
        // Use dictionary encoding if available and beneficial
        if let dictEncoder = dictionaryEncoder, dictEncoder.shouldUseDictionary {
            let indicesData = dictEncoder.indicesData()
            return (indicesData, .rleDictionary)
        }

        // Fall back to PLAIN encoding
        let encoder = PlainEncoder<String>()
        try encoder.encode(valueBuffer)
        return (encoder.data, .plain)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        let statistics = statisticsAccumulator?.build()  // W6

        return WriterColumnChunkMetadata(
            column: column,
            fileOffset: 0,  // Per spec: 0 when metadata is in footer (deprecated field)
            dataPageOffset: dataPageOffset,
            dictionaryPageOffset: dictionaryPageOffset,
            numValues: totalValues,
            totalCompressedSize: totalCompressedSize,
            totalUncompressedSize: totalUncompressedSize,
            encodings: Array(usedEncodings),
            codec: properties.compression(for: column.name),
            statistics: statistics
        )
    }

    private func shouldFlush() -> Bool {
        if isNullable {
            // For nullable columns, check both value buffer and definition level buffer
            let valueSize = valueBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
            // Definition levels: conservative estimate (4-byte length + ~1 bit per level)
            let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
            let estimatedSize = valueSize + defLevelSize
            return estimatedSize >= properties.dataPageSize
        } else {
            // Estimate size: 4-byte length + string bytes
            let estimatedSize = valueBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
            return estimatedSize >= properties.dataPageSize
        }
    }
}
