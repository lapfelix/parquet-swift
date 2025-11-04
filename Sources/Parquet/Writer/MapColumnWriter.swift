// MapColumnWriter.swift - Map column writers for Parquet
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Writer for map<string, int32> columns.
///
/// Maps in Parquet are represented as repeated groups of key-value pairs:
/// ```
/// map<K, V> =
///   repeated group map {
///     required K key;
///     optional V value;
///   }
/// ```
///
/// This writer handles the flattening of Swift dictionaries into key-value pairs
/// and writes pages directly to PageWriter for both key and value columns.
public final class StringInt32MapColumnWriter {
    private let keyColumn: Column
    private let valueColumn: Column
    private let properties: WriterProperties

    private let keyPageWriter: PageWriter
    private let valuePageWriter: PageWriter

    // Buffers for key and value data
    private var keyBuffer: [String] = []
    private var valueBuffer: [Int32?] = []

    // Shared repetition levels, separate definition levels
    private var repetitionLevelBuffer: [UInt16] = []
    private var keyDefinitionLevelBuffer: [UInt16] = []
    private var valueDefinitionLevelBuffer: [UInt16] = []

    private var rowCount: Int64 = 0
    private var totalValues: Int64 = 0

    private let keyMaxDefinitionLevel: Int
    private let valueMaxDefinitionLevel: Int
    private let maxRepetitionLevel: Int
    private let repeatedAncestorDefLevel: Int
    private let nullMapDefLevel: Int

    // Track metadata for both column chunks
    private var keyDataPageOffset: Int64?
    private var keyTotalCompressedSize: Int64 = 0
    private var keyTotalUncompressedSize: Int64 = 0
    private var keyUsedEncodings: Set<Encoding> = []

    private var valueDataPageOffset: Int64?
    private var valueTotalCompressedSize: Int64 = 0
    private var valueTotalUncompressedSize: Int64 = 0
    private var valueUsedEncodings: Set<Encoding> = []

    private var keyStatisticsAccumulator: StringStatisticsAccumulator?
    private var valueStatisticsAccumulator: Int32StatisticsAccumulator?

    init(
        column: Column,
        properties: WriterProperties,
        sink: OutputSink,
        startOffset: Int64,
        keyMaxDefinitionLevel: Int,
        valueMaxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullMapDefLevel: Int,
        keyColumn: Column,
        valueColumn: Column
    ) {
        self.keyColumn = keyColumn
        self.valueColumn = valueColumn
        self.properties = properties
        self.keyMaxDefinitionLevel = keyMaxDefinitionLevel
        self.valueMaxDefinitionLevel = valueMaxDefinitionLevel
        self.maxRepetitionLevel = maxRepetitionLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
        self.nullMapDefLevel = nullMapDefLevel

        // Create separate page writers for key and value columns
        let keyCodec = properties.compression(for: keyColumn.name)
        self.keyPageWriter = PageWriter(sink: sink, codec: keyCodec, properties: properties)

        let valueCodec = properties.compression(for: valueColumn.name)
        self.valuePageWriter = PageWriter(sink: sink, codec: valueCodec, properties: properties)

        // Initialize statistics accumulators if enabled
        if properties.statisticsEnabled(for: keyColumn.name) {
            self.keyStatisticsAccumulator = StringStatisticsAccumulator()
        }
        if properties.statisticsEnabled(for: valueColumn.name) {
            self.valueStatisticsAccumulator = Int32StatisticsAccumulator()
        }
    }

    /// Write an array of maps.
    ///
    /// - Parameter maps: Array of optional dictionaries
    /// - Returns: Number of maps written
    /// - Throws: WriterError if write fails
    @discardableResult
    public func writeMaps(_ maps: [[String: Int32]?]) throws -> Int {
        // Flatten maps to key-value pairs with levels
        for map in maps {
            if map == nil {
                // NULL map: emit one level entry with no values
                repetitionLevelBuffer.append(0)
                keyDefinitionLevelBuffer.append(UInt16(nullMapDefLevel))
                valueDefinitionLevelBuffer.append(UInt16(nullMapDefLevel))
                continue
            }

            let unwrappedMap = map!

            if unwrappedMap.isEmpty {
                // Empty map: emit one level entry with no values
                repetitionLevelBuffer.append(0)
                keyDefinitionLevelBuffer.append(UInt16(repeatedAncestorDefLevel))
                valueDefinitionLevelBuffer.append(UInt16(repeatedAncestorDefLevel))
                continue
            }

            // Map with entries: emit one (rep, def, key, value) tuple per entry
            // Sort keys for deterministic output
            let sortedKeys = unwrappedMap.keys.sorted()

            for (index, key) in sortedKeys.enumerated() {
                let value = unwrappedMap[key]!

                keyBuffer.append(key)
                valueBuffer.append(value)

                if index == 0 {
                    // First entry: new map boundary
                    repetitionLevelBuffer.append(0)
                } else {
                    // Continuation: same map, additional entry
                    repetitionLevelBuffer.append(UInt16(maxRepetitionLevel))
                }

                // Key definition level: keys are required, so always present at this point
                keyDefinitionLevelBuffer.append(UInt16(keyMaxDefinitionLevel))

                // Value definition level: depends on whether value is NULL
                let valueDefLevel = value != nil ? valueMaxDefinitionLevel : valueMaxDefinitionLevel - 1
                valueDefinitionLevelBuffer.append(UInt16(valueDefLevel))
            }
        }

        // Update statistics (only for non-null values)
        keyStatisticsAccumulator?.update(keyBuffer)
        let nonNullValues = valueBuffer.compactMap { $0 }
        valueStatisticsAccumulator?.update(nonNullValues)

        totalValues += Int64(repetitionLevelBuffer.count)
        rowCount += Int64(maps.count)

        if shouldFlush() {
            try flush()
        }

        return maps.count
    }

    /// Get the number of rows written (for row group metadata)
    var numRows: Int64 {
        return rowCount
    }

    private func flush() throws {
        guard !repetitionLevelBuffer.isEmpty else {
            return
        }

        // Encode shared repetition levels
        let repEncoder = LevelEncoder(maxLevel: maxRepetitionLevel)
        repEncoder.encode(repetitionLevelBuffer)
        let repetitionLevelsData = repEncoder.flush()

        // Encode separate definition levels for keys and values
        let keyDefEncoder = LevelEncoder(maxLevel: keyMaxDefinitionLevel)
        keyDefEncoder.encode(keyDefinitionLevelBuffer)
        let keyDefinitionLevelsData = keyDefEncoder.flush()

        let valueDefEncoder = LevelEncoder(maxLevel: valueMaxDefinitionLevel)
        valueDefEncoder.encode(valueDefinitionLevelBuffer)
        let valueDefinitionLevelsData = valueDefEncoder.flush()

        let numValues = Int32(repetitionLevelBuffer.count)

        // Write key page
        let keyEncoder = PlainEncoder<String>()
        try keyEncoder.encode(keyBuffer)

        let keyResult = try keyPageWriter.writeDataPage(
            values: keyEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: keyDefinitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if keyDataPageOffset == nil {
            keyDataPageOffset = keyResult.startOffset
        }

        let keyHeaderLength = keyResult.bytesWritten - keyResult.compressedSize
        keyTotalCompressedSize += Int64(keyResult.compressedSize + keyHeaderLength)
        keyTotalUncompressedSize += Int64(keyResult.uncompressedSize + keyHeaderLength)
        keyUsedEncodings.insert(.plain)
        keyUsedEncodings.insert(.rle)

        // Write value page
        let valueEncoder = PlainEncoder<Int32>()

        // For optional values, encode all values (including NULLs as 0)
        // The def levels indicate which are actually NULL
        let valuesToEncode = valueBuffer.map { $0 ?? 0 }
        try valueEncoder.encode(valuesToEncode)

        let valueResult = try valuePageWriter.writeDataPage(
            values: valueEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: valueDefinitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if valueDataPageOffset == nil {
            valueDataPageOffset = valueResult.startOffset
        }

        let valueHeaderLength = valueResult.bytesWritten - valueResult.compressedSize
        valueTotalCompressedSize += Int64(valueResult.compressedSize + valueHeaderLength)
        valueTotalUncompressedSize += Int64(valueResult.uncompressedSize + valueHeaderLength)
        valueUsedEncodings.insert(.plain)
        valueUsedEncodings.insert(.rle)

        // Clear buffers
        keyBuffer.removeAll(keepingCapacity: true)
        valueBuffer.removeAll(keepingCapacity: true)
        repetitionLevelBuffer.removeAll(keepingCapacity: true)
        keyDefinitionLevelBuffer.removeAll(keepingCapacity: true)
        valueDefinitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    /// Close the map writer and return metadata for key and value columns
    /// - Returns: Tuple of (key metadata, value metadata)
    /// - Throws: WriterError if close fails
    func close() throws -> (key: WriterColumnChunkMetadata, value: WriterColumnChunkMetadata) {
        try flush()

        guard let keyDataPageOffset = keyDataPageOffset else {
            throw WriterError.invalidState("No data pages written for key column \(keyColumn.name)")
        }

        guard let valueDataPageOffset = valueDataPageOffset else {
            throw WriterError.invalidState("No data pages written for value column \(valueColumn.name)")
        }

        let keyStatistics = keyStatisticsAccumulator?.build()
        let valueStatistics = valueStatisticsAccumulator?.build()

        let keyMetadata = WriterColumnChunkMetadata(
            column: keyColumn,
            fileOffset: 0,
            dataPageOffset: keyDataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: keyTotalCompressedSize,
            totalUncompressedSize: keyTotalUncompressedSize,
            encodings: Array(keyUsedEncodings),
            codec: properties.compression(for: keyColumn.name),
            statistics: keyStatistics
        )

        let valueMetadata = WriterColumnChunkMetadata(
            column: valueColumn,
            fileOffset: 0,
            dataPageOffset: valueDataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: valueTotalCompressedSize,
            totalUncompressedSize: valueTotalUncompressedSize,
            encodings: Array(valueUsedEncodings),
            codec: properties.compression(for: valueColumn.name),
            statistics: valueStatistics
        )

        return (keyMetadata, valueMetadata)
    }

    private func shouldFlush() -> Bool {
        // Estimate page size based on buffered data
        let keySize = keyBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
        let valueSize = valueBuffer.count * 4
        let defLevelSize = 4 + (keyDefinitionLevelBuffer.count + 7) / 8
        let repLevelSize = 4 + (repetitionLevelBuffer.count + 7) / 8
        let estimatedSize = keySize + valueSize + defLevelSize + repLevelSize
        return estimatedSize >= properties.dataPageSize
    }
}

/// Writer for map<string, int64> columns.
public final class StringInt64MapColumnWriter {
    private let keyColumn: Column
    private let valueColumn: Column
    private let properties: WriterProperties

    private let keyPageWriter: PageWriter
    private let valuePageWriter: PageWriter

    private var keyBuffer: [String] = []
    private var valueBuffer: [Int64?] = []
    private var repetitionLevelBuffer: [UInt16] = []
    private var keyDefinitionLevelBuffer: [UInt16] = []
    private var valueDefinitionLevelBuffer: [UInt16] = []

    private var rowCount: Int64 = 0
    private var totalValues: Int64 = 0

    private let keyMaxDefinitionLevel: Int
    private let valueMaxDefinitionLevel: Int
    private let maxRepetitionLevel: Int
    private let repeatedAncestorDefLevel: Int
    private let nullMapDefLevel: Int

    private var keyDataPageOffset: Int64?
    private var keyTotalCompressedSize: Int64 = 0
    private var keyTotalUncompressedSize: Int64 = 0
    private var keyUsedEncodings: Set<Encoding> = []

    private var valueDataPageOffset: Int64?
    private var valueTotalCompressedSize: Int64 = 0
    private var valueTotalUncompressedSize: Int64 = 0
    private var valueUsedEncodings: Set<Encoding> = []

    private var keyStatisticsAccumulator: StringStatisticsAccumulator?
    private var valueStatisticsAccumulator: Int64StatisticsAccumulator?

    init(
        column: Column,
        properties: WriterProperties,
        sink: OutputSink,
        startOffset: Int64,
        keyMaxDefinitionLevel: Int,
        valueMaxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullMapDefLevel: Int,
        keyColumn: Column,
        valueColumn: Column
    ) {
        self.keyColumn = keyColumn
        self.valueColumn = valueColumn
        self.properties = properties
        self.keyMaxDefinitionLevel = keyMaxDefinitionLevel
        self.valueMaxDefinitionLevel = valueMaxDefinitionLevel
        self.maxRepetitionLevel = maxRepetitionLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
        self.nullMapDefLevel = nullMapDefLevel

        let keyCodec = properties.compression(for: keyColumn.name)
        self.keyPageWriter = PageWriter(sink: sink, codec: keyCodec, properties: properties)

        let valueCodec = properties.compression(for: valueColumn.name)
        self.valuePageWriter = PageWriter(sink: sink, codec: valueCodec, properties: properties)

        if properties.statisticsEnabled(for: keyColumn.name) {
            self.keyStatisticsAccumulator = StringStatisticsAccumulator()
        }
        if properties.statisticsEnabled(for: valueColumn.name) {
            self.valueStatisticsAccumulator = Int64StatisticsAccumulator()
        }
    }

    @discardableResult
    public func writeMaps(_ maps: [[String: Int64]?]) throws -> Int {
        for map in maps {
            if map == nil {
                repetitionLevelBuffer.append(0)
                keyDefinitionLevelBuffer.append(UInt16(nullMapDefLevel))
                valueDefinitionLevelBuffer.append(UInt16(nullMapDefLevel))
                continue
            }

            let unwrappedMap = map!

            if unwrappedMap.isEmpty {
                repetitionLevelBuffer.append(0)
                keyDefinitionLevelBuffer.append(UInt16(repeatedAncestorDefLevel))
                valueDefinitionLevelBuffer.append(UInt16(repeatedAncestorDefLevel))
                continue
            }

            let sortedKeys = unwrappedMap.keys.sorted()

            for (index, key) in sortedKeys.enumerated() {
                let value = unwrappedMap[key]!

                keyBuffer.append(key)
                valueBuffer.append(value)

                repetitionLevelBuffer.append(index == 0 ? 0 : UInt16(maxRepetitionLevel))

                // Key definition level: keys are required, so always present
                keyDefinitionLevelBuffer.append(UInt16(keyMaxDefinitionLevel))

                // Value definition level: depends on whether value is NULL
                let valueDefLevel = value != nil ? valueMaxDefinitionLevel : valueMaxDefinitionLevel - 1
                valueDefinitionLevelBuffer.append(UInt16(valueDefLevel))
            }
        }

        keyStatisticsAccumulator?.update(keyBuffer)
        let nonNullValues = valueBuffer.compactMap { $0 }
        valueStatisticsAccumulator?.update(nonNullValues)

        totalValues += Int64(repetitionLevelBuffer.count)
        rowCount += Int64(maps.count)

        if shouldFlush() {
            try flush()
        }

        return maps.count
    }

    var numRows: Int64 {
        return rowCount
    }

    private func flush() throws {
        guard !repetitionLevelBuffer.isEmpty else {
            return
        }

        // Encode shared repetition levels
        let repEncoder = LevelEncoder(maxLevel: maxRepetitionLevel)
        repEncoder.encode(repetitionLevelBuffer)
        let repetitionLevelsData = repEncoder.flush()

        // Encode separate definition levels for keys and values
        let keyDefEncoder = LevelEncoder(maxLevel: keyMaxDefinitionLevel)
        keyDefEncoder.encode(keyDefinitionLevelBuffer)
        let keyDefinitionLevelsData = keyDefEncoder.flush()

        let valueDefEncoder = LevelEncoder(maxLevel: valueMaxDefinitionLevel)
        valueDefEncoder.encode(valueDefinitionLevelBuffer)
        let valueDefinitionLevelsData = valueDefEncoder.flush()

        let numValues = Int32(repetitionLevelBuffer.count)

        // Write key page
        let keyEncoder = PlainEncoder<String>()
        try keyEncoder.encode(keyBuffer)

        let keyResult = try keyPageWriter.writeDataPage(
            values: keyEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: keyDefinitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if keyDataPageOffset == nil {
            keyDataPageOffset = keyResult.startOffset
        }

        let keyHeaderLength = keyResult.bytesWritten - keyResult.compressedSize
        keyTotalCompressedSize += Int64(keyResult.compressedSize + keyHeaderLength)
        keyTotalUncompressedSize += Int64(keyResult.uncompressedSize + keyHeaderLength)
        keyUsedEncodings.insert(.plain)
        keyUsedEncodings.insert(.rle)

        // Write value page
        let valueEncoder = PlainEncoder<Int64>()
        let valuesToEncode = valueBuffer.map { $0 ?? 0 }
        try valueEncoder.encode(valuesToEncode)

        let valueResult = try valuePageWriter.writeDataPage(
            values: valueEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: valueDefinitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if valueDataPageOffset == nil {
            valueDataPageOffset = valueResult.startOffset
        }

        let valueHeaderLength = valueResult.bytesWritten - valueResult.compressedSize
        valueTotalCompressedSize += Int64(valueResult.compressedSize + valueHeaderLength)
        valueTotalUncompressedSize += Int64(valueResult.uncompressedSize + valueHeaderLength)
        valueUsedEncodings.insert(.plain)
        valueUsedEncodings.insert(.rle)

        keyBuffer.removeAll(keepingCapacity: true)
        valueBuffer.removeAll(keepingCapacity: true)
        repetitionLevelBuffer.removeAll(keepingCapacity: true)
        keyDefinitionLevelBuffer.removeAll(keepingCapacity: true)
        valueDefinitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> (key: WriterColumnChunkMetadata, value: WriterColumnChunkMetadata) {
        try flush()

        guard let keyDataPageOffset = keyDataPageOffset else {
            throw WriterError.invalidState("No data pages written for key column \(keyColumn.name)")
        }

        guard let valueDataPageOffset = valueDataPageOffset else {
            throw WriterError.invalidState("No data pages written for value column \(valueColumn.name)")
        }

        let keyStatistics = keyStatisticsAccumulator?.build()
        let valueStatistics = valueStatisticsAccumulator?.build()

        let keyMetadata = WriterColumnChunkMetadata(
            column: keyColumn,
            fileOffset: 0,
            dataPageOffset: keyDataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: keyTotalCompressedSize,
            totalUncompressedSize: keyTotalUncompressedSize,
            encodings: Array(keyUsedEncodings),
            codec: properties.compression(for: keyColumn.name),
            statistics: keyStatistics
        )

        let valueMetadata = WriterColumnChunkMetadata(
            column: valueColumn,
            fileOffset: 0,
            dataPageOffset: valueDataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: valueTotalCompressedSize,
            totalUncompressedSize: valueTotalUncompressedSize,
            encodings: Array(valueUsedEncodings),
            codec: properties.compression(for: valueColumn.name),
            statistics: valueStatistics
        )

        return (keyMetadata, valueMetadata)
    }

    private func shouldFlush() -> Bool {
        let keySize = keyBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
        let valueSize = valueBuffer.count * 8
        let defLevelSize = 4 + (keyDefinitionLevelBuffer.count + 7) / 8
        let repLevelSize = 4 + (repetitionLevelBuffer.count + 7) / 8
        let estimatedSize = keySize + valueSize + defLevelSize + repLevelSize
        return estimatedSize >= properties.dataPageSize
    }
}

/// Writer for map<string, string> columns.
public final class StringStringMapColumnWriter {
    private let keyColumn: Column
    private let valueColumn: Column
    private let properties: WriterProperties

    private let keyPageWriter: PageWriter
    private let valuePageWriter: PageWriter

    private var keyBuffer: [String] = []
    private var valueBuffer: [String?] = []
    private var repetitionLevelBuffer: [UInt16] = []
    private var keyDefinitionLevelBuffer: [UInt16] = []
    private var valueDefinitionLevelBuffer: [UInt16] = []

    private var rowCount: Int64 = 0
    private var totalValues: Int64 = 0

    private let keyMaxDefinitionLevel: Int
    private let valueMaxDefinitionLevel: Int
    private let maxRepetitionLevel: Int
    private let repeatedAncestorDefLevel: Int
    private let nullMapDefLevel: Int

    private var keyDataPageOffset: Int64?
    private var keyTotalCompressedSize: Int64 = 0
    private var keyTotalUncompressedSize: Int64 = 0
    private var keyUsedEncodings: Set<Encoding> = []

    private var valueDataPageOffset: Int64?
    private var valueTotalCompressedSize: Int64 = 0
    private var valueTotalUncompressedSize: Int64 = 0
    private var valueUsedEncodings: Set<Encoding> = []

    private var keyStatisticsAccumulator: StringStatisticsAccumulator?
    private var valueStatisticsAccumulator: StringStatisticsAccumulator?

    init(
        column: Column,
        properties: WriterProperties,
        sink: OutputSink,
        startOffset: Int64,
        keyMaxDefinitionLevel: Int,
        valueMaxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullMapDefLevel: Int,
        keyColumn: Column,
        valueColumn: Column
    ) {
        self.keyColumn = keyColumn
        self.valueColumn = valueColumn
        self.properties = properties
        self.keyMaxDefinitionLevel = keyMaxDefinitionLevel
        self.valueMaxDefinitionLevel = valueMaxDefinitionLevel
        self.maxRepetitionLevel = maxRepetitionLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
        self.nullMapDefLevel = nullMapDefLevel

        let keyCodec = properties.compression(for: keyColumn.name)
        self.keyPageWriter = PageWriter(sink: sink, codec: keyCodec, properties: properties)

        let valueCodec = properties.compression(for: valueColumn.name)
        self.valuePageWriter = PageWriter(sink: sink, codec: valueCodec, properties: properties)

        if properties.statisticsEnabled(for: keyColumn.name) {
            self.keyStatisticsAccumulator = StringStatisticsAccumulator()
        }
        if properties.statisticsEnabled(for: valueColumn.name) {
            self.valueStatisticsAccumulator = StringStatisticsAccumulator()
        }
    }

    @discardableResult
    public func writeMaps(_ maps: [[String: String]?]) throws -> Int {
        for map in maps {
            if map == nil {
                repetitionLevelBuffer.append(0)
                keyDefinitionLevelBuffer.append(UInt16(nullMapDefLevel))
                valueDefinitionLevelBuffer.append(UInt16(nullMapDefLevel))
                continue
            }

            let unwrappedMap = map!

            if unwrappedMap.isEmpty {
                repetitionLevelBuffer.append(0)
                keyDefinitionLevelBuffer.append(UInt16(repeatedAncestorDefLevel))
                valueDefinitionLevelBuffer.append(UInt16(repeatedAncestorDefLevel))
                continue
            }

            let sortedKeys = unwrappedMap.keys.sorted()

            for (index, key) in sortedKeys.enumerated() {
                let value = unwrappedMap[key]!

                keyBuffer.append(key)
                valueBuffer.append(value)

                repetitionLevelBuffer.append(index == 0 ? 0 : UInt16(maxRepetitionLevel))

                // Key definition level: keys are required, so always present
                keyDefinitionLevelBuffer.append(UInt16(keyMaxDefinitionLevel))

                // Value definition level: depends on whether value is NULL
                let valueDefLevel = value != nil ? valueMaxDefinitionLevel : valueMaxDefinitionLevel - 1
                valueDefinitionLevelBuffer.append(UInt16(valueDefLevel))
            }
        }

        keyStatisticsAccumulator?.update(keyBuffer)
        let nonNullValues = valueBuffer.compactMap { $0 }
        valueStatisticsAccumulator?.update(nonNullValues)

        totalValues += Int64(repetitionLevelBuffer.count)
        rowCount += Int64(maps.count)

        if shouldFlush() {
            try flush()
        }

        return maps.count
    }

    var numRows: Int64 {
        return rowCount
    }

    private func flush() throws {
        guard !repetitionLevelBuffer.isEmpty else {
            return
        }

        // Encode shared repetition levels
        let repEncoder = LevelEncoder(maxLevel: maxRepetitionLevel)
        repEncoder.encode(repetitionLevelBuffer)
        let repetitionLevelsData = repEncoder.flush()

        // Encode separate definition levels for keys and values
        let keyDefEncoder = LevelEncoder(maxLevel: keyMaxDefinitionLevel)
        keyDefEncoder.encode(keyDefinitionLevelBuffer)
        let keyDefinitionLevelsData = keyDefEncoder.flush()

        let valueDefEncoder = LevelEncoder(maxLevel: valueMaxDefinitionLevel)
        valueDefEncoder.encode(valueDefinitionLevelBuffer)
        let valueDefinitionLevelsData = valueDefEncoder.flush()

        let numValues = Int32(repetitionLevelBuffer.count)

        // Write key page
        let keyEncoder = PlainEncoder<String>()
        try keyEncoder.encode(keyBuffer)

        let keyResult = try keyPageWriter.writeDataPage(
            values: keyEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: keyDefinitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if keyDataPageOffset == nil {
            keyDataPageOffset = keyResult.startOffset
        }

        let keyHeaderLength = keyResult.bytesWritten - keyResult.compressedSize
        keyTotalCompressedSize += Int64(keyResult.compressedSize + keyHeaderLength)
        keyTotalUncompressedSize += Int64(keyResult.uncompressedSize + keyHeaderLength)
        keyUsedEncodings.insert(.plain)
        keyUsedEncodings.insert(.rle)

        // Write value page
        let valueEncoder = PlainEncoder<String>()
        let valuesToEncode = valueBuffer.map { $0 ?? "" }  // NULL strings encoded as empty
        try valueEncoder.encode(valuesToEncode)

        let valueResult = try valuePageWriter.writeDataPage(
            values: valueEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: valueDefinitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if valueDataPageOffset == nil {
            valueDataPageOffset = valueResult.startOffset
        }

        let valueHeaderLength = valueResult.bytesWritten - valueResult.compressedSize
        valueTotalCompressedSize += Int64(valueResult.compressedSize + valueHeaderLength)
        valueTotalUncompressedSize += Int64(valueResult.uncompressedSize + valueHeaderLength)
        valueUsedEncodings.insert(.plain)
        valueUsedEncodings.insert(.rle)

        keyBuffer.removeAll(keepingCapacity: true)
        valueBuffer.removeAll(keepingCapacity: true)
        repetitionLevelBuffer.removeAll(keepingCapacity: true)
        keyDefinitionLevelBuffer.removeAll(keepingCapacity: true)
        valueDefinitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> (key: WriterColumnChunkMetadata, value: WriterColumnChunkMetadata) {
        try flush()

        guard let keyDataPageOffset = keyDataPageOffset else {
            throw WriterError.invalidState("No data pages written for key column \(keyColumn.name)")
        }

        guard let valueDataPageOffset = valueDataPageOffset else {
            throw WriterError.invalidState("No data pages written for value column \(valueColumn.name)")
        }

        let keyStatistics = keyStatisticsAccumulator?.build()
        let valueStatistics = valueStatisticsAccumulator?.build()

        let keyMetadata = WriterColumnChunkMetadata(
            column: keyColumn,
            fileOffset: 0,
            dataPageOffset: keyDataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: keyTotalCompressedSize,
            totalUncompressedSize: keyTotalUncompressedSize,
            encodings: Array(keyUsedEncodings),
            codec: properties.compression(for: keyColumn.name),
            statistics: keyStatistics
        )

        let valueMetadata = WriterColumnChunkMetadata(
            column: valueColumn,
            fileOffset: 0,
            dataPageOffset: valueDataPageOffset,
            dictionaryPageOffset: nil,
            numValues: totalValues,
            totalCompressedSize: valueTotalCompressedSize,
            totalUncompressedSize: valueTotalUncompressedSize,
            encodings: Array(valueUsedEncodings),
            codec: properties.compression(for: valueColumn.name),
            statistics: valueStatistics
        )

        return (keyMetadata, valueMetadata)
    }

    private func shouldFlush() -> Bool {
        let keySize = keyBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
        let valueSize = valueBuffer.reduce(0) { $0 + 4 + ($1?.utf8.count ?? 0) }
        let defLevelSize = 4 + (keyDefinitionLevelBuffer.count + 7) / 8
        let repLevelSize = 4 + (repetitionLevelBuffer.count + 7) / 8
        let estimatedSize = keySize + valueSize + defLevelSize + repLevelSize
        return estimatedSize >= properties.dataPageSize
    }
}
