// ListColumnWriter.swift - Column writers for list (repeated) types
//
// Licensed under the Apache License, Version 2.0

import Foundation

// MARK: - Int32 List Column Writer

/// Writer for list<int32> columns (repeated int32)
///
/// Handles single-level lists like `[[Int32]]` and `[[Int32?]]`.
///
/// **Usage**:
/// ```swift
/// let writer = try rowGroup.int32ListColumn(at: columnIndex)
/// try writer.writeValues([[1, 2], [3], []])
/// ```
///
/// **Level Computation**:
/// Uses `LevelComputer` to flatten nested lists into:
/// - Flat value array
/// - Repetition levels (0 = new list, 1 = continuation)
/// - Definition levels (distinguish NULL/empty/present)
public final class Int32ListColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Int32] = []
    private var definitionLevelBuffer: [UInt16] = []
    private var repetitionLevelBuffer: [UInt16] = []
    private var totalValues: Int64 = 0  // Number of level entries (for column chunk metadata)
    private var rowCount: Int64 = 0      // Actual number of rows (top-level lists)
    private let columnStartOffset: Int64

    // Track metadata for column chunk
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []

    // Statistics tracking (for element values, not lists)
    private var statisticsAccumulator: Int32StatisticsAccumulator?

    // Level configuration
    private let maxDefinitionLevel: Int
    private let maxRepetitionLevel: Int
    private let repeatedAncestorDefLevel: Int
    private let nullListDefLevel: Int

    init(
        column: Column,
        properties: WriterProperties,
        pageWriter: PageWriter,
        startOffset: Int64,
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullListDefLevel: Int
    ) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.maxDefinitionLevel = maxDefinitionLevel
        self.maxRepetitionLevel = maxRepetitionLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
        self.nullListDefLevel = nullListDefLevel

        // Statistics for element values (if enabled)
        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = Int32StatisticsAccumulator()
        }
    }

    /// Write a batch of lists with required (non-null) elements
    /// - Parameter lists: Array of lists, where each list can be nil or empty
    /// - Throws: WriterError if write fails
    /// - Returns: Number of rows (top-level lists) written
    @discardableResult
    public func writeValues(_ lists: [[Int32]?]) throws -> Int {
        // Validate: required lists (nullListDefLevel < 0) cannot contain nil
        if nullListDefLevel < 0 {
            if lists.contains(where: { $0 == nil }) {
                throw WriterError.invalidState(
                    "Column \(column.name) is a required list and cannot contain nil"
                )
            }
        }

        // Use LevelComputer to flatten lists into values + levels
        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel
        )

        // Update statistics (element values only, not lists)
        statisticsAccumulator?.update(result.values)

        // Buffer values and levels
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)

        // totalValues = number of level entries (not value count)
        // This includes empty/NULL lists which emit levels but no values
        totalValues += Int64(result.repetitionLevels.count)

        // Track actual row count (number of top-level lists)
        rowCount += Int64(lists.count)

        if shouldFlush() {
            try flush()
        }

        // Return actual row count (number of top-level lists)
        return lists.count
    }

    /// Write a batch of lists with nullable elements
    /// - Parameter lists: Array of lists with optional elements
    /// - Throws: WriterError if write fails
    /// - Returns: Number of rows (top-level lists) written
    @discardableResult
    public func writeValuesWithNullableElements(
        _ lists: [[Int32?]?],
        nullElementDefLevel: Int
    ) throws -> Int {
        // Validate: required lists (nullListDefLevel < 0) cannot contain nil
        if nullListDefLevel < 0 {
            if lists.contains(where: { $0 == nil }) {
                throw WriterError.invalidState(
                    "Column \(column.name) is a required list and cannot contain nil"
                )
            }
        }

        // Use LevelComputer for nullable elements
        let result = LevelComputer.computeLevelsForListWithNullableElements(
            lists: lists,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel,
            nullElementDefLevel: nullElementDefLevel
        )

        // Update statistics (non-null element values only)
        statisticsAccumulator?.update(result.values)

        // Buffer values and levels
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)

        totalValues += Int64(result.repetitionLevels.count)
        rowCount += Int64(lists.count)

        if shouldFlush() {
            try flush()
        }

        return lists.count
    }

    /// Write multi-level nested lists (maxRepetitionLevel > 1)
    ///
    /// Handles nested list structures like:
    /// - 2-level: `[[[Int32]?]?]` (list of optional lists)
    /// - 3-level: `[[[[Int32]?]?]?]` (and so on)
    ///
    /// **Requirements**:
    /// - `column.maxRepetitionLevel >= 2`
    /// - `repeatedAncestorDefLevels.count == maxRepetitionLevel`
    /// - `nullListDefLevels.count == maxRepetitionLevel`
    ///
    /// **Usage**:
    /// ```swift
    /// let lists: [[[Int32]?]?] = [[[1, 2], [3]], [[4]]]
    /// try writer.writeNestedValues(
    ///     lists,
    ///     repeatedAncestorDefLevels: [1, 3],
    ///     nullListDefLevels: [0, 2]
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - lists: Type-erased nested array (will be cast to appropriate depth)
    ///   - repeatedAncestorDefLevels: Def levels for empty lists at each nesting level
    ///   - nullListDefLevels: Def levels for NULL lists at each nesting level
    ///
    /// - Returns: Number of rows (top-level lists) written
    /// - Throws: WriterError if write fails or types don't match
    @discardableResult
    public func writeNestedValues(
        _ lists: Any,
        repeatedAncestorDefLevels: [Int],
        nullListDefLevels: [Int]
    ) throws -> Int {
        guard maxRepetitionLevel >= 2 else {
            throw WriterError.invalidState(
                "writeNestedValues requires maxRepetitionLevel >= 2, got \(maxRepetitionLevel). " +
                "Use writeValues() for single-level lists."
            )
        }

        // Use LevelComputer to flatten multi-level nested lists
        let result: (values: [Int32], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: maxDefinitionLevel,
                maxRepetitionLevel: maxRepetitionLevel,
                repeatedAncestorDefLevels: repeatedAncestorDefLevels,
                nullListDefLevels: nullListDefLevels
            )

        // Update statistics (element values only)
        statisticsAccumulator?.update(result.values)

        // Buffer values and levels
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)

        totalValues += Int64(result.repetitionLevels.count)

        // Count top-level lists
        // For nested lists, we count entries with rep=0 (new top-level list)
        let topLevelListCount = result.repetitionLevels.filter { $0 == 0 }.count
        rowCount += Int64(topLevelListCount)

        if shouldFlush() {
            try flush()
        }

        return topLevelListCount
    }

    /// Get the number of rows written (for row group metadata)
    var numRows: Int64 {
        return rowCount
    }

    /// Flush buffered values to disk as a data page
    /// - Throws: WriterError if flush fails
    func flush() throws {
        // Must have at least one level entry (even if no values)
        guard !repetitionLevelBuffer.isEmpty else {
            return
        }

        // Encode repetition levels (always present for lists)
        let repEncoder = LevelEncoder(maxLevel: maxRepetitionLevel)
        repEncoder.encode(repetitionLevelBuffer)
        let repetitionLevelsData = repEncoder.flush()

        // Encode definition levels (always present for lists)
        let defEncoder = LevelEncoder(maxLevel: maxDefinitionLevel)
        defEncoder.encode(definitionLevelBuffer)
        let definitionLevelsData = defEncoder.flush()

        // Encode values using PLAIN encoding
        let valueEncoder = PlainEncoder<Int32>()
        valueEncoder.encode(valueBuffer)

        // Number of values in page = number of level entries (not value count)
        let numValues = Int32(repetitionLevelBuffer.count)

        // Write data page with both repetition and definition levels
        let result = try pageWriter.writeDataPage(
            values: valueEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData,
            repetitionLevels: repetitionLevelsData
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
        usedEncodings.insert(.rle)  // For levels

        // Clear buffers
        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
        repetitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    /// Close the column writer (flush any remaining data)
    /// - Returns: Metadata for this column chunk
    /// - Throws: WriterError if close fails
    func close() throws -> WriterColumnChunkMetadata {
        // Flush any remaining values
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        // Build statistics if enabled
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
        // Estimate page size: values + definition levels + repetition levels
        let valueSize = valueBuffer.count * 4  // 4 bytes per Int32

        // Level sizes: conservative estimate (4-byte length + ~1 bit per level)
        let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
        let repLevelSize = 4 + (repetitionLevelBuffer.count + 7) / 8

        let estimatedSize = valueSize + defLevelSize + repLevelSize
        return estimatedSize >= properties.dataPageSize
    }
}

// MARK: - Int64 List Column Writer

/// Writer for list<int64> columns (repeated int64)
public final class Int64ListColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [Int64] = []
    private var definitionLevelBuffer: [UInt16] = []
    private var repetitionLevelBuffer: [UInt16] = []
    private var totalValues: Int64 = 0  // Number of level entries (for column chunk metadata)
    private var rowCount: Int64 = 0      // Actual number of rows (top-level lists)
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []
    private var statisticsAccumulator: Int64StatisticsAccumulator?

    private let maxDefinitionLevel: Int
    private let maxRepetitionLevel: Int
    private let repeatedAncestorDefLevel: Int
    private let nullListDefLevel: Int

    init(
        column: Column,
        properties: WriterProperties,
        pageWriter: PageWriter,
        startOffset: Int64,
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullListDefLevel: Int
    ) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.maxDefinitionLevel = maxDefinitionLevel
        self.maxRepetitionLevel = maxRepetitionLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
        self.nullListDefLevel = nullListDefLevel

        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = Int64StatisticsAccumulator()
        }
    }

    @discardableResult
    public func writeValues(_ lists: [[Int64]?]) throws -> Int {
        // Validate: required lists cannot contain nil
        if nullListDefLevel < 0 {
            if lists.contains(where: { $0 == nil }) {
                throw WriterError.invalidState(
                    "Column \(column.name) is a required list and cannot contain nil"
                )
            }
        }

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel
        )

        statisticsAccumulator?.update(result.values)
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)
        totalValues += Int64(result.repetitionLevels.count)
        rowCount += Int64(lists.count)

        if shouldFlush() {
            try flush()
        }

        return lists.count
    }

    @discardableResult
    public func writeValuesWithNullableElements(
        _ lists: [[Int64?]?],
        nullElementDefLevel: Int
    ) throws -> Int {
        // Validate: required lists cannot contain nil
        if nullListDefLevel < 0 {
            if lists.contains(where: { $0 == nil }) {
                throw WriterError.invalidState(
                    "Column \(column.name) is a required list and cannot contain nil"
                )
            }
        }

        let result = LevelComputer.computeLevelsForListWithNullableElements(
            lists: lists,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel,
            nullElementDefLevel: nullElementDefLevel
        )

        statisticsAccumulator?.update(result.values)
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)
        totalValues += Int64(result.repetitionLevels.count)
        rowCount += Int64(lists.count)

        if shouldFlush() {
            try flush()
        }

        return lists.count
    }

    /// Write multi-level nested lists (maxRepetitionLevel >= 2)
    ///
    /// For 2-level nested lists: `[[[Int64]?]?]`
    /// For 3-level nested lists: `[[[[Int64]?]?]?]`
    ///
    /// - Parameters:
    ///   - lists: Type-erased nested array structure (use `Any` for flexibility)
    ///   - repeatedAncestorDefLevels: Array of def levels for empty lists at each nesting level.
    ///                                 Index 0 = outermost, index maxRep-1 = innermost.
    ///                                 Length must equal maxRepetitionLevel.
    ///   - nullListDefLevels: Array of def levels for NULL lists at each nesting level.
    ///                        Index 0 = outermost, index maxRep-1 = innermost.
    ///                        Length must equal maxRepetitionLevel.
    ///
    /// - Returns: Number of top-level lists written
    /// - Throws: WriterError if validation fails or nesting structure doesn't match maxRepetitionLevel
    @discardableResult
    public func writeNestedValues(
        _ lists: Any,
        repeatedAncestorDefLevels: [Int],
        nullListDefLevels: [Int]
    ) throws -> Int {
        guard maxRepetitionLevel >= 2 else {
            throw WriterError.invalidState(
                "writeNestedValues requires maxRepetitionLevel >= 2, got \(maxRepetitionLevel). " +
                "Use writeValues() for single-level lists."
            )
        }

        // Use LevelComputer to flatten multi-level nested lists
        let result: (values: [Int64], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: maxDefinitionLevel,
                maxRepetitionLevel: maxRepetitionLevel,
                repeatedAncestorDefLevels: repeatedAncestorDefLevels,
                nullListDefLevels: nullListDefLevels
            )

        // Update statistics and buffers
        statisticsAccumulator?.update(result.values)
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)
        totalValues += Int64(result.repetitionLevels.count)

        // Count top-level lists (rep=0 entries)
        let topLevelListCount = result.repetitionLevels.filter { $0 == 0 }.count
        rowCount += Int64(topLevelListCount)

        if shouldFlush() {
            try flush()
        }

        return topLevelListCount
    }

    /// Get the number of rows written (for row group metadata)
    var numRows: Int64 {
        return rowCount
    }

    func flush() throws {
        guard !repetitionLevelBuffer.isEmpty else {
            return
        }

        let repEncoder = LevelEncoder(maxLevel: maxRepetitionLevel)
        repEncoder.encode(repetitionLevelBuffer)
        let repetitionLevelsData = repEncoder.flush()

        let defEncoder = LevelEncoder(maxLevel: maxDefinitionLevel)
        defEncoder.encode(definitionLevelBuffer)
        let definitionLevelsData = defEncoder.flush()

        let valueEncoder = PlainEncoder<Int64>()
        valueEncoder.encode(valueBuffer)

        let numValues = Int32(repetitionLevelBuffer.count)

        let result = try pageWriter.writeDataPage(
            values: valueEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)
        usedEncodings.insert(.rle)

        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
        repetitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        let statistics = statisticsAccumulator?.build()

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
        let valueSize = valueBuffer.count * 8  // 8 bytes per Int64
        let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
        let repLevelSize = 4 + (repetitionLevelBuffer.count + 7) / 8
        let estimatedSize = valueSize + defLevelSize + repLevelSize
        return estimatedSize >= properties.dataPageSize
    }
}

// MARK: - String List Column Writer

/// Writer for list<string> columns (repeated byte arrays)
public final class StringListColumnWriter {
    private let column: Column
    private let properties: WriterProperties
    private let pageWriter: PageWriter
    private var valueBuffer: [String] = []
    private var definitionLevelBuffer: [UInt16] = []
    private var repetitionLevelBuffer: [UInt16] = []
    private var totalValues: Int64 = 0  // Number of level entries (for column chunk metadata)
    private var rowCount: Int64 = 0      // Actual number of rows (top-level lists)
    private let columnStartOffset: Int64
    private var dataPageOffset: Int64?
    private var totalCompressedSize: Int64 = 0
    private var totalUncompressedSize: Int64 = 0
    private var usedEncodings: Set<Encoding> = []
    private var statisticsAccumulator: StringStatisticsAccumulator?

    private let maxDefinitionLevel: Int
    private let maxRepetitionLevel: Int
    private let repeatedAncestorDefLevel: Int
    private let nullListDefLevel: Int

    init(
        column: Column,
        properties: WriterProperties,
        pageWriter: PageWriter,
        startOffset: Int64,
        maxDefinitionLevel: Int,
        maxRepetitionLevel: Int,
        repeatedAncestorDefLevel: Int,
        nullListDefLevel: Int
    ) {
        self.column = column
        self.properties = properties
        self.pageWriter = pageWriter
        self.columnStartOffset = startOffset
        self.maxDefinitionLevel = maxDefinitionLevel
        self.maxRepetitionLevel = maxRepetitionLevel
        self.repeatedAncestorDefLevel = repeatedAncestorDefLevel
        self.nullListDefLevel = nullListDefLevel

        if properties.statisticsEnabled(for: column.name) {
            self.statisticsAccumulator = StringStatisticsAccumulator()
        }
    }

    @discardableResult
    public func writeValues(_ lists: [[String]?]) throws -> Int {
        // Validate: required lists cannot contain nil
        if nullListDefLevel < 0 {
            if lists.contains(where: { $0 == nil }) {
                throw WriterError.invalidState(
                    "Column \(column.name) is a required list and cannot contain nil"
                )
            }
        }

        let result = LevelComputer.computeLevelsForList(
            lists: lists,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel
        )

        statisticsAccumulator?.update(result.values)
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)
        totalValues += Int64(result.repetitionLevels.count)
        rowCount += Int64(lists.count)

        if shouldFlush() {
            try flush()
        }

        return lists.count
    }

    @discardableResult
    public func writeValuesWithNullableElements(
        _ lists: [[String?]?],
        nullElementDefLevel: Int
    ) throws -> Int {
        // Validate: required lists cannot contain nil
        if nullListDefLevel < 0 {
            if lists.contains(where: { $0 == nil }) {
                throw WriterError.invalidState(
                    "Column \(column.name) is a required list and cannot contain nil"
                )
            }
        }

        let result = LevelComputer.computeLevelsForListWithNullableElements(
            lists: lists,
            maxDefinitionLevel: maxDefinitionLevel,
            maxRepetitionLevel: maxRepetitionLevel,
            repeatedAncestorDefLevel: repeatedAncestorDefLevel,
            nullListDefLevel: nullListDefLevel,
            nullElementDefLevel: nullElementDefLevel
        )

        statisticsAccumulator?.update(result.values)
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)
        totalValues += Int64(result.repetitionLevels.count)
        rowCount += Int64(lists.count)

        if shouldFlush() {
            try flush()
        }

        return lists.count
    }

    /// Write multi-level nested lists (maxRepetitionLevel >= 2)
    ///
    /// For 2-level nested lists: `[[[String]?]?]`
    /// For 3-level nested lists: `[[[[String]?]?]?]`
    ///
    /// - Parameters:
    ///   - lists: Type-erased nested array structure (use `Any` for flexibility)
    ///   - repeatedAncestorDefLevels: Array of def levels for empty lists at each nesting level.
    ///                                 Index 0 = outermost, index maxRep-1 = innermost.
    ///                                 Length must equal maxRepetitionLevel.
    ///   - nullListDefLevels: Array of def levels for NULL lists at each nesting level.
    ///                        Index 0 = outermost, index maxRep-1 = innermost.
    ///                        Length must equal maxRepetitionLevel.
    ///
    /// - Returns: Number of top-level lists written
    /// - Throws: WriterError if validation fails or nesting structure doesn't match maxRepetitionLevel
    @discardableResult
    public func writeNestedValues(
        _ lists: Any,
        repeatedAncestorDefLevels: [Int],
        nullListDefLevels: [Int]
    ) throws -> Int {
        guard maxRepetitionLevel >= 2 else {
            throw WriterError.invalidState(
                "writeNestedValues requires maxRepetitionLevel >= 2, got \(maxRepetitionLevel). " +
                "Use writeValues() for single-level lists."
            )
        }

        // Use LevelComputer to flatten multi-level nested lists
        let result: (values: [String], repetitionLevels: [UInt16], definitionLevels: [UInt16]) =
            try LevelComputer.computeLevelsForNestedList(
                lists: lists,
                maxDefinitionLevel: maxDefinitionLevel,
                maxRepetitionLevel: maxRepetitionLevel,
                repeatedAncestorDefLevels: repeatedAncestorDefLevels,
                nullListDefLevels: nullListDefLevels
            )

        // Update statistics and buffers
        statisticsAccumulator?.update(result.values)
        valueBuffer.append(contentsOf: result.values)
        definitionLevelBuffer.append(contentsOf: result.definitionLevels)
        repetitionLevelBuffer.append(contentsOf: result.repetitionLevels)
        totalValues += Int64(result.repetitionLevels.count)

        // Count top-level lists (rep=0 entries)
        let topLevelListCount = result.repetitionLevels.filter { $0 == 0 }.count
        rowCount += Int64(topLevelListCount)

        if shouldFlush() {
            try flush()
        }

        return topLevelListCount
    }

    /// Get the number of rows written (for row group metadata)
    var numRows: Int64 {
        return rowCount
    }

    func flush() throws {
        guard !repetitionLevelBuffer.isEmpty else {
            return
        }

        let repEncoder = LevelEncoder(maxLevel: maxRepetitionLevel)
        repEncoder.encode(repetitionLevelBuffer)
        let repetitionLevelsData = repEncoder.flush()

        let defEncoder = LevelEncoder(maxLevel: maxDefinitionLevel)
        defEncoder.encode(definitionLevelBuffer)
        let definitionLevelsData = defEncoder.flush()

        let valueEncoder = PlainEncoder<String>()
        try valueEncoder.encode(valueBuffer)

        let numValues = Int32(repetitionLevelBuffer.count)

        let result = try pageWriter.writeDataPage(
            values: valueEncoder.data,
            numValues: numValues,
            encoding: .plain,
            definitionLevels: definitionLevelsData,
            repetitionLevels: repetitionLevelsData
        )

        if dataPageOffset == nil {
            dataPageOffset = result.startOffset
        }

        let headerLength = result.bytesWritten - result.compressedSize
        totalCompressedSize += Int64(result.compressedSize + headerLength)
        totalUncompressedSize += Int64(result.uncompressedSize + headerLength)

        usedEncodings.insert(.plain)
        usedEncodings.insert(.rle)

        valueBuffer.removeAll(keepingCapacity: true)
        definitionLevelBuffer.removeAll(keepingCapacity: true)
        repetitionLevelBuffer.removeAll(keepingCapacity: true)
    }

    func close() throws -> WriterColumnChunkMetadata {
        try flush()

        guard let dataPageOffset = dataPageOffset else {
            throw WriterError.invalidState("No data pages written for column \(column.name)")
        }

        let statistics = statisticsAccumulator?.build()

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
        let valueSize = valueBuffer.reduce(0) { $0 + 4 + $1.utf8.count }
        let defLevelSize = 4 + (definitionLevelBuffer.count + 7) / 8
        let repLevelSize = 4 + (repetitionLevelBuffer.count + 7) / 8
        let estimatedSize = valueSize + defLevelSize + repLevelSize
        return estimatedSize >= properties.dataPageSize
    }
}
