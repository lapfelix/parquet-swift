// ThriftWriter.swift - Thrift Compact Binary Protocol serializer for writing metadata
//
// Licensed under the Apache License, Version 2.0

import Foundation

/// Serializer for Thrift Compact Binary Protocol
///
/// Used to write Parquet file metadata (FileMetaData, PageHeader, etc.)
/// Follows the Thrift Compact Protocol specification
final class ThriftWriter {
    private(set) var data: Data
    private var lastFieldIdStack: [Int16] = []

    init() {
        self.data = Data()
    }

    // MARK: - File Metadata Writing

    func writeFileMetaData(_ metadata: ThriftFileMetaData) throws {
        // FileMetaData is a Thrift struct
        try writeStructBegin()

        // Field 1: version (i32, required)
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(metadata.version)

        // Field 2: schema (list<SchemaElement>, required)
        try writeFieldBegin(type: .list, id: 2)
        try writeSchemaElementList(metadata.schema)

        // Field 3: num_rows (i64, required)
        try writeFieldBegin(type: .i64, id: 3)
        try writeI64(metadata.numRows)

        // Field 4: row_groups (list<RowGroup>, required)
        try writeFieldBegin(type: .list, id: 4)
        try writeRowGroupList(metadata.rowGroups)

        // Field 5: key_value_metadata (list<KeyValue>, optional)
        if let keyValueMetadata = metadata.keyValueMetadata {
            try writeFieldBegin(type: .list, id: 5)
            try writeKeyValueList(keyValueMetadata)
        }

        // Field 6: created_by (string, optional)
        if let createdBy = metadata.createdBy {
            try writeFieldBegin(type: .string, id: 6)
            try writeString(createdBy)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - Page Header Writing

    func writePageHeader(_ header: ThriftPageHeader) throws {
        try writeStructBegin()

        // Field 1: type (PageType, required)
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(Int32(header.type.rawValue))

        // Field 2: uncompressed_page_size (i32, required)
        try writeFieldBegin(type: .i32, id: 2)
        try writeI32(header.uncompressedPageSize)

        // Field 3: compressed_page_size (i32, required)
        try writeFieldBegin(type: .i32, id: 3)
        try writeI32(header.compressedPageSize)

        // Field 4: crc (i32, optional)
        if let crc = header.crc {
            try writeFieldBegin(type: .i32, id: 4)
            try writeI32(crc)
        }

        // Field 5: data_page_header (DataPageHeader, optional)
        if let dataPageHeader = header.dataPageHeader {
            try writeFieldBegin(type: .struct, id: 5)
            try writeDataPageHeader(dataPageHeader)
        }

        // Field 6: index_page_header (IndexPageHeader, optional) - not implemented

        // Field 7: dictionary_page_header (DictionaryPageHeader, optional)
        if let dictionaryPageHeader = header.dictionaryPageHeader {
            try writeFieldBegin(type: .struct, id: 7)
            try writeDictionaryPageHeader(dictionaryPageHeader)
        }

        // Field 8: data_page_header_v2 (DataPageHeaderV2, optional)
        if let dataPageHeaderV2 = header.dataPageHeaderV2 {
            try writeFieldBegin(type: .struct, id: 8)
            try writeDataPageHeaderV2(dataPageHeaderV2)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeDataPageHeader(_ header: ThriftDataPageHeader) throws {
        try writeStructBegin()

        // Field 1: num_values (i32, required)
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(header.numValues)

        // Field 2: encoding (Encoding, required)
        try writeFieldBegin(type: .i32, id: 2)
        try writeI32(Int32(header.encoding.rawValue))

        // Field 3: definition_level_encoding (Encoding, required)
        try writeFieldBegin(type: .i32, id: 3)
        try writeI32(Int32(header.definitionLevelEncoding.rawValue))

        // Field 4: repetition_level_encoding (Encoding, required)
        try writeFieldBegin(type: .i32, id: 4)
        try writeI32(Int32(header.repetitionLevelEncoding.rawValue))

        // Field 5: statistics (Statistics, optional)
        if let statistics = header.statistics {
            try writeFieldBegin(type: .struct, id: 5)
            try writeStatistics(statistics)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeDictionaryPageHeader(_ header: ThriftDictionaryPageHeader) throws {
        try writeStructBegin()

        // Field 1: num_values (i32, required)
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(header.numValues)

        // Field 2: encoding (Encoding, required)
        try writeFieldBegin(type: .i32, id: 2)
        try writeI32(Int32(header.encoding.rawValue))

        // Field 3: is_sorted (bool, optional)
        if let isSorted = header.isSorted {
            try writeFieldBegin(type: .bool, id: 3)
            try writeBool(isSorted)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeDataPageHeaderV2(_ header: ThriftDataPageHeaderV2) throws {
        try writeStructBegin()

        // Field 1: num_values (i32, required)
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(header.numValues)

        // Field 2: num_nulls (i32, required)
        try writeFieldBegin(type: .i32, id: 2)
        try writeI32(header.numNulls)

        // Field 3: num_rows (i32, required)
        try writeFieldBegin(type: .i32, id: 3)
        try writeI32(header.numRows)

        // Field 4: encoding (Encoding, required)
        try writeFieldBegin(type: .i32, id: 4)
        try writeI32(Int32(header.encoding.rawValue))

        // Field 5: definition_levels_byte_length (i32, required)
        try writeFieldBegin(type: .i32, id: 5)
        try writeI32(header.definitionLevelsByteLength)

        // Field 6: repetition_levels_byte_length (i32, required)
        try writeFieldBegin(type: .i32, id: 6)
        try writeI32(header.repetitionLevelsByteLength)

        // Field 7: is_compressed (bool, optional)
        try writeFieldBegin(type: .bool, id: 7)
        try writeBool(header.isCompressed)

        // Field 8: statistics (Statistics, optional)
        if let statistics = header.statistics {
            try writeFieldBegin(type: .struct, id: 8)
            try writeStatistics(statistics)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeStatistics(_ statistics: ThriftStatistics) throws {
        try writeStructBegin()

        // Field 1: max (binary, optional)
        if let max = statistics.max {
            try writeFieldBegin(type: .string, id: 1)
            try writeBinary(max)
        }

        // Field 2: min (binary, optional)
        if let min = statistics.min {
            try writeFieldBegin(type: .string, id: 2)
            try writeBinary(min)
        }

        // Field 3: null_count (i64, optional)
        if let nullCount = statistics.nullCount {
            try writeFieldBegin(type: .i64, id: 3)
            try writeI64(nullCount)
        }

        // Field 4: distinct_count (i64, optional)
        if let distinctCount = statistics.distinctCount {
            try writeFieldBegin(type: .i64, id: 4)
            try writeI64(distinctCount)
        }

        // Field 5: max_value (binary, optional)
        if let maxValue = statistics.maxValue {
            try writeFieldBegin(type: .string, id: 5)
            try writeBinary(maxValue)
        }

        // Field 6: min_value (binary, optional)
        if let minValue = statistics.minValue {
            try writeFieldBegin(type: .string, id: 6)
            try writeBinary(minValue)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - Schema Element Writing

    private func writeSchemaElementList(_ elements: [ThriftSchemaElement]) throws {
        try writeListBegin(elementType: .struct, size: elements.count)
        for element in elements {
            try writeSchemaElement(element)
        }
    }

    private func writeSchemaElement(_ element: ThriftSchemaElement) throws {
        try writeStructBegin()

        // Field 1: type (Type, optional)
        if let type = element.type {
            try writeFieldBegin(type: .i32, id: 1)
            try writeI32(Int32(type.rawValue))
        }

        // Field 2: type_length (i32, optional)
        if let typeLength = element.typeLength {
            try writeFieldBegin(type: .i32, id: 2)
            try writeI32(typeLength)
        }

        // Field 3: repetition_type (FieldRepetitionType, optional)
        if let repetitionType = element.repetitionType {
            try writeFieldBegin(type: .i32, id: 3)
            try writeI32(Int32(repetitionType.rawValue))
        }

        // Field 4: name (string, required)
        try writeFieldBegin(type: .string, id: 4)
        try writeString(element.name)

        // Field 5: num_children (i32, optional)
        if let numChildren = element.numChildren {
            try writeFieldBegin(type: .i32, id: 5)
            try writeI32(numChildren)
        }

        // Field 6: converted_type (ConvertedType, optional)
        if let convertedType = element.convertedType {
            try writeFieldBegin(type: .i32, id: 6)
            try writeI32(Int32(convertedType.rawValue))
        }

        // Field 7: scale (i32, optional)
        if let scale = element.scale {
            try writeFieldBegin(type: .i32, id: 7)
            try writeI32(scale)
        }

        // Field 8: precision (i32, optional)
        if let precision = element.precision {
            try writeFieldBegin(type: .i32, id: 8)
            try writeI32(precision)
        }

        // Field 10: logicalType (LogicalType, optional)
        if let logicalType = element.logicalType {
            try writeFieldBegin(type: .struct, id: 10)
            try writeLogicalType(logicalType)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - Row Group Writing

    private func writeRowGroupList(_ rowGroups: [ThriftRowGroup]) throws {
        try writeListBegin(elementType: .struct, size: rowGroups.count)
        for rowGroup in rowGroups {
            try writeRowGroup(rowGroup)
        }
    }

    private func writeRowGroup(_ rowGroup: ThriftRowGroup) throws {
        try writeStructBegin()

        // Field 1: columns (list<ColumnChunk>, required)
        try writeFieldBegin(type: .list, id: 1)
        try writeColumnChunkList(rowGroup.columns)

        // Field 2: total_byte_size (i64, required)
        try writeFieldBegin(type: .i64, id: 2)
        try writeI64(rowGroup.totalByteSize)

        // Field 3: num_rows (i64, required)
        try writeFieldBegin(type: .i64, id: 3)
        try writeI64(rowGroup.numRows)

        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - Column Chunk Writing

    private func writeColumnChunkList(_ columns: [ThriftColumnChunk]) throws {
        try writeListBegin(elementType: .struct, size: columns.count)
        for column in columns {
            try writeColumnChunk(column)
        }
    }

    private func writeColumnChunk(_ column: ThriftColumnChunk) throws {
        try writeStructBegin()

        // Field 1: file_path (string, optional)
        if let filePath = column.filePath {
            try writeFieldBegin(type: .string, id: 1)
            try writeString(filePath)
        }

        // Field 2: file_offset (i64, required)
        try writeFieldBegin(type: .i64, id: 2)
        try writeI64(column.fileOffset)

        // Field 3: meta_data (ColumnMetaData, optional)
        if let metaData = column.metaData {
            try writeFieldBegin(type: .struct, id: 3)
            try writeColumnMetaData(metaData)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeColumnMetaData(_ metaData: ThriftColumnMetaData) throws {
        try writeStructBegin()

        // Field 1: type (Type, required)
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(Int32(metaData.type.rawValue))

        // Field 2: encodings (list<Encoding>, required)
        try writeFieldBegin(type: .list, id: 2)
        try writeEncodingList(metaData.encodings)

        // Field 3: path_in_schema (list<string>, required)
        try writeFieldBegin(type: .list, id: 3)
        try writeStringList(metaData.pathInSchema)

        // Field 4: codec (CompressionCodec, required)
        try writeFieldBegin(type: .i32, id: 4)
        try writeI32(Int32(metaData.codec.rawValue))

        // Field 5: num_values (i64, required)
        try writeFieldBegin(type: .i64, id: 5)
        try writeI64(metaData.numValues)

        // Field 6: total_uncompressed_size (i64, required)
        try writeFieldBegin(type: .i64, id: 6)
        try writeI64(metaData.totalUncompressedSize)

        // Field 7: total_compressed_size (i64, required)
        try writeFieldBegin(type: .i64, id: 7)
        try writeI64(metaData.totalCompressedSize)

        // Field 9: data_page_offset (i64, required)
        try writeFieldBegin(type: .i64, id: 9)
        try writeI64(metaData.dataPageOffset)

        // Field 10: index_page_offset (i64, optional)
        if let indexPageOffset = metaData.indexPageOffset {
            try writeFieldBegin(type: .i64, id: 10)
            try writeI64(indexPageOffset)
        }

        // Field 11: dictionary_page_offset (i64, optional)
        if let dictionaryPageOffset = metaData.dictionaryPageOffset {
            try writeFieldBegin(type: .i64, id: 11)
            try writeI64(dictionaryPageOffset)
        }

        // Field 12: statistics (Statistics, optional)
        if let statistics = metaData.statistics {
            try writeFieldBegin(type: .struct, id: 12)
            try writeStatistics(statistics)
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - Logical Type Writing

    private func writeLogicalType(_ logicalType: ThriftLogicalType) throws {
        try writeStructBegin()

        switch logicalType {
        case .string:
            try writeFieldBegin(type: .struct, id: 1)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .map:
            try writeFieldBegin(type: .struct, id: 2)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .list:
            try writeFieldBegin(type: .struct, id: 3)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .enum:
            try writeFieldBegin(type: .struct, id: 4)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .decimal(let decimalType):
            try writeFieldBegin(type: .struct, id: 5)
            try writeDecimalType(decimalType)
        case .date:
            try writeFieldBegin(type: .struct, id: 6)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .time(let timeType):
            try writeFieldBegin(type: .struct, id: 7)
            try writeTimeType(timeType)
        case .timestamp(let timestampType):
            try writeFieldBegin(type: .struct, id: 8)
            try writeTimestampType(timestampType)
        case .integer(let intType):
            try writeFieldBegin(type: .struct, id: 10)
            try writeIntType(intType)
        case .unknown:
            try writeFieldBegin(type: .struct, id: 11)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .json:
            try writeFieldBegin(type: .struct, id: 12)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .bson:
            try writeFieldBegin(type: .struct, id: 13)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .uuid:
            try writeFieldBegin(type: .struct, id: 14)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        case .float16:
            try writeFieldBegin(type: .struct, id: 15)
            try writeStructBegin()
            try writeFieldStop()
            try writeStructEnd()
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeDecimalType(_ decimalType: ThriftDecimalType) throws {
        try writeStructBegin()
        try writeFieldBegin(type: .i32, id: 1)
        try writeI32(decimalType.scale)
        try writeFieldBegin(type: .i32, id: 2)
        try writeI32(decimalType.precision)
        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeTimeType(_ timeType: ThriftTimeType) throws {
        try writeStructBegin()
        try writeFieldBegin(type: .bool, id: 1)
        try writeBool(timeType.isAdjustedToUTC)
        try writeFieldBegin(type: .struct, id: 2)
        try writeTimeUnit(timeType.unit)
        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeTimestampType(_ timestampType: ThriftTimestampType) throws {
        try writeStructBegin()
        try writeFieldBegin(type: .bool, id: 1)
        try writeBool(timestampType.isAdjustedToUTC)
        try writeFieldBegin(type: .struct, id: 2)
        try writeTimeUnit(timestampType.unit)
        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeIntType(_ intType: ThriftIntType) throws {
        try writeStructBegin()
        try writeFieldBegin(type: .byte, id: 1)
        try writeByte(intType.bitWidth)
        try writeFieldBegin(type: .bool, id: 2)
        try writeBool(intType.isSigned)
        try writeFieldStop()
        try writeStructEnd()
    }

    private func writeTimeUnit(_ timeUnit: ThriftTimeUnit) throws {
        try writeStructBegin()

        switch timeUnit {
        case .millis:
            try writeFieldBegin(type: .struct, id: 1)
            try writeStructBegin()  // Empty struct
            try writeFieldStop()
            try writeStructEnd()
        case .micros:
            try writeFieldBegin(type: .struct, id: 2)
            try writeStructBegin()  // Empty struct
            try writeFieldStop()
            try writeStructEnd()
        case .nanos:
            try writeFieldBegin(type: .struct, id: 3)
            try writeStructBegin()  // Empty struct
            try writeFieldStop()
            try writeStructEnd()
        }

        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - List Writing

    private func writeEncodingList(_ encodings: [ThriftEncoding]) throws {
        try writeListBegin(elementType: .i32, size: encodings.count)
        for encoding in encodings {
            try writeI32(Int32(encoding.rawValue))
        }
    }

    private func writeStringList(_ strings: [String]) throws {
        try writeListBegin(elementType: .string, size: strings.count)
        for string in strings {
            try writeString(string)
        }
    }

    private func writeKeyValueList(_ keyValues: [ThriftKeyValue]) throws {
        try writeListBegin(elementType: .struct, size: keyValues.count)
        for keyValue in keyValues {
            try writeKeyValue(keyValue)
        }
    }

    private func writeKeyValue(_ keyValue: ThriftKeyValue) throws {
        try writeStructBegin()
        try writeFieldBegin(type: .string, id: 1)
        try writeString(keyValue.key)
        if let value = keyValue.value {
            try writeFieldBegin(type: .string, id: 2)
            try writeString(value)
        }
        try writeFieldStop()
        try writeStructEnd()
    }

    // MARK: - Compact Protocol Primitives

    private enum ThriftType: UInt8 {
        case stop = 0
        case bool = 1
        case byte = 3
        case i16 = 4
        case i32 = 5
        case i64 = 6
        case double = 7
        case string = 8
        case list = 9
        case set = 10
        case map = 11
        case `struct` = 12
    }

    private func writeStructBegin() throws {
        // Push 0 to track last field ID for this struct level
        lastFieldIdStack.append(0)
    }

    private func writeStructEnd() throws {
        // Pop last field ID when exiting struct
        lastFieldIdStack.removeLast()
    }

    private func writeFieldBegin(type: ThriftType, id: Int16) throws {
        // Get last field ID from current struct level
        let lastFieldId = lastFieldIdStack.last ?? 0
        let delta = id - lastFieldId

        if delta > 0 && delta <= 15 && type != .bool {
            // Short form: delta in upper 4 bits, type in lower 4 bits
            let header = UInt8((delta << 4) | Int16(type.rawValue))
            data.append(header)
        } else {
            // Long form: zero delta, then zigzag varint id
            let header = type.rawValue
            data.append(header)
            try writeZigZagVarint(Int64(id))
        }

        // Update last field ID for this struct level
        if !lastFieldIdStack.isEmpty {
            lastFieldIdStack[lastFieldIdStack.count - 1] = id
        }
    }

    private func writeFieldStop() throws {
        data.append(0)  // STOP byte
    }

    private func writeListBegin(elementType: ThriftType, size: Int) throws {
        if size < 15 {
            // Short form: size in upper 4 bits, type in lower 4 bits
            let header = UInt8((size << 4) | Int(elementType.rawValue))
            data.append(header)
        } else {
            // Long form: 0xF0 | type, then varint size
            let header = UInt8(0xF0 | elementType.rawValue)
            data.append(header)
            try writeVarint(UInt64(size))
        }
    }

    private func writeBool(_ value: Bool) throws {
        data.append(value ? 1 : 0)
    }

    private func writeByte(_ value: Int8) throws {
        data.append(UInt8(bitPattern: value))
    }

    private func writeI32(_ value: Int32) throws {
        try writeZigZagVarint(Int64(value))
    }

    private func writeI64(_ value: Int64) throws {
        try writeZigZagVarint(value)
    }

    private func writeString(_ value: String) throws {
        let utf8 = Data(value.utf8)
        try writeVarint(UInt64(utf8.count))
        data.append(utf8)
    }

    private func writeBinary(_ value: Data) throws {
        try writeVarint(UInt64(value.count))
        data.append(value)
    }

    private func writeVarint(_ value: UInt64) throws {
        var v = value
        while v >= 0x80 {
            data.append(UInt8((v & 0x7F) | 0x80))
            v >>= 7
        }
        data.append(UInt8(v & 0x7F))
    }

    private func writeZigZagVarint(_ value: Int64) throws {
        let zigzag = UInt64(bitPattern: (value << 1) ^ (value >> 63))
        try writeVarint(zigzag)
    }
}
