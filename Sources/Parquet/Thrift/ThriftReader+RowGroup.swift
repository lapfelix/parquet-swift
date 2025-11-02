// ThriftReader extensions for RowGroup deserialization
//
// Licensed under the Apache License, Version 2.0

import Foundation

extension ThriftReader {
    /// Reads a RowGroup from the current position.
    func readRowGroup() throws -> ThriftRowGroup {
        var columns: [ThriftColumnChunk]?
        var totalByteSize: Int64?
        var numRows: Int64?
        var sortingColumns: [ThriftSortingColumn]?
        var fileOffset: Int64?
        var totalCompressedSize: Int64?
        var ordinal: Int16?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // columns
                let (elementType, count) = try readListHeader()
                guard elementType == .struct else {
                    throw ThriftError.protocolError("Expected struct list for columns")
                }
                var chunks: [ThriftColumnChunk] = []
                for _ in 0..<count {
                    chunks.append(try readColumnChunk())
                }
                columns = chunks
            case 2: // total_byte_size
                totalByteSize = try readVarint()
            case 3: // num_rows
                numRows = try readVarint()
            case 4: // sorting_columns
                let (elementType, count) = try readListHeader()
                guard elementType == .struct else {
                    throw ThriftError.protocolError("Expected struct list for sorting_columns")
                }
                var sorting: [ThriftSortingColumn] = []
                for _ in 0..<count {
                    sorting.append(try readSortingColumn())
                }
                sortingColumns = sorting
            case 5: // file_offset
                fileOffset = try readVarint()
            case 6: // total_compressed_size
                totalCompressedSize = try readVarint()
            case 7: // ordinal
                ordinal = try readVarint16()
            default:
                try skipField(type: field.type)
            }
        }

        guard let columns = columns,
              let totalByteSize = totalByteSize,
              let numRows = numRows else {
            throw ThriftError.invalidData("Missing required fields in RowGroup")
        }

        return ThriftRowGroup(
            columns: columns,
            totalByteSize: totalByteSize,
            numRows: numRows,
            sortingColumns: sortingColumns,
            fileOffset: fileOffset,
            totalCompressedSize: totalCompressedSize,
            ordinal: ordinal
        )
    }

    func readSortingColumn() throws -> ThriftSortingColumn {
        var columnIdx: Int32?
        var descending: Bool?
        var nullsFirst: Bool?
        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // column_idx
                columnIdx = try readVarint32()
            case 2: // descending
                descending = field.type == .boolTrue
            case 3: // nulls_first
                nullsFirst = field.type == .boolTrue
            default:
                try skipField(type: field.type)
            }
        }

        guard let columnIdx = columnIdx,
              let descending = descending,
              let nullsFirst = nullsFirst else {
            throw ThriftError.invalidData("Missing required fields in SortingColumn")
        }

        return ThriftSortingColumn(
            columnIdx: columnIdx,
            descending: descending,
            nullsFirst: nullsFirst
        )
    }

    func readColumnChunk() throws -> ThriftColumnChunk {
        var filePath: String?
        var fileOffset: Int64 = 0
        var metaData: ThriftColumnMetaData?
        var offsetIndexOffset: Int64?
        var offsetIndexLength: Int32?
        var columnIndexOffset: Int64?
        var columnIndexLength: Int32?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // file_path
                filePath = try readString()
            case 2: // file_offset
                fileOffset = try readVarint()
            case 3: // meta_data
                metaData = try readColumnMetaData()
            case 4: // offset_index_offset
                offsetIndexOffset = try readVarint()
            case 5: // offset_index_length
                offsetIndexLength = try readVarint32()
            case 6: // column_index_offset
                columnIndexOffset = try readVarint()
            case 7: // column_index_length
                columnIndexLength = try readVarint32()
            default:
                try skipField(type: field.type)
            }
        }

        return ThriftColumnChunk(
            filePath: filePath,
            fileOffset: fileOffset,
            metaData: metaData,
            offsetIndexOffset: offsetIndexOffset,
            offsetIndexLength: offsetIndexLength,
            columnIndexOffset: columnIndexOffset,
            columnIndexLength: columnIndexLength
        )
    }

    func readColumnMetaData() throws -> ThriftColumnMetaData {
        var type: ThriftType?
        var encodings: [ThriftEncoding]?
        var pathInSchema: [String]?
        var codec: ThriftCompressionCodec?
        var numValues: Int64?
        var totalUncompressedSize: Int64?
        var totalCompressedSize: Int64?
        var keyValueMetadata: [ThriftKeyValue]?
        var dataPageOffset: Int64?
        var indexPageOffset: Int64?
        var dictionaryPageOffset: Int64?
        var statistics: ThriftStatistics?
        var encodingStats: [ThriftPageEncodingStats]?
        var bloomFilterOffset: Int64?
        var bloomFilterLength: Int32?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // type
                let value = try readVarint32()
                type = ThriftType(rawValue: value)
            case 2: // encodings
                let (elementType, count) = try readListHeader()
                guard elementType == .i32 else {
                    throw ThriftError.protocolError("Expected i32 list for encodings")
                }
                var encs: [ThriftEncoding] = []
                for _ in 0..<count {
                    let value = try readVarint32()
                    if let encoding = ThriftEncoding(rawValue: value) {
                        encs.append(encoding)
                    }
                }
                encodings = encs
            case 3: // path_in_schema
                let (elementType, count) = try readListHeader()
                guard elementType == .binary else {
                    throw ThriftError.protocolError("Expected binary list for path_in_schema")
                }
                var paths: [String] = []
                for _ in 0..<count {
                    paths.append(try readString())
                }
                pathInSchema = paths
            case 4: // codec
                let value = try readVarint32()
                codec = ThriftCompressionCodec(rawValue: value)
            case 5: // num_values
                numValues = try readVarint()
            case 6: // total_uncompressed_size
                totalUncompressedSize = try readVarint()
            case 7: // total_compressed_size
                totalCompressedSize = try readVarint()
            case 8: // key_value_metadata
                let (elementType, count) = try readListHeader()
                guard elementType == .struct else {
                    throw ThriftError.protocolError("Expected struct list for key_value_metadata")
                }
                var kvs: [ThriftKeyValue] = []
                for _ in 0..<count {
                    kvs.append(try readKeyValue())
                }
                keyValueMetadata = kvs
            case 9: // data_page_offset
                dataPageOffset = try readVarint()
            case 10: // index_page_offset
                indexPageOffset = try readVarint()
            case 11: // dictionary_page_offset
                dictionaryPageOffset = try readVarint()
            case 12: // statistics
                statistics = try readStatistics()
            case 13: // encoding_stats
                let (elementType, count) = try readListHeader()
                guard elementType == .struct else {
                    throw ThriftError.protocolError("Expected struct list for encoding_stats")
                }
                var stats: [ThriftPageEncodingStats] = []
                for _ in 0..<count {
                    stats.append(try readPageEncodingStats())
                }
                encodingStats = stats
            case 14: // bloom_filter_offset
                bloomFilterOffset = try readVarint()
            case 15: // bloom_filter_length
                bloomFilterLength = try readVarint32()
            default:
                try skipField(type: field.type)
            }
        }

        guard let type = type,
              let encodings = encodings,
              let pathInSchema = pathInSchema,
              let codec = codec,
              let numValues = numValues,
              let totalUncompressedSize = totalUncompressedSize,
              let totalCompressedSize = totalCompressedSize,
              let dataPageOffset = dataPageOffset else {
            throw ThriftError.invalidData("Missing required fields in ColumnMetaData")
        }

        return ThriftColumnMetaData(
            type: type,
            encodings: encodings,
            pathInSchema: pathInSchema,
            codec: codec,
            numValues: numValues,
            totalUncompressedSize: totalUncompressedSize,
            totalCompressedSize: totalCompressedSize,
            keyValueMetadata: keyValueMetadata,
            dataPageOffset: dataPageOffset,
            indexPageOffset: indexPageOffset,
            dictionaryPageOffset: dictionaryPageOffset,
            statistics: statistics,
            encodingStats: encodingStats,
            bloomFilterOffset: bloomFilterOffset,
            bloomFilterLength: bloomFilterLength
        )
    }

    func readStatistics() throws -> ThriftStatistics {
        var max: Data?
        var min: Data?
        var nullCount: Int64?
        var distinctCount: Int64?
        var maxValue: Data?
        var minValue: Data?
        var isMaxValueExact: Bool?
        var isMinValueExact: Bool?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // max
                max = try readBinary()
            case 2: // min
                min = try readBinary()
            case 3: // null_count
                nullCount = try readVarint()
            case 4: // distinct_count
                distinctCount = try readVarint()
            case 5: // max_value
                maxValue = try readBinary()
            case 6: // min_value
                minValue = try readBinary()
            case 7: // is_max_value_exact
                isMaxValueExact = field.type == .boolTrue
            case 8: // is_min_value_exact
                isMinValueExact = field.type == .boolTrue
            default:
                try skipField(type: field.type)
            }
        }

        return ThriftStatistics(
            max: max,
            min: min,
            nullCount: nullCount,
            distinctCount: distinctCount,
            maxValue: maxValue,
            minValue: minValue,
            isMaxValueExact: isMaxValueExact,
            isMinValueExact: isMinValueExact
        )
    }

    func readPageEncodingStats() throws -> ThriftPageEncodingStats {
        var pageType: ThriftPageType?
        var encoding: ThriftEncoding?
        var count: Int32?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // page_type
                let value = try readVarint32()
                pageType = ThriftPageType(rawValue: value)
            case 2: // encoding
                let value = try readVarint32()
                encoding = ThriftEncoding(rawValue: value)
            case 3: // count
                count = try readVarint32()
            default:
                try skipField(type: field.type)
            }
        }

        guard let pageType = pageType,
              let encoding = encoding,
              let count = count else {
            throw ThriftError.invalidData("Missing required fields in PageEncodingStats")
        }

        return ThriftPageEncodingStats(
            pageType: pageType,
            encoding: encoding,
            count: count
        )
    }
}
