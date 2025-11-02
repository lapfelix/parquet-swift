// ThriftReader extensions for PageHeader deserialization
//
// Licensed under the Apache License, Version 2.0

import Foundation

extension ThriftReader {
    /// Reads a PageHeader from the current position.
    public func readPageHeader() throws -> ThriftPageHeader {
        var type: ThriftPageType?
        var uncompressedPageSize: Int32?
        var compressedPageSize: Int32?
        var crc: Int32?
        var dataPageHeader: ThriftDataPageHeader?
        var dictionaryPageHeader: ThriftDictionaryPageHeader?
        var dataPageHeaderV2: ThriftDataPageHeaderV2?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // type
                let value = try readVarint32()
                type = ThriftPageType(rawValue: value)
            case 2: // uncompressed_page_size
                uncompressedPageSize = try readVarint32()
            case 3: // compressed_page_size
                compressedPageSize = try readVarint32()
            case 4: // crc
                crc = try readVarint32()
            case 5: // data_page_header
                dataPageHeader = try readDataPageHeader()
            case 6: // index_page_header
                // Skip index page header (not needed for Phase 1)
                try skipField(type: field.type)
            case 7: // dictionary_page_header
                dictionaryPageHeader = try readDictionaryPageHeader()
            case 8: // data_page_header_v2
                dataPageHeaderV2 = try readDataPageHeaderV2()
            default:
                try skipField(type: field.type)
            }
        }

        guard let type = type,
              let uncompressedPageSize = uncompressedPageSize,
              let compressedPageSize = compressedPageSize else {
            throw ThriftError.invalidData("Missing required fields in PageHeader")
        }

        return ThriftPageHeader(
            type: type,
            uncompressedPageSize: uncompressedPageSize,
            compressedPageSize: compressedPageSize,
            crc: crc,
            dataPageHeader: dataPageHeader,
            dictionaryPageHeader: dictionaryPageHeader,
            dataPageHeaderV2: dataPageHeaderV2
        )
    }

    func readDataPageHeader() throws -> ThriftDataPageHeader {
        var numValues: Int32?
        var encoding: ThriftEncoding?
        var definitionLevelEncoding: ThriftEncoding?
        var repetitionLevelEncoding: ThriftEncoding?
        var statistics: ThriftStatistics?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // num_values
                numValues = try readVarint32()
            case 2: // encoding
                let value = try readVarint32()
                encoding = ThriftEncoding(rawValue: value)
            case 3: // definition_level_encoding
                let value = try readVarint32()
                definitionLevelEncoding = ThriftEncoding(rawValue: value)
            case 4: // repetition_level_encoding
                let value = try readVarint32()
                repetitionLevelEncoding = ThriftEncoding(rawValue: value)
            case 5: // statistics
                statistics = try readStatistics()
            default:
                try skipField(type: field.type)
            }
        }

        guard let numValues = numValues,
              let encoding = encoding,
              let definitionLevelEncoding = definitionLevelEncoding,
              let repetitionLevelEncoding = repetitionLevelEncoding else {
            throw ThriftError.invalidData("Missing required fields in DataPageHeader")
        }

        return ThriftDataPageHeader(
            numValues: numValues,
            encoding: encoding,
            definitionLevelEncoding: definitionLevelEncoding,
            repetitionLevelEncoding: repetitionLevelEncoding,
            statistics: statistics
        )
    }

    func readDictionaryPageHeader() throws -> ThriftDictionaryPageHeader {
        var numValues: Int32?
        var encoding: ThriftEncoding?
        var isSorted: Bool?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // num_values
                numValues = try readVarint32()
            case 2: // encoding
                let value = try readVarint32()
                encoding = ThriftEncoding(rawValue: value)
            case 3: // is_sorted
                isSorted = field.type == .boolTrue
            default:
                try skipField(type: field.type)
            }
        }

        guard let numValues = numValues,
              let encoding = encoding else {
            throw ThriftError.invalidData("Missing required fields in DictionaryPageHeader")
        }

        return ThriftDictionaryPageHeader(
            numValues: numValues,
            encoding: encoding,
            isSorted: isSorted
        )
    }

    func readDataPageHeaderV2() throws -> ThriftDataPageHeaderV2 {
        var numValues: Int32?
        var numNulls: Int32?
        var numRows: Int32?
        var encoding: ThriftEncoding?
        var definitionLevelsByteLength: Int32?
        var repetitionLevelsByteLength: Int32?
        var isCompressed: Bool = true  // Default is true
        var statistics: ThriftStatistics?

        var lastFieldId: Int16 = 0

        while let field = try readFieldHeader(lastFieldId: &lastFieldId) {
            switch field.fieldId {
            case 1: // num_values
                numValues = try readVarint32()
            case 2: // num_nulls
                numNulls = try readVarint32()
            case 3: // num_rows
                numRows = try readVarint32()
            case 4: // encoding
                let value = try readVarint32()
                encoding = ThriftEncoding(rawValue: value)
            case 5: // definition_levels_byte_length
                definitionLevelsByteLength = try readVarint32()
            case 6: // repetition_levels_byte_length
                repetitionLevelsByteLength = try readVarint32()
            case 7: // is_compressed
                isCompressed = field.type == .boolTrue
            case 8: // statistics
                statistics = try readStatistics()
            default:
                try skipField(type: field.type)
            }
        }

        guard let numValues = numValues,
              let numNulls = numNulls,
              let numRows = numRows,
              let encoding = encoding,
              let definitionLevelsByteLength = definitionLevelsByteLength,
              let repetitionLevelsByteLength = repetitionLevelsByteLength else {
            throw ThriftError.invalidData("Missing required fields in DataPageHeaderV2")
        }

        return ThriftDataPageHeaderV2(
            numValues: numValues,
            numNulls: numNulls,
            numRows: numRows,
            encoding: encoding,
            definitionLevelsByteLength: definitionLevelsByteLength,
            repetitionLevelsByteLength: repetitionLevelsByteLength,
            isCompressed: isCompressed,
            statistics: statistics
        )
    }
}
