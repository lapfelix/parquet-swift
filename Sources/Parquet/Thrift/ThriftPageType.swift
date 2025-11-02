// Thrift PageType enum - Page types in Parquet format
//
// Licensed under the Apache License, Version 2.0

/// Type of page in a column chunk.
///
/// Maps directly to the Thrift `PageType` enum.
public enum ThriftPageType: Int32, Sendable {
    case dataPage = 0
    case indexPage = 1
    case dictionaryPage = 2
    case dataPageV2 = 3

    public var name: String {
        switch self {
        case .dataPage: return "DATA_PAGE"
        case .indexPage: return "INDEX_PAGE"
        case .dictionaryPage: return "DICTIONARY_PAGE"
        case .dataPageV2: return "DATA_PAGE_V2"
        }
    }
}
