# Thrift Implementation Limitations

This document details the Thrift structures and fields that are **intentionally skipped** or **not yet supported** in the current Phase 1 implementation.

## Current Status: M1.3 Complete

**Implemented:** Complete Compact Binary Protocol deserializer with all metadata structures
**Focus:** Read-only support for Phase 1 (Practical Reader)
**Version:** Parquet format 2.x

---

## Unsupported Structures (Phase 1)

These structures are defined in `parquet.thrift` but **not implemented** for Phase 1:

### 1. Encryption Metadata ❌
**Status:** Not supported in Phase 1
**Reason:** Encryption is advanced feature (Phase 4+)

- `EncryptionAlgorithm` (union)
- `AesGcmV1` (struct)
- `AesGcmCtrV1` (struct)
- `ColumnCryptoMetaData` (union)
- `EncryptionWithFooterKey` (struct)
- `EncryptionWithColumnKey` (struct)
- `FileCryptoMetaData` (struct)

**Impact:** Cannot read encrypted Parquet files
**Workaround:** Use unencrypted files for Phase 1 testing

### 2. Bloom Filters ❌
**Status:** Partially implemented (structures exist, no reading logic)
**Reason:** Advanced filtering feature (Phase 4+)

- `BloomFilterHeader` (struct)
- `BloomFilterAlgorithm` (union)
- `BloomFilterHash` (union)
- `BloomFilterCompression` (union)
- `SplitBlockAlgorithm` (struct)
- `XxHash` (struct)
- `Uncompressed` (struct)

**Impact:** Cannot use Bloom filters for predicate pushdown
**Workaround:** Read and filter data in memory

### 3. Page Index ❌
**Status:** Structures exist but not used
**Reason:** Optional optimization (Phase 4+)

- `ColumnIndex` (struct) - Per-page min/max statistics
- `OffsetIndex` (struct) - Page location information
- `PageLocation` (struct) - Individual page offsets
- `BoundaryOrder` (enum) - Min/max ordering

**Impact:** Cannot skip pages based on statistics
**Workaround:** Read all pages sequentially

### 4. Size Statistics ❌
**Status:** Structure exists but not used
**Reason:** Memory estimation feature (Phase 4+)

- `SizeStatistics` (struct)
  - `unencoded_byte_array_data_bytes`
  - `repetition_level_histogram`
  - `definition_level_histogram`

**Impact:** Cannot estimate memory requirements upfront
**Workaround:** Allocate memory as needed during read

### 5. Geospatial Types ❌
**Status:** Not supported
**Reason:** Specialized logical type (Future enhancement)

- `GeometryType` (struct)
- `GeographyType` (struct)
- `GeospatialStatistics` (struct)
- `BoundingBox` (struct)
- `EdgeInterpolationAlgorithm` (enum)

**Impact:** Cannot read geospatial Parquet files
**Workaround:** Treat as BYTE_ARRAY and parse externally

### 6. Variant Type ❌
**Status:** Not supported
**Reason:** New feature in recent Parquet versions

- `VariantType` (struct)

**Impact:** Cannot read Variant logical type
**Workaround:** Not applicable (rarely used)

---

## Partially Supported Structures

These structures are **implemented** but have **limited functionality** in Phase 1:

### 1. Data Page V2 ⚠️
**Status:** Structure exists, **not read in Phase 1**
**Implementation:** `ThriftDataPageHeaderV2` and deserialization complete

- Phase 1: **Only reads v1 data pages**
- Phase 2: Full v2 support (uncompressed levels)

**Reason:** V1 is more common, V2 adds complexity

### 2. Index Pages ⚠️
**Status:** Skipped during reading
**Implementation:** Header parsing exists but index page content ignored

- `IndexPageHeader` - Defined as empty stub in spec
- No actual fields defined

**Impact:** None (index pages are optional)

### 3. Delta Encodings ⚠️
**Status:** Enum values defined, **decode logic Phase 2**

- `DELTA_BINARY_PACKED`
- `DELTA_LENGTH_BYTE_ARRAY`
- `DELTA_BYTE_ARRAY`

**Impact:** Cannot read delta-encoded columns
**Workaround:** Files with PLAIN or RLE_DICTIONARY work fine

### 4. Byte Stream Split ⚠️
**Status:** Enum value defined, **decode logic Phase 2+**

- `BYTE_STREAM_SPLIT` (encoding)

**Impact:** Cannot read byte-stream-split encoded columns
**Workaround:** Rare encoding, mostly for FLOAT/DOUBLE

---

## Fields Intentionally Skipped

These fields **are read but ignored** during deserialization:

### FileMetaData
- `encryption_algorithm` (field 8) - Skipped (encryption not supported)
- `footer_signing_key_metadata` (field 9) - Skipped (encryption not supported)

### ColumnMetaData
- `bloom_filter_offset` (field 14) - Read but not used (Phase 4)
- `bloom_filter_length` (field 15) - Read but not used (Phase 4)
- `size_statistics` (field 16) - Not read (Phase 4)
- `geospatial_statistics` (field 17) - Not read (geospatial not supported)

### ColumnChunk
- `crypto_metadata` (field 8) - Not read (encryption not supported)
- `encrypted_column_metadata` (field 9) - Not read (encryption not supported)

### RowGroup
- `sorting_columns` (field 4) - Read but not used (Phase 2+)
- `ordinal` (field 7) - Read but not used (optimization, Phase 4)

---

## Metadata Wrapper Layer (M1.6)

### Fields Not Yet Exposed in Swift API

The metadata wrapper layer (M1.6) provides idiomatic Swift APIs around Thrift metadata.
Some fields are parsed by Thrift but not yet exposed through the wrapper API:

#### FileMetadata
- ✅ `version` - Exposed
- ✅ `schema` - Exposed as `Schema` object
- ✅ `numRows` - Exposed
- ✅ `rowGroups` - Exposed as `[RowGroupMetadata]`
- ✅ `keyValueMetadata` - Exposed as `[String: String]`
- ✅ `createdBy` - Exposed
- ⚠️ `columnOrders` - Exposed as raw `[ThriftColumnOrder]?` (not yet wrapped)
  - **Status:** Low priority, rarely used in Phase 1
  - **TODO:** Create `ColumnOrder` wrapper type in Phase 2+

#### ColumnMetadata
- ✅ `physicalType`, `encodings`, `codec`, `path` - All exposed
- ✅ `numValues`, `totalUncompressedSize`, `totalCompressedSize` - All exposed
- ✅ `dataPageOffset`, `dictionaryPageOffset`, `indexPageOffset` - All exposed
- ✅ `statistics` - Exposed as `Statistics` wrapper
- ✅ `keyValueMetadata` - Exposed as `[String: String]`
- ✅ `encodingStats` - Exposed as `[EncodingStat]`
- ✅ `bloomFilterOffset`, `bloomFilterLength` - Exposed (not usable yet)
- ⚠️ `sizeStatistics` - Not exposed
  - **Status:** Thrift struct exists but not read (Phase 4)
  - **Impact:** Cannot estimate memory requirements upfront

#### RowGroupMetadata
- ✅ `numRows`, `totalByteSize`, `columns` - All exposed
- ✅ `fileOffset`, `totalCompressedSize`, `ordinal` - All exposed
- ⚠️ `sortingColumns` - Not exposed
  - **Status:** Thrift reads but wrapper doesn't expose (Phase 2+)
  - **Impact:** Cannot determine if row group is sorted

**Note:** All unexposed fields can still be accessed via the internal `thrift` property
if needed for advanced use cases. The wrapper API focuses on common Phase 1 operations.

---

## Known Limitations

### 1. Thrift Protocol
- **Only Compact Binary Protocol** supported
- Standard Binary Protocol: ❌ Not implemented
- JSON Protocol: ❌ Not implemented

**Impact:** Cannot read files using non-compact protocols
**Note:** Compact is the standard for Parquet, 99%+ of files use it

### 1b. Unsigned Varint Support (Fixed in M1.4)
- **Binary/string lengths use unsigned varints** (not zigzag encoded)
- **Collection sizes use unsigned varints** (not zigzag encoded)
- **Signed integers (i16, i32, i64) use zigzag-encoded varints**

**Bug Fix (2025-11-02):**
- Previously all varints were zigzag decoded, which was incorrect for lengths/sizes
- This caused "Negative binary length" errors with real Parquet files
- Fixed by separating `readUnsignedVarint()` from `readVarint()`

**Status:** ✅ Fixed - Integration tests with apache/parquet-testing files now pass

### 1c. Schema Root Name Flexibility (Fixed in M1.4)
- **Schema root can have any name**, not just "schema"
- Common names in real files: "schema", "hive_schema", "m" (message), etc.

**Change (2025-11-02):**
- Previously required root element to be named "schema" (lowercase)
- This was too strict and failed with real Parquet files
- Now validates root is a group with children, regardless of name

**Status:** ✅ Fixed - Schema builder accepts any valid root name

### 2. Deprecated Types
These are **parsed but marked deprecated**:

- `INT96` physical type (legacy timestamps)
- `PLAIN_DICTIONARY` encoding (use `RLE_DICTIONARY`)
- `BIT_PACKED` encoding (use `RLE`)
- `ConvertedType` enum (superseded by `LogicalType`)

**Support:** Read support for compatibility, but deprecated in new files

### 3. Compression Codecs
Phase 1 support:

- ✅ `UNCOMPRESSED`
- ✅ `GZIP` (via Foundation)
- ⚠️ `SNAPPY` (best-effort, needs external library)
- ❌ `LZ4`, `LZ4_RAW`, `ZSTD`, `BROTLI`, `LZO` (Phase 2+)

---

## Forward Compatibility

The implementation supports **forward compatibility** through:

1. **Unknown field skipping** - Fields with unrecognized IDs are skipped
2. **Optional field handling** - All optional fields properly handled as nil
3. **Union type extensibility** - Unknown union variants skipped gracefully

**Result:** Can read files from newer Parquet versions, ignoring new features

---

## Testing Gaps

### Current Test Coverage
- ✅ Basic type reading (varint, string, binary, double)
- ✅ Field header parsing (delta encoding, boolean encoding)
- ✅ Error handling (EOF, invalid data, protocol errors)
- ✅ Simple struct reading (KeyValue, Statistics)
- ✅ Enum raw value validation

### Missing Tests (Need Real Fixtures)
- ⚠️ Complete FileMetaData parsing from real files
- ⚠️ Full SchemaElement tree with nested types
- ⚠️ RowGroup with multiple ColumnChunks
- ⚠️ PageHeader variants (data, dictionary, v2)
- ⚠️ LogicalType union deserialization

**Reason:** Hand-crafting Thrift Compact Binary is error-prone
**Solution:** Use real Parquet files from apache/parquet-testing

---

## Recommendations for Phase 2+

### High Priority
1. **Real Parquet fixtures** - Add test files from parquet-testing repo
2. **Data Page V2** - Implement uncompressed levels reading
3. **Delta encodings** - DELTA_BINARY_PACKED most common
4. **Additional compression** - LZ4, ZSTD are popular

### Medium Priority
5. **Bloom filters** - Useful for predicate pushdown
6. **Page index** - Skip pages based on statistics
7. **Size statistics** - Memory estimation

### Low Priority
8. **Encryption** - Enterprise feature
9. **Geospatial types** - Niche use case
10. **Standard Binary Protocol** - Rarely used

---

## Version Information

- **Parquet Format Version:** 2.x (latest as of 2025)
- **Thrift Source:** `apache/arrow/cpp/src/parquet/parquet.thrift`
- **Implementation Date:** 2025-11-02
- **Phase:** Phase 1 (M1.6 Complete - Metadata Parsing)

---

## Related Documentation

- [Implementation Roadmap](implementation-roadmap.md) - Overall development plan
- [Phase Review](phase-review.md) - Detailed phase breakdown
- [C++ Analysis](cpp-analysis.md) - Reference implementation notes
- [API Guide](api-guide.md) - User-facing API (draft)

---

**Last Updated:** 2025-11-02
**Status:** M1.6 Complete - Metadata Parsing ✅
