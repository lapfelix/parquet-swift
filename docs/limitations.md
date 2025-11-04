# Known Limitations - Phase 4

This document tracks known limitations and compatibility issues in the current implementation.

**Latest Updates**:
- ✅ **Structs with complex children fully supported (Phase 4.5)** ✨ NEW!
- ✅ Array reconstruction for repeated columns (Phase 3)
- ✅ PyArrow compatibility fixed (Phase 3)
- ✅ Nullable columns fully supported (Phase 3)
- ✅ Dictionary encoding for all types (Phase 2)

## Parquet Metadata Format Compatibility

### PyArrow-Generated Files (FIXED ✅)

**Status**: ✅ **Fully supported as of Phase 3!**

**What Was Fixed**: Three critical bugs in the Thrift Compact Binary Protocol parser were identified and fixed:

1. **LogicalType early return bug**: `readLogicalType()` was returning immediately after reading a field (e.g., `.string`), preventing it from consuming the STOP byte that ends the struct. This caused 1-byte misalignment.

2. **TimeUnit early return bug**: Same pattern in `readTimeUnit()` - early returns prevented STOP byte consumption.

3. **skipStruct() bug (CRITICAL)**: `skipStruct()` was only reading field headers but not actually skipping the field data. This caused severe misalignment when encountering unknown struct fields like the newer `size_statistics` field in ColumnMetaData.

**Root Cause**: The parser wasn't properly handling:
- Struct STOP byte consumption in nested type readers
- Skipping unknown fields introduced in newer Parquet versions
- Forward compatibility with extended Thrift schemas

**Test Coverage**:
- ✅ `testPyArrowGeneratedFile()` - verifies PyArrow 21.0.0 compatibility
- ✅ Includes `pyarrow_test.parquet` fixture (5 rows, 3 columns)

**Files Now Supported**:
- ✅ Any Parquet file with `created_by: "parquet-cpp-arrow version X.X.X"`
- ✅ Tested with PyArrow 21.0.0
- ✅ parquet-mr generated files (Spark, Hive, parquet-mr tools) still work

**Impact**: Python ecosystem files (pandas, PyArrow, Dask) are now fully readable!

## Encoding Support

### Dictionary Encoding (FULLY SUPPORTED ✅)

**Status**: ✅ **Fully supported in Phase 2 and 3!**

**Supported Encodings**:
- ✅ `PLAIN_DICTIONARY` (deprecated but supported)
- ✅ `RLE_DICTIONARY`
- ✅ PLAIN encoding

**Supported Types**:
- ✅ Int32, Int64, Float, Double, String
- ✅ Required (non-nullable) columns
- ✅ Nullable columns with definition levels

**Impact**: Most real-world Parquet files with dictionary encoding are now fully readable!

## Compression Support

### Snappy Compression (IMPLEMENTED ✅)

**Status**: ✅ Implemented in Phase 2 (M2.0) - Pure Swift!

**Supported Codecs**:
- ✅ UNCOMPRESSED
- ✅ GZIP
- ✅ **SNAPPY** (most common in production) - Pure Swift implementation!

**Unsupported Codecs**:
- ❌ LZ4
- ❌ LZ4_RAW
- ❌ ZSTD
- ❌ BROTLI
- ❌ LZO

**Implementation**: Uses [snappy-swift](https://github.com/codelynx/snappy-swift), a pure Swift implementation with:
- ✅ **Zero system dependencies** - no brew/apt installation required
- ✅ **100% C++ compatible** - verified against Google's reference implementation
- ✅ **Fast performance** - 64-128 MB/s compression, 203-261 MB/s decompression
- ✅ **Cross-platform** - works on macOS, iOS, Linux, and all Swift platforms

**Build**: Simple `swift build` - no environment variables needed!

**Impact**: Most production Parquet files now readable! Snappy is the default compression in Apache Spark.

## Test Fixtures

**Status**: ✅ Good test coverage

**Available Fixtures**:
- ✅ `alltypes_plain.parquet`: Dictionary encoding, multiple types
- ✅ `datapage_v1-snappy-compressed-checksum.parquet`: Snappy compression (parquet-mr 1.13.0)
- ✅ `pyarrow_test.parquet`: PyArrow-generated with nullable columns (parquet-cpp-arrow 21.0.0)

**Coverage**:
- ✅ Both parquet-mr and PyArrow generated files
- ✅ Multiple encodings: PLAIN, RLE_DICTIONARY
- ✅ Multiple codecs: UNCOMPRESSED, GZIP, Snappy
- ✅ All primitive types: Int32, Int64, Float, Double, String
- ✅ Required and nullable columns

## Column Features

### Nullable Columns (IMPLEMENTED ✅)

**Status**: ✅ Implemented in Phase 3!

**Supported**:
- Definition level decoding (RLE/bit-packed hybrid encoding)
- Nullable columns for all primitive types (Int32, Int64, Float, Double, String)
- Both PLAIN and dictionary encoding with nullable columns
- Correct null value representation in returned arrays

**API Changes**:
- Column readers now return optional arrays: `[Int32?]`, `[Int64?]`, `[Float?]`, `[Double?]`, `[String?]`
- `readOne()` returns double optional (outer for end-of-stream, inner for NULL value)
- Required columns return all non-nil values
- Nullable columns return nil for NULL values

**Impact**: Most real Parquet files with nullable columns are now readable!

### Nested Types (MOSTLY IMPLEMENTED)

**Status**: ✅ Mostly implemented (Phase 4.5)

**Supported**:
- ✅ Single-level repeated columns (maxRepetitionLevel = 1)
- ✅ **Multi-level nested lists** (maxRepetitionLevel > 1)
  - ✅ Lists of lists (e.g., `[[[1, 2], [3]], [[4]]]`)
  - ✅ Distinguishes NULL lists vs EMPTY lists
  - ✅ Handles all edge cases (null inner/outer lists, empty inner/outer lists)
  - ✅ `readAllNested()` API returns nested arrays
- ✅ **Structs** (Phase 4) ✨
  - ✅ Simple structs (scalar fields only)
  - ✅ Nested structs (struct in struct)
  - ✅ Nullable structs and nullable fields
  - ✅ **Structs with complex children (maps, lists)** ✨ NEW in Phase 4.5!
- ✅ **Maps** (Phase 4)
  - ✅ Root-level maps: `map<primitive, primitive>`
  - ✅ Maps with NULL keys or values
  - ✅ Empty maps vs NULL maps

**Missing Support**:
- ❌ Lists of structs with complex children (workaround available)
- ❌ Deeply nested combinations (e.g., `list<map<string, list<struct>>>`)

**Impact**: Can read most common nested structures including structs with maps/lists!

## Summary

Phase 4 implementation supports:
- ✅ parquet-mr generated files (Spark, Hive, parquet-mr tools)
- ✅ **PyArrow-generated files** (parquet-cpp-arrow) ✨
- ✅ PLAIN encoding
- ✅ **Dictionary encoding (RLE_DICTIONARY, PLAIN_DICTIONARY)** ✨
- ✅ UNCOMPRESSED, GZIP, and **Snappy** compression
- ✅ **All primitive types: Int32, Int64, Float, Double, String** ✨
- ✅ **Required (non-nullable) columns** ✨
- ✅ **Nullable columns (definition level support)** ✨
- ✅ **Repeated columns (single-level arrays/lists)** ✨
- ✅ **Multi-level nested lists (lists of lists)** ✨
- ✅ **Structs** (simple, nested, and with complex children) ✨ NEW!
- ✅ **Maps** (root-level, nullable keys/values) ✨ NEW!
- ✅ **Structs with maps/lists** ✨ NEW in Phase 4.5!

**Major Improvements**:
- ✅ **PyArrow compatibility** - Python ecosystem files now readable! (pandas, PyArrow, Dask) 🎉
- ✅ Snappy compression support (~80% of production files)
- ✅ Dictionary encoding for ALL primitive types (~90% of string/enum columns!)
- ✅ **Nullable column support** - can read NULL values in optional columns! (~90% of schemas!)
- ✅ **Repeated column support** - can read arrays/lists with empty lists and null elements! 🎉
- ✅ **Struct and Map support** - can read complex nested structures! 🎉 NEW!
- ✅ **Phase 4.5: Full struct support** - maps and lists accessible in structs! ✨ NEW!

### Dictionary Encoding - Complete Status

**What works:**
- ✅ **All primitive types**: Int32, Int64, Float, Double, String
- ✅ **Required columns** with dictionary encoding
- ✅ **Nullable columns** with dictionary encoding ✨ NEW in Phase 3!
- ✅ Both RLE_DICTIONARY and PLAIN_DICTIONARY encodings
- ✅ Full overflow protection in RLE decoder
- ✅ Strict byte-exact validation
- ✅ Definition level decoding for nullable columns

**What works with repeated columns:**
- ✅ **Single-level repeated columns** (maxRepetitionLevel = 1) - FULLY SUPPORTED!
  - ✅ Repetition levels decoded from pages
  - ✅ Array reconstruction logic implemented
  - ✅ `readAllRepeated()` API returns `[[T?]]` for arrays with nullable elements
  - ✅ Handles empty lists, null elements, and all primitive types
  - ✅ All 5 column types: Int32, Int64, Float, Double, String

**What doesn't work yet:**
- 🚧 **Complex nested types** - Partially implemented
  - ✅ **Nested lists** (lists of lists) - FULLY SUPPORTED! ✨
  - ❌ Lists of structs
  - ❌ Maps
  - ❌ Nested structs

**Phase 3-4 Achievements:**

- ✅ Nullable columns fully supported! Definition levels decoded from each page
- ✅ Both PLAIN and dictionary encoding work with nullable columns
- ✅ Structs and maps fully supported (Phase 4)
- ✅ **Structs with complex children (maps, lists) fully supported!** (Phase 4.5) ✨

Completed milestones:
1. ✅ **Dictionary encoding for required columns** (Phase 2.1)
2. ✅ **Extend dictionary encoding to all types** (Phase 2.2)
3. ✅ **Definition levels** (nullable columns) (Phase 3) ✨
4. ✅ **PyArrow compatibility** (Python ecosystem) ✨
5. ✅ **Repetition levels and array reconstruction** (Phase 3) ✨
   - ✅ Decode repetition levels from pages
   - ✅ Reconstruct arrays from flat value sequences
   - ✅ Handle empty lists and null elements
   - ✅ `readAllRepeated()` API for all primitive types
6. ✅ **Multi-level nested lists** (Phase 3) ✨
   - ✅ `readAllNested()` API for maxRepetitionLevel > 1
   - ✅ ArrayReconstructor with explicit ListState tracking
   - ✅ Follows Apache Arrow's DefRepLevelsToListInfo pattern
   - ✅ Handles NULL vs EMPTY vs POPULATED lists correctly
   - ✅ Comprehensive test coverage for all edge cases
7. ✅ **Struct and Map support** (Phase 4) ✨
   - ✅ Simple struct reading
   - ✅ Nested structs
   - ✅ Root-level map reading
   - ✅ Nullable structs and maps
8. ✅ **Structs with complex children** (Phase 4.5) ✨ NEW!
   - ✅ DefRepLevelsToBitmap for struct validity
   - ✅ Child array reconstruction (maps, lists, scalars)
   - ✅ Proper truncation to values_read bound
   - ✅ Map key type preservation (AnyHashable)
   - ✅ Schema node identity matching

Remaining priorities:
9. **Lists of structs with complex children** - Phase 5
10. **Deeply nested combinations** (e.g., `list<map<string, list<struct>>>`) - Phase 5+

---

## Nested Structure Limitations (Phase 3-4)

**Added**: 2025-11-03
**Updated**: 2025-11-04 (Phase 4.5 Complete)

### ✅ FIXED: Structs Containing Complex Children (Phase 4.5)

**Status**: ✅ **FULLY SUPPORTED as of Phase 4.5!**

**What Was Fixed**: Structs with complex children (maps, lists, repeated fields) now fully supported using Arrow C++ StructReader pattern.

**Example Schemas NOW WORKING**:
- ✅ `struct { string name; map<string,int> attrs; }` - struct with map field
- ✅ `struct { int32 id; list<string> tags; }` - struct with list field
- ✅ Repeated scalar fields in structs

**Implementation**:
- Uses Arrow C++ StructReader::BuildArray pattern
- DefRepLevelsToBitmap computes struct validity → values_read
- Each child BuildArray(values_read) with proper truncation
- Map fields returned as `[AnyHashable: Any?]` dictionaries
- List fields returned as `[[Any?]?]` arrays
- All children accessible via `StructValue.get()`

**Test Coverage**:
- ✅ Struct validity (NULL vs present)
- ✅ Map child reconstruction
- ✅ List child reconstruction
- ✅ Empty maps/lists vs NULL maps/lists
- ✅ Backward compatibility with simple structs

**Bug Fixes in Phase 4.5**:
1. ✅ Child arrays truncated to struct's values_read (HIGH priority)
2. ✅ Map key types preserved using AnyHashable (MEDIUM priority)
3. ✅ Schema node identity matching instead of substring paths (MEDIUM priority)

**Remaining Limitation**: List of structs with complex children not yet supported (see below)

### ⚠️ list<map> - Flattens Intermediate Dimension

**Status**: Partial support - reads but loses structure

**Problem**: Loses intermediate list dimension, merges maps

**Example**: `[[{a:1},{b:2}], [{c:3}]]` → `[[{a:1, b:2}], [{c:3}]]` (2 maps in first list merged into 1)

**Workaround**: None - requires LevelInfo port

**When This Will Be Fixed**: Phase 4 - Proper multi-level repetition support

### Implementation Details

See `docs/map-bugs-exposed.md` for:
- Detailed technical analysis
- Test coverage
- Future LevelInfo implementation plan

### What DOES Work

- ✅ Root-level maps: `map<primitive, primitive>`
- ✅ Flat structs: primitives only
- ✅ Simple `list<struct>`: primitives only  
- ✅ Multi-level lists: `list<list<T>>`

---

**For complete details and examples**: See above sections in this document.

