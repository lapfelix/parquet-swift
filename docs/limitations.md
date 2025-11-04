# Known Limitations - Phase 3

This document tracks known limitations and compatibility issues in the current implementation.

**Latest Updates**:
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

### Nested Types (PARTIALLY IMPLEMENTED)

**Status**: 🚧 Partially implemented

**Supported**:
- ✅ Single-level repeated columns (maxRepetitionLevel = 1)
- ✅ **Multi-level nested lists** (maxRepetitionLevel > 1) ✨ NEW!
  - ✅ Lists of lists (e.g., `[[[1, 2], [3]], [[4]]]`)
  - ✅ Distinguishes NULL lists vs EMPTY lists
  - ✅ Handles all edge cases (null inner/outer lists, empty inner/outer lists)
  - ✅ `readAllNested()` API returns nested arrays

**Missing Support**:
- ❌ Nested structs
- ❌ Maps
- ❌ Lists of structs

**Impact**: Can read primitive columns, single-level arrays, and multi-level nested lists. Cannot read maps or structs yet.

## Summary

Phase 3 implementation supports:
- ✅ parquet-mr generated files (Spark, Hive, parquet-mr tools)
- ✅ **PyArrow-generated files** (parquet-cpp-arrow) ✨
- ✅ PLAIN encoding
- ✅ **Dictionary encoding (RLE_DICTIONARY, PLAIN_DICTIONARY)** ✨
- ✅ UNCOMPRESSED, GZIP, and **Snappy** compression
- ✅ **All primitive types: Int32, Int64, Float, Double, String** ✨
- ✅ **Required (non-nullable) columns** ✨
- ✅ **Nullable columns (definition level support)** ✨
- ✅ **Repeated columns (single-level arrays/lists)** ✨
- ✅ **Multi-level nested lists (lists of lists)** ✨ NEW!

**Major Improvements**:
- ✅ **PyArrow compatibility** - Python ecosystem files now readable! (pandas, PyArrow, Dask) 🎉
- ✅ Snappy compression support (~80% of production files)
- ✅ Dictionary encoding for ALL primitive types (~90% of string/enum columns!)
- ✅ **Nullable column support** - can read NULL values in optional columns! (~90% of schemas!)
- ✅ **Repeated column support** - can read arrays/lists with empty lists and null elements! 🎉

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

**Phase 3 Achievement:**

Nullable columns now fully supported! The implementation decodes definition levels from each page
to determine which values are NULL. Both PLAIN and dictionary encoding work correctly with
nullable columns.

Still **does not work** with:
- ❌ Complex nested types (lists of structs, maps, nested structs) - Phase 4+

Completed milestones:
1. ✅ **Dictionary encoding for required columns** (Phase 2.1)
2. ✅ **Extend dictionary encoding to all types** (Phase 2.2)
3. ✅ **Definition levels** (nullable columns) (Phase 3) ✨
4. ✅ **PyArrow compatibility** (Python ecosystem) ✨
5. ✅ **Repetition levels and array reconstruction** (Phase 3) ✨ DONE!
   - ✅ Decode repetition levels from pages
   - ✅ Reconstruct arrays from flat value sequences
   - ✅ Handle empty lists and null elements
   - ✅ `readAllRepeated()` API for all primitive types
6. ✅ **Multi-level nested lists** (Phase 3) ✨ DONE!
   - ✅ `readAllNested()` API for maxRepetitionLevel > 1
   - ✅ ArrayReconstructor with explicit ListState tracking
   - ✅ Follows Apache Arrow's DefRepLevelsToListInfo pattern
   - ✅ Handles NULL vs EMPTY vs POPULATED lists correctly
   - ✅ Comprehensive test coverage for all edge cases

Remaining priorities:
7. **Complex nested types** (lists of structs, maps, nested structs) - Phase 4+

---

## Nested Structure Limitations (Phase 3)

**Added**: 2025-11-03

### ❌ CRITICAL: Structs Containing Complex Children

**Status**: Not supported - throws error with workarounds

**Problem**: Structs with complex children (maps, lists, nested structs) require multi-level reconstruction not yet implemented.

**Example Schemas**:
- `struct { string name; map<string,int> attrs; }`
- `struct { int32 id; list<string> tags; }`
- `struct { struct inner { ... } }`

**Behavior**:
- ❌ `readStruct()` throws `unsupportedType` error
- ❌ `readRepeatedStruct()` throws `unsupportedType` error
- ✅ Clear error message with workarounds

**Error Message**:
```
Structs containing complex fields (maps, lists, nested structs) are not yet supported.

Workarounds:
1. Read maps directly: readMap(at: ["your_struct", "map_field"])
2. Read lists directly: readRepeatedStruct(at: ["your_struct", "list_field", "list", "element"])
3. Read primitive fields individually via column readers

This limitation will be removed once proper multi-level reconstruction (LevelInfo) is implemented.
```

**When This Will Be Fixed**: Phase 4 - Port Arrow C++'s `DefRepLevelsToListInfo` for proper multi-level reconstruction

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

