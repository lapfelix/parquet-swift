# Phase 10: Apache Arrow C++ Writer Implementation Analysis

**Date**: 2025-11-04
**Purpose**: Deep dive into Arrow C++ writer internals to inform Swift implementation
**Companion to**: phase10-writer-design.md

---

## Executive Summary

This document provides a comprehensive analysis of the Apache Arrow C++ Parquet writer implementation (`cpp/src/parquet/file_writer.cc`, `column_writer.cc`, etc.) to guide the Swift port. The C++ writer is battle-tested, handling edge cases and performance optimizations that should inform our Swift design.

**Key Insights**:
- Adaptive dictionary encoding with fallback
- Hierarchical metadata builder pattern
- Careful level computation and encoding
- Page boundary detection for nested data
- Statistics generation with size limits

---

## 1. File Writer Architecture

### Class Hierarchy

```cpp
// Public API (pimpl pattern)
class ParquetFileWriter {
  std::unique_ptr<Contents> contents_;  // Actual implementation
  std::shared_ptr<FileMetaData> file_metadata_;
};

// Internal implementation
class FileSerializer : public ParquetFileWriter::Contents {
  // Manages complete file lifecycle
};
```

**Key Design Decision**: The pimpl pattern separates public API stability from internal implementation details. Swift equivalent would use a similar pattern with a protocol-based abstraction.

### Output Stream Management

Arrow C++ uses `ArrowOutputStream` abstraction:

```cpp
class ArrowOutputStream {
  virtual Status Write(const void* data, int64_t nbytes) = 0;
  virtual Status Tell(int64_t* position) const = 0;
};
```

**Operations**:
- `Tell()` - Track current position for offset calculations
- `Write()` - Emit bytes to sink
- Little-endian conversion: `::arrow::bit_util::ToLittleEndian()` for metadata lengths

**Swift Implications**: Need similar abstraction over FileHandle, OutputStream, or memory buffers.

### Footer Writing Sequence

The footer is written in **reverse-readable** format:

```cpp
// 1. Write FileMetaData (Thrift-serialized)
file_metadata_->WriteTo(sink_);

// 2. Write metadata length (4 bytes, little-endian)
uint32_t metadata_len = static_cast<uint32_t>(Tell() - metadata_start);
::arrow::bit_util::ToLittleEndian(metadata_len, footer_buffer);
sink_->Write(footer_buffer, 4);

// 3. Write magic number "PAR1" (4 bytes)
sink_->Write("PAR1", 4);
```

**Why Reverse-Readable**: Readers can seek to end of file, read magic + length, then seek backward to metadata start. This enables random access without scanning the entire file.

**Swift Implementation Note**: Must ensure little-endian byte order on all platforms (including big-endian ARM variants).

---

## 2. Column Writer Implementation

### TypedColumnWriter Structure

```cpp
template <typename DType>
class TypedColumnWriterImpl : public TypedColumnWriter<DType> {
  using T = typename DType::c_type;  // Int32Type → int32_t

  // Core write method
  int64_t WriteBatch(
    int64_t num_values,
    const int16_t* def_levels,  // NULL if no nulls
    const int16_t* rep_levels,  // NULL if not repeated
    const T* values
  ) override;

  // Key members
  std::unique_ptr<Encoder> current_encoder_;
  std::unique_ptr<PageWriter> pager_;
  ColumnChunkMetaDataBuilder* metadata_;
  Statistics* chunk_statistics_;
};
```

**Type Mappings**:
- `BooleanType` → `bool`
- `Int32Type` → `int32_t`
- `Int64Type` → `int64_t`
- `FloatType` → `float`
- `DoubleType` → `double`
- `ByteArrayType` → `ByteArray` struct (length + pointer)
- `FLBAType` → `FixedLenByteArray` struct

### WriteBatch Flow

```cpp
int64_t WriteBatchInternal(int64_t num_values,
                           const int16_t* def_levels,
                           const int16_t* rep_levels,
                           const T* values) {
  // 1. Handle page boundaries for repeated columns
  //    (ensure pages split on record boundaries)
  DoInBatches(num_values, def_levels, rep_levels, values,
    [&](int64_t batch_size) {
      WriteMiniBatch(batch_size, def_levels, rep_levels, values);
    });

  // 2. Check if page is full
  if (EstimatedBufferedValueBytes() >= properties_->data_pagesize()) {
    BuildDataPage();  // Flush page
  }

  // 3. Check dictionary size limit
  if (has_dictionary_ && !fallback_) {
    CheckDictionarySizeLimit();
  }

  return num_values;
}
```

**Record Boundary Detection**: The `DoInBatches` method ensures pages don't split in the middle of a record when dealing with nested data. This is critical for correct reading.

### Dictionary Encoding Decision

```cpp
void CheckDictionarySizeLimit() {
  // Cast encoder to DictEncoder to access size
  auto dict_encoder = dynamic_cast<DictEncoder<DType>*>(current_encoder_.get());

  if (dict_encoder->dict_encoded_size() >=
      properties_->dictionary_pagesize_limit()) {
    // Dictionary too large - fall back to PLAIN
    fallback_ = true;

    // Write dictionary page if we have buffered data
    WriteDictionaryPage();

    // Flush buffered pages with dictionary indices
    FlushBufferedPages();

    // Switch to PLAIN encoder for subsequent pages
    current_encoder_ = MakeEncoder(Type::type, Encoding::PLAIN, ...);
  }
}
```

**Key Insight**: Dictionary encoding is **adaptive**. Start with dictionary, track size, fall back if limit exceeded. This is why dictionaries are buffered - the decision isn't final until the column chunk is complete.

**Default Limit**: `kDefaultDataPageSize` = 1MB

### Statistics Generation

```cpp
void UpdatePageStatistics(const T* values, int64_t num_values,
                         const int16_t* def_levels) {
  // Per-page statistics
  page_statistics_->Update(values, num_values, def_levels,
                          descr_->max_definition_level());

  // Size statistics (uncompressed data bytes)
  page_size_statistics_->AddPageStats(...);
}

void FinalizeChunkStatistics() {
  // Merge all page statistics into chunk statistics
  chunk_statistics_->Merge(*page_statistics_);

  // Apply size limits (default: 4KB max)
  auto [page_stats, page_size_stats] = GetPageStatistics();
  page_stats.ApplyStatSizeLimits(properties_->max_statistics_size());

  // Encode and write to metadata
  metadata_->SetStatistics(page_stats.Encode());
}
```

**Statistics Types**:
- **min/max**: Comparable values (excludes NULLs)
- **null_count**: Count of NULL values
- **distinct_count**: Optional (expensive, often skipped)

**Size Limit**: Stats larger than 4KB are dropped (not truncated) to avoid misleading query engines.

### Page Buffering Strategy

```cpp
// Two PageWriter implementations

// 1. SerializedPageWriter - immediate writes
class SerializedPageWriter : public PageWriter {
  int64_t WriteDataPage(const DataPage& page) override {
    // Write immediately to sink
    return WritePageHeader(page) + WritePageData(page);
  }
};

// 2. BufferedPageWriter - defers writes
class BufferedPageWriter : public PageWriter {
  std::shared_ptr<InMemoryOutputStream> buffer_;

  int64_t WriteDataPage(const DataPage& page) override {
    // Write to memory buffer
    serialized_writer_->WriteDataPage(page);  // Uses buffer_, not final sink
    return page.size;
  }

  void Close() override {
    // Flush entire buffer to final sink at once
    buffer_->Flush(final_sink_);
  }
};
```

**When Buffering is Used**:
- **Dictionary encoding**: Pages buffered until dictionary size decision finalized
- **Page index generation**: Offsets need adjustment after all pages written

**When Direct Writing is Used**:
- **PLAIN encoding**: No uncertainty, write immediately
- **No page index**: Offsets don't need adjustment

**Swift Consideration**: Use `Data` for buffering, flush to FileHandle/OutputStream when ready.

---

## 3. Level Encoding for Writing

### Definition Level Computation

Arrow C++ computes definition levels from Arrow Arrays (which have validity bitmaps):

```cpp
// From Arrow Array to definition levels
void ComputeDefLevels(const Array& array, LevelInfo level_info,
                     int16_t* def_levels) {
  int16_t max_def_level = level_info.def_level;
  const uint8_t* valid_bits = array.null_bitmap_data();
  int64_t valid_bits_offset = array.offset();

  for (int64_t i = 0; i < array.length(); ++i) {
    if (BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
      def_levels[i] = max_def_level;  // Present
    } else {
      def_levels[i] = max_def_level - 1;  // NULL
    }
  }
}
```

**LevelInfo Structure**:
```cpp
struct LevelInfo {
  int16_t def_level;  // Max definition level for this column
  int16_t rep_level;  // Max repetition level for this column
  int16_t repeated_ancestor_def_level;  // For nested null handling
  int16_t null_slot_usage;  // >1 for FixedSizeList
};
```

**Swift Equivalent**: Accept optional array `[Int32?]`, compute levels:
```swift
func computeDefinitionLevels(values: [Int32?], maxDefLevel: Int16) -> [Int16] {
  values.map { $0 == nil ? maxDefLevel - 1 : maxDefLevel }
}
```

### Repetition Level Computation

For nested lists, repetition levels mark list boundaries:

```cpp
// Simplified example for single-level list
void ComputeRepLevels(const ListArray& list_array, int16_t* rep_levels) {
  int64_t index = 0;

  for (int64_t i = 0; i < list_array.length(); ++i) {
    if (list_array.IsNull(i)) {
      rep_levels[index++] = 0;  // NULL list
      continue;
    }

    int64_t list_start = list_array.value_offset(i);
    int64_t list_end = list_array.value_offset(i + 1);
    int64_t list_length = list_end - list_start;

    if (list_length == 0) {
      rep_levels[index++] = 0;  // Empty list
      continue;
    }

    // First element: new list
    rep_levels[index++] = 0;

    // Subsequent elements: continuation
    for (int64_t j = 1; j < list_length; ++j) {
      rep_levels[index++] = 1;  // rep_level = list nesting depth
    }
  }
}
```

**Multi-level Lists**: The algorithm recurses through nested ListArray structures, incrementing rep_level at each nesting depth.

### RLE Encoding for Levels

```cpp
int64_t RleEncodeLevels(const int16_t* levels, int64_t num_levels,
                       int16_t max_level, uint8_t* output,
                       bool include_length_prefix = true) {
  int bit_width = BitUtil::Log2(max_level + 1);  // Minimum bits needed

  RleEncoder encoder(output, bit_width);
  for (int64_t i = 0; i < num_levels; ++i) {
    encoder.Put(levels[i]);
  }

  int64_t encoded_size = encoder.Flush();

  if (include_length_prefix) {
    // Data Page V1: prepend 4-byte length
    // (Data Page V2 uses different format)
    std::memmove(output + 4, output, encoded_size);
    *reinterpret_cast<uint32_t*>(output) = static_cast<uint32_t>(encoded_size);
    return encoded_size + 4;
  }

  return encoded_size;
}
```

**Bit Width Calculation**:
- max_level = 1 → 1 bit per value
- max_level = 3 → 2 bits per value
- max_level = 7 → 3 bits per value

**Length Prefix**: Data Page V1 requires 4-byte little-endian length before RLE data.

**Swift Implementation**: Can reuse existing RLE decoder logic, add encoder methods.

---

## 4. Page Writing

### BuildDataPageV1 Method

This is the **core page construction** logic:

```cpp
void BuildDataPageV1(int64_t num_values) {
  // 1. Encode definition levels (if column is nullable)
  int64_t def_levels_size = 0;
  if (descr_->max_definition_level() > 0) {
    def_levels_size = RleEncodeLevels(
      definition_levels_sink_.data(),
      definition_levels_rle_.get(),
      descr_->max_definition_level(),
      /*include_length_prefix=*/true
    );
  }

  // 2. Encode repetition levels (if column is repeated)
  int64_t rep_levels_size = 0;
  if (descr_->max_repetition_level() > 0) {
    rep_levels_size = RleEncodeLevels(
      repetition_levels_sink_.data(),
      repetition_levels_rle_.get(),
      descr_->max_repetition_level(),
      /*include_length_prefix=*/true
    );
  }

  // 3. Get encoded values from current encoder
  std::shared_ptr<Buffer> values = current_encoder_->FlushValues();

  // 4. Concatenate: rep_levels + def_levels + values
  std::shared_ptr<ResizableBuffer> uncompressed_data =
    AllocateBuffer(pool_, rep_levels_size + def_levels_size + values->size());

  ConcatenateBuffers(
    rep_levels_size,
    def_levels_size,
    values,
    uncompressed_data->mutable_data()
  );

  // 5. Compress if compressor configured
  std::shared_ptr<Buffer> compressed_data;
  if (pager_->has_compressor()) {
    pager_->Compress(*uncompressed_data, compressed_data);
  } else {
    compressed_data = uncompressed_data;
  }

  // 6. Create page statistics
  EncodedStatistics page_stats = page_statistics_->Encode();

  // 7. Build DataPage object
  DataPage page(
    compressed_data,
    num_values,
    current_encoder_->encoding(),
    Encoding::RLE,  // definition_level_encoding (always RLE)
    Encoding::RLE,  // repetition_level_encoding (always RLE)
    uncompressed_data->size(),
    page_stats
  );

  // 8. Write page via PageWriter
  total_bytes_written_ += pager_->WriteDataPage(page);

  // 9. Reset buffers for next page
  current_encoder_->Clear();
  definition_levels_sink_.clear();
  repetition_levels_sink_.clear();
}
```

**Critical Order**: Repetition levels MUST come before definition levels (Parquet spec requirement).

**Memory Layout**:
```
[Page Header - Thrift serialized]
[Repetition Levels - RLE with 4-byte length prefix] (if maxRepLevel > 0)
[Definition Levels - RLE with 4-byte length prefix]  (if maxDefLevel > 0)
[Values - PLAIN or RLE_DICTIONARY encoded]
```

### Dictionary Page Writing

```cpp
void WriteDictionaryPage() {
  auto dict_encoder = dynamic_cast<DictEncoder<DType>*>(current_encoder_.get());

  // 1. Allocate buffer for dictionary values
  std::shared_ptr<ResizableBuffer> buffer =
    AllocateBuffer(pool_, dict_encoder->dict_encoded_size());

  // 2. Encode dictionary values using PLAIN encoding
  dict_encoder->WriteDict(buffer->mutable_data());

  // 3. Create DictionaryPage
  DictionaryPage page(
    buffer,
    dict_encoder->num_entries(),  // Number of unique values
    properties_->dictionary_page_encoding()  // Always PLAIN
  );

  // 4. Write dictionary page (NO compression for dict pages in V1)
  total_bytes_written_ += pager_->WriteDictionaryPage(page);
}
```

**Important**: Dictionary pages:
- Always use PLAIN encoding
- Written BEFORE any data pages for that column chunk
- NOT compressed in Data Page V1 (only data pages are compressed)

### Page Header Creation

```cpp
void WritePageHeader(const DataPage& page, OutputStream* sink) {
  // Create Thrift PageHeader structure
  format::PageHeader thrift_header;
  thrift_header.__set_type(format::PageType::DATA_PAGE);
  thrift_header.__set_uncompressed_page_size(page.uncompressed_size());
  thrift_header.__set_compressed_page_size(page.size());
  thrift_header.__set_num_values(page.num_values());

  // Data page specific fields
  format::DataPageHeader data_page_header;
  data_page_header.__set_num_values(page.num_values());
  data_page_header.__set_encoding(ToThrift(page.encoding()));
  data_page_header.__set_definition_level_encoding(
    ToThrift(Encoding::RLE)
  );
  data_page_header.__set_repetition_level_encoding(
    ToThrift(Encoding::RLE)
  );

  // Statistics (optional)
  if (page.statistics().has_min) {
    data_page_header.__set_statistics(ToThrift(page.statistics()));
  }

  thrift_header.__set_data_page_header(data_page_header);

  // Serialize Thrift to output
  ThriftSerializer serializer;
  serializer.Serialize(&thrift_header, sink);
}
```

**Swift Note**: Need Thrift serialization for PageHeader (compact binary protocol).

---

## 5. Metadata Builders

### Hierarchical Builder Pattern

```cpp
// File level
class FileMetaDataBuilder {
  std::vector<std::unique_ptr<RowGroupMetaDataBuilder>> row_group_builders_;

  RowGroupMetaDataBuilder* AppendRowGroup() {
    auto builder = RowGroupMetaDataBuilder::Make(...);
    row_group_builders_.push_back(std::move(builder));
    return row_group_builders_.back().get();
  }

  std::unique_ptr<FileMetaData> Finish() {
    // Collect all row group metadata
    std::vector<std::unique_ptr<RowGroupMetaData>> row_groups;
    for (auto& builder : row_group_builders_) {
      row_groups.push_back(builder->Finish());
    }

    return FileMetaData::Make(schema_, row_groups, key_value_metadata_);
  }
};

// Row group level
class RowGroupMetaDataBuilder {
  std::vector<std::unique_ptr<ColumnChunkMetaDataBuilder>> column_builders_;
  int current_column_ = 0;

  ColumnChunkMetaDataBuilder* NextColumnChunk() {
    if (current_column_ >= schema_->num_columns()) {
      throw ParquetException("All columns already written");
    }
    return column_builders_[current_column_++].get();
  }

  std::unique_ptr<RowGroupMetaData> Finish(int64_t total_bytes_written,
                                           int16_t ordinal) {
    // Validate all columns written
    if (current_column_ != schema_->num_columns()) {
      throw ParquetException("Not all columns written");
    }

    // Collect column chunk metadata
    std::vector<std::unique_ptr<ColumnChunkMetaData>> columns;
    for (auto& builder : column_builders_) {
      columns.push_back(builder->Finish());
    }

    return RowGroupMetaData::Make(columns, total_bytes_written, ordinal);
  }
};

// Column chunk level
class ColumnChunkMetaDataBuilder {
  void SetStatistics(const EncodedStatistics& stats) {
    properties_.statistics = stats;
  }

  void Finish(int64_t num_values, int64_t dictionary_page_offset,
             int64_t index_page_offset, int64_t data_page_offset,
             int64_t compressed_size, int64_t uncompressed_size,
             bool has_dictionary, bool dictionary_fallback) {
    properties_.num_values = num_values;
    properties_.dictionary_page_offset = dictionary_page_offset;
    properties_.data_page_offset = data_page_offset;
    properties_.total_compressed_size = compressed_size;
    properties_.total_uncompressed_size = uncompressed_size;
    // ... other properties
  }

  std::unique_ptr<ColumnChunkMetaData> Build() {
    return ColumnChunkMetaData::Make(properties_);
  }
};
```

**Sequential Pattern**:
1. File builder creates row group builders
2. Row group builders create column chunk builders
3. Column chunk builders finalized first
4. Then row group builders finalized
5. Finally file builder creates FileMetaData

**Offset Tracking**: Each builder tracks file positions for metadata:
- `dictionary_page_offset`: Where dictionary page starts
- `data_page_offset`: Where first data page starts
- `total_compressed_size`: Sum of compressed page sizes
- `total_uncompressed_size`: Sum of uncompressed page sizes

---

## 6. Configuration Properties

### WriterProperties Defaults

From `cpp/src/parquet/properties.h`:

```cpp
// Page sizes
constexpr int64_t kDefaultDataPageSize = 1024 * 1024;  // 1MB
constexpr int64_t kDefaultMaxRowsPerPage = 20000;
constexpr int64_t DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT = kDefaultDataPageSize;

// Row group
constexpr int64_t DEFAULT_MAX_ROW_GROUP_LENGTH = 1024 * 1024;  // 1M rows

// Batch sizes
constexpr int64_t DEFAULT_WRITE_BATCH_SIZE = 1024;

// Encoding defaults
constexpr bool DEFAULT_IS_DICTIONARY_ENABLED = true;
constexpr Encoding::type DEFAULT_ENCODING = Encoding::UNKNOWN;  // Auto-select
constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;

// Statistics
constexpr bool DEFAULT_ARE_STATISTICS_ENABLED = true;
constexpr int64_t DEFAULT_MAX_STATISTICS_SIZE = 4096;  // 4KB

// Page index
constexpr bool DEFAULT_IS_PAGE_INDEX_ENABLED = true;

// File version
constexpr ParquetVersion::type DEFAULT_PARQUET_VERSION = ParquetVersion::PARQUET_2_6;
```

### Builder Pattern Configuration

```cpp
class WriterProperties {
public:
  class Builder {
    Builder* compression(Compression::type codec);
    Builder* compression(const std::string& path, Compression::type codec);
    Builder* compression_level(int level);

    Builder* enable_dictionary();
    Builder* disable_dictionary();
    Builder* enable_dictionary(const std::string& path);
    Builder* disable_dictionary(const std::string& path);

    Builder* dictionary_pagesize_limit(int64_t limit);
    Builder* data_pagesize(int64_t page_size);
    Builder* max_rows_per_page(int64_t max_rows);

    Builder* encoding(Encoding::type encoding);
    Builder* encoding(const std::string& path, Encoding::type encoding);

    Builder* enable_statistics();
    Builder* disable_statistics();
    Builder* max_statistics_size(int64_t max_size);

    Builder* enable_write_page_index();
    Builder* disable_write_page_index();

    std::shared_ptr<WriterProperties> build();
  };
};
```

**Per-Column Configuration**: Most properties can be set globally or per-column path (e.g., "address.city").

**Swift API Design**:
```swift
struct WriterProperties {
  var compression: CompressionType = .uncompressed
  var compressionLevel: Int? = nil
  var dictionaryEnabled: Bool = true
  var dictionaryPageSizeLimit: Int64 = 1024 * 1024
  var dataPageSize: Int64 = 1024 * 1024
  var maxRowsPerPage: Int64 = 20000
  var statisticsEnabled: Bool = true
  var maxStatisticsSize: Int64 = 4096

  // Per-column overrides
  var columnProperties: [String: ColumnProperties] = [:]
}
```

---

## 7. Common Patterns & Pitfalls

### Pattern 1: Encoder Lifecycle

```cpp
// Create encoder at column writer initialization
current_encoder_ = MakeEncoder(
  descr_->physical_type(),
  dictionary_enabled ? Encoding::PLAIN_DICTIONARY : Encoding::PLAIN,
  use_dictionary_,
  descr_,
  pool_
);

// Use encoder for multiple pages
for each write batch:
  current_encoder_->Put(values, num_values);

  if page full:
    buffer = current_encoder_->FlushValues();  // Get encoded data
    WriteDataPage(buffer);
    current_encoder_->Clear();  // Reset for next page

// On dictionary fallback
if fallback:
  WriteDictionaryPage();
  FlushBufferedPages();
  current_encoder_.reset(MakeEncoder(..., Encoding::PLAIN, ...));
```

**Pitfall**: Forgetting to call `Clear()` after `FlushValues()` causes data duplication across pages.

### Pattern 2: Statistics with Null Handling

```cpp
void TypedStatistics::Update(const T* values, int64_t num_values,
                            const int16_t* def_levels,
                            int16_t max_def_level) {
  for (int64_t i = 0; i < num_values; ++i) {
    if (def_levels[i] == max_def_level) {
      // Value is present
      UpdateMinMax(values[i]);
      num_values_++;
    } else {
      // Value is NULL
      null_count_++;
    }
  }
}
```

**Pitfall**: Including NULLs in min/max computation produces incorrect statistics.

### Pattern 3: Level Buffer Management

```cpp
// Separate buffers for levels and values
std::vector<int16_t> definition_levels_sink_;
std::vector<int16_t> repetition_levels_sink_;
std::unique_ptr<RleEncoder> definition_levels_rle_;
std::unique_ptr<RleEncoder> repetition_levels_rle_;

// Accumulate levels as values arrive
void WriteBatch(num_values, def_levels, rep_levels, values) {
  // Copy to sink
  definition_levels_sink_.insert(
    definition_levels_sink_.end(),
    def_levels,
    def_levels + num_values
  );

  // On page flush
  int64_t encoded_size = RleEncodeLevels(
    definition_levels_sink_.data(),
    definition_levels_sink_.size(),
    max_def_level,
    definition_levels_rle_.get()
  );

  // Clear sink for next page
  definition_levels_sink_.clear();
}
```

**Pitfall**: Not clearing buffers between pages causes level data to accumulate incorrectly.

### Pattern 4: Byte Order Handling

```cpp
// PLAIN encoding for Int32 (little-endian)
void PlainEncoder<Int32Type>::Put(const int32_t* src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    int32_t value = ::arrow::bit_util::ToLittleEndian(src[i]);
    sink_->Write(&value, sizeof(int32_t));
  }
}

// ByteArray encoding (length + data)
void PlainEncoder<ByteArrayType>::Put(const ByteArray* src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    uint32_t length = ::arrow::bit_util::ToLittleEndian(src[i].len);
    sink_->Write(&length, sizeof(uint32_t));  // 4-byte length
    sink_->Write(src[i].ptr, src[i].len);     // Raw bytes
  }
}
```

**Pitfall**: Forgetting little-endian conversion on big-endian platforms breaks compatibility.

### Pattern 5: Variable-Length Data Structures

```cpp
// ByteArray: non-owning view
struct ByteArray {
  uint32_t len;
  const uint8_t* ptr;

  static ByteArray FromString(const std::string& s) {
    return ByteArray{static_cast<uint32_t>(s.size()),
                    reinterpret_cast<const uint8_t*>(s.data())};
  }
};

// FixedLenByteArray: fixed-size binary
struct FixedLenByteArray {
  const uint8_t* ptr;
  // Length is implicit from schema
};
```

**Swift Equivalent**:
```swift
struct ByteArray {
  let length: UInt32
  let data: UnsafePointer<UInt8>

  init(string: String) {
    let utf8 = string.utf8
    self.length = UInt32(utf8.count)
    self.data = utf8.withContiguousStorageIfAvailable { $0.baseAddress! }!
  }
}
```

**Pitfall**: ByteArray doesn't own memory - must ensure source data lifetime exceeds encoder usage.

---

## 8. Algorithm Descriptions

### Dictionary Encoding Algorithm

**Phase 1: Accumulation**
```
1. Initialize empty dictionary: []
2. Initialize index map: {}
3. Initialize indices: []

For each value:
  If value in index_map:
    index = index_map[value]
  Else:
    index = dictionary.length
    dictionary.append(value)
    index_map[value] = index

  indices.append(index)

  Check dictionary size:
    If size > limit:
      Mark fallback = true
      Break
```

**Phase 2: Encoding**
```
If fallback:
  Write dictionary page (all values so far)
  Write buffered data pages (with indices)
  Switch to PLAIN encoder
  Continue with PLAIN encoding
Else:
  Write dictionary page
  For each data page:
    RLE encode indices
    Write data page
```

### Record Boundary Detection (for nested data)

**Purpose**: Ensure pages don't split mid-record when dealing with repeated fields.

```
Algorithm: FindRecordBoundary
Input:
  - rep_levels: repetition level array
  - start_index: where to begin search
  - target_count: desired number of values

Output: actual number of values to include (≤ target_count)

Implementation:
  current = start_index + target_count - 1

  // Walk backward until we find rep_level = 0 (new record)
  While current > start_index:
    If rep_levels[current] == 0:
      Return current - start_index  // Found record boundary
    current -= 1

  Return target_count  // No split needed (single record)
```

**Used in**: `DoInBatches` to ensure page boundaries align with record boundaries.

### Statistics Merging

**Merging page statistics into chunk statistics**:

```
Algorithm: MergeStatistics
Input: stats_list = [page_stats_1, page_stats_2, ...]
Output: merged_stats

merged_stats.min = min(stats.min for stats in stats_list if stats.has_min)
merged_stats.max = max(stats.max for stats in stats_list if stats.has_max)
merged_stats.null_count = sum(stats.null_count for stats in stats_list)
merged_stats.distinct_count = null  // Cannot merge accurately

merged_stats.has_min = any(stats.has_min for stats in stats_list)
merged_stats.has_max = any(stats.has_max for stats in stats_list)
merged_stats.has_null_count = all(stats.has_null_count for stats in stats_list)
```

**Why distinct_count can't be merged**: Page 1 might have {1, 2, 3} (distinct=3), Page 2 might have {2, 3, 4} (distinct=3), but combined distinct is 4, not 6.

---

## 9. Design Decisions & Rationale

### Decision 1: Buffered vs Immediate Page Writing

**Decision**: Use BufferedPageWriter for dictionary encoding, SerializedPageWriter for PLAIN.

**Rationale**:
- Dictionary decision isn't finalized until column chunk complete
- Buffering allows writing dictionary page before data pages
- Page index generation requires knowing all page offsets
- PLAIN encoding has no uncertainty, can write immediately

### Decision 2: RLE Encoding for Levels

**Decision**: Always use RLE encoding for definition and repetition levels.

**Rationale**:
- Parquet spec requires RLE for levels in Data Page V1
- Levels are typically highly repetitive (e.g., all non-null = all same level)
- RLE provides excellent compression for such data
- Bit-packing handles non-repetitive sections efficiently

### Decision 3: Statistics Size Limit

**Decision**: Drop statistics entirely if encoded size > 4KB (don't truncate).

**Rationale**:
- Truncated min/max would be incorrect and misleading
- Query engines rely on accurate statistics for pruning
- Better to have no stats than wrong stats
- 4KB is sufficient for most types (even long strings)

### Decision 4: Per-Column Configuration

**Decision**: Allow compression, encoding, statistics settings per column.

**Rationale**:
- Different columns have different characteristics
- String columns benefit from dictionary, numeric columns don't
- High-cardinality columns should disable dictionary
- Sensitive columns might disable statistics
- Flexibility without complexity

### Decision 5: Page Size Defaults

**Decision**: 1MB data page size, 20K max rows per page.

**Rationale**:
- 1MB balances compression efficiency vs memory usage
- Too small: overhead from many pages, poor compression
- Too large: memory pressure, slow seeks
- 20K rows prevents pathological cases (e.g., 1M tiny values)
- Aligns with most query engine optimizations

---

## 10. Edge Cases Handled in C++

### Edge Case 1: Empty Row Groups

```cpp
void RowGroupWriter::Close() {
  if (num_rows_ == 0) {
    // Arrow C++ allows empty row groups
    // But warns in debug builds
  }

  // Validate all columns have same row count
  for (auto& col : columns_) {
    if (col->rows_written() != num_rows_) {
      throw ParquetException("Column row count mismatch");
    }
  }
}
```

**Handling**: Allowed but discouraged. Metadata still written correctly.

### Edge Case 2: Dictionary Fallback Mid-Page

```cpp
void CheckDictionarySizeLimit() {
  if (dictionary_too_large && values_in_current_page > 0) {
    // We have values buffered in current page using dictionary
    // Can't switch encoding mid-page!

    // Finish current page with dictionary
    BuildDataPage();

    // THEN switch to PLAIN for next page
    SwitchToPlainEncoder();
  }
}
```

**Handling**: Never mix encodings within a page. Flush page first, then switch.

### Edge Case 3: NULL-only Pages

```cpp
void BuildDataPageV1() {
  if (all_values_null) {
    // Page has no actual values, only definition levels
    // Values buffer is empty

    page_stats.all_null_value = true;
    page_stats.has_min = false;
    page_stats.has_max = false;
    page_stats.null_count = num_values;
  }
}
```

**Handling**: Special statistics flag `all_null_value` for efficient NULL page detection.

### Edge Case 4: Very Long Strings

```cpp
void StatisticsAccumulator::Update(const ByteArray& value) {
  if (value.len > max_statistics_size_) {
    // String too long for statistics
    has_min_ = false;
    has_max_ = false;
    // But null_count still tracked
  }
}
```

**Handling**: Disable min/max if any value exceeds size limit, but continue tracking nulls.

### Edge Case 5: Nested NULL Handling

```cpp
// List of structs where entire list is NULL
// vs list with NULL struct elements

void ComputeLevels(const ListArray& list_array) {
  if (list_array.IsNull(i)) {
    // Entire list is NULL
    def_level = max_def_level - 1;
    rep_level = 0;
    // Single entry in level arrays
  } else {
    // List is present, but may have NULL elements
    for (element in list) {
      if (element.IsNull()) {
        def_level = repeated_ancestor_def_level;
      } else {
        def_level = max_def_level;
      }
      rep_level = (first_element ? 0 : list_rep_level);
    }
  }
}
```

**Handling**: Careful level computation distinguishes NULL container from NULL elements.

---

## 11. Swift Implementation Recommendations

### Recommendation 1: Type-Safe Writers

Follow the same pattern as readers - concrete types, not generics:

```swift
protocol ColumnWriter {
  func close() throws -> ColumnChunkMetaData
}

class Int32ColumnWriter: ColumnWriter {
  func writeValues(_ values: [Int32]) throws
  func writeOptionalValues(_ values: [Int32?]) throws
}

class StringColumnWriter: ColumnWriter {
  func writeValues(_ values: [String]) throws
  func writeOptionalValues(_ values: [String?]) throws
}
```

**Rationale**: Same generic limitations as decoders.

### Recommendation 2: Encoder Protocol

```swift
protocol Encoder {
  func encode(_ values: UnsafeBufferPointer<UInt8>) throws
  func flush() throws -> Data
  func clear()
  func estimatedSize() -> Int
}

class PlainEncoder: Encoder { ... }
class DictionaryEncoder: Encoder { ... }
class RLEEncoder: Encoder { ... }
```

### Recommendation 3: Page Writer Abstraction

```swift
protocol PageWriter {
  func writeDictionaryPage(_ page: DictionaryPage) throws
  func writeDataPage(_ page: DataPage) throws
  func close() throws
}

class SerializedPageWriter: PageWriter {
  let sink: OutputStream
  // Write immediately
}

class BufferedPageWriter: PageWriter {
  var buffer: Data
  // Write to buffer, flush on close
}
```

### Recommendation 4: Statistics Type Design

```swift
protocol Statistics {
  var nullCount: Int64 { get }
  var hasMinMax: Bool { get }

  func encode() throws -> Data
  mutating func merge(_ other: Self)
}

struct Int32Statistics: Statistics {
  var min: Int32?
  var max: Int32?
  var nullCount: Int64

  mutating func update(_ value: Int32)
  mutating func updateNull()
}
```

### Recommendation 5: Builder State Machine

```swift
class ParquetFileWriter {
  private enum State {
    case created
    case schemaSet
    case rowGroupOpen
    case closed
  }

  private var state: State = .created

  func setSchema(_ schema: Schema) throws {
    guard state == .created else {
      throw WriterError.invalidState("Schema already set")
    }
    // ...
    state = .schemaSet
  }
}
```

**Rationale**: Prevent misuse through type-safe state transitions.

---

## 12. Testing Recommendations

### Critical Test Cases

1. **Round-Trip Tests**:
   ```swift
   // Write → Read → Verify
   let writer = ParquetFileWriter(url: tempURL)
   writer.setSchema(schema)
   let rg = try writer.createRowGroup()
   let col = try rg.int32ColumnWriter(at: 0)
   try col.writeValues([1, 2, 3, 4, 5])
   try writer.close()

   let reader = try ParquetFileReader(url: tempURL)
   let values = try reader.rowGroup(at: 0).int32Column(at: 0).readAll()
   XCTAssertEqual(values, [1, 2, 3, 4, 5])
   ```

2. **PyArrow Compatibility**:
   ```python
   # Read Swift-written file with PyArrow
   table = pq.read_table('swift_output.parquet')
   assert table.num_rows == expected_rows
   assert table.column('id').to_pylist() == expected_values
   ```

3. **Dictionary Fallback**:
   ```swift
   // Force fallback by exceeding size limit
   let manyUniqueStrings = (0..<1_000_000).map { "string_\($0)" }
   try writer.writeValues(manyUniqueStrings)

   // Verify metadata shows PLAIN encoding (not RLE_DICTIONARY)
   let metadata = try reader.metadata
   XCTAssertEqual(metadata.rowGroup(0).column(0).encoding, .plain)
   ```

4. **Level Encoding**:
   ```swift
   // Nested structure with multiple NULLs
   let lists: [[Int32?]?] = [
     [1, nil, 3],
     nil,
     [],
     [nil, nil],
     [4, 5, 6]
   ]

   // Write and read back
   // Verify exact structure preserved
   ```

5. **Statistics Validation**:
   ```swift
   try writer.writeOptionalValues([nil, 5, 3, nil, 8, 1])

   let stats = metadata.rowGroup(0).column(0).statistics
   XCTAssertEqual(stats.min, 1)
   XCTAssertEqual(stats.max, 8)
   XCTAssertEqual(stats.nullCount, 2)
   ```

---

## Summary

The Apache Arrow C++ implementation provides a robust, well-tested foundation for the Swift writer. Key takeaways:

1. **Layered Architecture**: File → RowGroup → Column → Page hierarchy maintains clear separation of concerns

2. **Adaptive Encoding**: Dictionary encoding starts optimistically, falls back when limits exceeded

3. **Careful Level Handling**: Definition and repetition levels require precise computation and ordering

4. **Statistics Generation**: Track min/max/null_count per page, merge into chunk statistics, apply size limits

5. **Memory Management**: Page-level buffering prevents unbounded memory growth, dictionary buffering enables late decision-making

6. **Compatibility Focus**: Little-endian byte order, correct Thrift serialization, proper level encoding ensure broad compatibility

The Swift implementation should mirror these patterns while leveraging Swift's type safety, value semantics, and memory safety guarantees.

---

**Next Action**: Begin implementation of Milestone W1 - File Structure & Metadata
