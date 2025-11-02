# Implementation Plan Review

**Date:** 2025-11-02
**Purpose:** Detailed review of all phases and milestones for discussion and refinement

---

## Overview: 5-Phase Plan (REVISED)

| Phase | Duration | Cumulative | Key Outcome |
|-------|----------|------------|-------------|
| **Phase 1** | **10 weeks** | **10 weeks** | **Read real-world files (flat schema, PLAIN + DICT, optional columns)** |
| **Phase 2** | 6-8 weeks | 16-18 weeks | Read complex files (nested types, delta encodings) |
| **Phase 3** | 8 weeks | 24-26 weeks | Write files (compatible with other tools) |
| **Phase 4** | 6 weeks | 30-32 weeks | Production features (bloom filters, async, perf) |
| **Phase 5** | 6 weeks | 36-38 weeks | Encryption (optional) |

**Total:** 30-32 weeks (~7-8 months) for Phases 1-4

**Note:** Phase 1 revised from 6 weeks to 10 weeks to include dictionary encoding and optional column support, delivering a practical reader instead of a minimal POC.

---

## Phase 1: Foundation (Practical Reader) - 10 weeks

### Overview
Build a practical reader that can handle real-world Parquet files (flat schema only; nested types deferred to Phase 2).

### Revised Success Criteria ✨
- Read flat schema (no nested types yet)
- **PLAIN + DICTIONARY encoding** (dictionary is very common)
- **Optional columns** (basic null handling via definition levels)
- Required columns
- GZIP compression (Snappy if integration goes smoothly)
- Pass 10-15 parquet-testing files including:
  - `alltypes_plain.parquet` (all types, PLAIN)
  - `alltypes_dictionary.parquet` (dictionary encoding)
  - Files with optional columns

### Scope Changes from Original Plan
**Added:**
- ✅ Dictionary encoding (critical for real files)
- ✅ Definition levels for optional columns (nulls)
- ✅ **Minimal RLE decoder** (~200-300 lines)
  - **Scope:** Dictionary index packs + definition levels only
  - **NOT included:** Repetition levels, full RLE for boolean columns
  - Hybrid RLE/bit-packing for int32 values (indices and levels)
  - Sufficient for dict-encoded columns and max_def_level=1

**Deferred to Phase 2:**
- Repetition levels (nested types)
- Delta encodings (DELTA_BINARY_PACKED, etc.)
- Full RLE implementation (for boolean columns, complex level schemes)

### Milestone Breakdown

#### ✅ M1.0: Planning & Analysis (COMPLETED)
- C++ analysis, roadmap, package design
- **Status:** Done

#### M1.1: Project Setup (~1 day)
- Package.swift
- Directory structure
- LICENSE, README
- **Risk:** Low
- **Blockers:** None

#### M1.2: Core Type System (~2-3 days)
- Physical types (enum)
- Logical types (protocol hierarchy)
- Repetition, Encoding, Compression enums
- **Risk:** Low
- **Dependencies:** None
- **Note:** Pure Swift, no external deps

#### M1.3: Thrift Integration (~7-9 days) ⚠️ CRITICAL DECISION
- **Options:**
  1. Use existing Swift Thrift library
  2. Manual implementation of Parquet subset
  3. Code generation from parquet.thrift

- **Recommendation:** Manual implementation for Parquet subset
  - Avoids dependency complexity
  - Only need subset of Thrift spec

- **Required Thrift Structs (enumerated):**
  1. `FileMetaData` - Top-level file metadata
  2. `SchemaElement` - Schema node definition
  3. `RowGroup` - Row group metadata
  4. `ColumnChunk` - Column chunk info
  5. `ColumnMetaData` - Column metadata details
  6. `PageHeader` - Page header (union type)
  7. `DataPageHeader` - Data page details
  8. `DataPageHeaderV2` - V2 page details
  9. `DictionaryPageHeader` - Dictionary page
  10. `Statistics` - Column statistics (optional)
  11. `Encoding` - Encoding enum
  12. `CompressionCodec` - Compression enum
  13. `Type` - Physical type enum
  14. `FieldRepetitionType` - Repetition enum
  15. `ConvertedType` / `LogicalType` - Logical types

- **Implementation strategy:**
  - Compact Binary Protocol only (most common)
  - Read-only for Phase 1 (deserialization)
  - ~800-1000 lines of Swift for basic Thrift support

- **Risk:** Medium-High (Thrift is fiddly)
- **Blockers:** None (can start immediately after M1.2)
- **Time estimate:** 2 days design + 5 days implementation + 2 days testing
- **Buffer:** +2 days for edge cases

#### M1.4: Schema Representation (~3-4 days)
- Node protocol, PrimitiveNode, GroupNode
- SchemaDescriptor (flatten schema tree)
- **Simple level calculation only** (required columns, max_def=0)
- **Risk:** Medium (level calculation is tricky)
- **Dependencies:** Types (M1.2), Thrift (M1.3)

#### M1.5: Basic I/O Layer (~2-3 days)
- RandomAccessFile protocol
- FileHandle-based implementation
- BufferedReader (simple buffering)
- **Risk:** Low
- **Dependencies:** None
- **Note:** Keep simple, don't over-engineer

#### M1.6: Metadata Parsing (~3-4 days)
- Read footer (last 8 bytes = footer length)
- Validate magic bytes "PAR1"
- Deserialize FileMetaData (Thrift)
- Wrapper classes
- **Risk:** Medium (depends on Thrift)
- **Dependencies:** Thrift (M1.3), I/O (M1.5)

#### M1.7: PLAIN Encoding (~3-4 days)
- Decoder protocol
- PlainDecoder for each physical type
- Endianness handling (little-endian)
- **Risk:** Low
- **Dependencies:** Types (M1.2)
- **Note:** Most straightforward encoding

#### M1.8: Compression Support (~3-4 days)
- **Codec protocol**
- **GZIP implementation** (via Foundation's `Compression` framework)
  - Built-in, no dependencies
  - 1 day

- **Snappy integration** (optional but recommended)
  - **Option A:** Use Swift package (e.g., `https://github.com/michaelnisi/SwiftSnappy`)
  - **Option B:** System library via SPM system target
    ```swift
    .systemLibrary(
        name: "CSnappy",
        pkgConfig: "snappy",
        providers: [.brew(["snappy"]), .apt(["libsnappy-dev"])]
    )
    ```
  - **Fallback:** Skip Snappy in Phase 1 if integration is problematic
  - 2 days (if it goes well)

- **Implementation:**
  ```swift
  public protocol Codec {
      func decompress(_ data: Data, uncompressedSize: Int) throws -> Data
  }
  ```

- **Risk:** Medium (Snappy C library integration can be tricky)
- **Dependencies:** None
- **Time estimate:** 1 day GZIP + 2 days Snappy (or skip)
- **Deliverable:** GZIP works; Snappy is best-effort

#### M1.9: Column Reader - SPLIT INTO SUB-MILESTONES ⚠️

This is the most complex milestone. Breaking it down:

##### M1.9a: Page Reading & Decompression (~3-4 days)
- **PageReader implementation**
  - Read page headers (Thrift deserialization)
  - Read page data (raw bytes)
  - Validate page types
- **Decompression**
  - Decompress pages using Codec
  - Handle uncompressed pages
- **Testing:**
  - Unit test: read compressed pages
  - Verify page header parsing

- **Risk:** Medium
- **Dependencies:** Thrift (M1.3), I/O (M1.5), Compression (M1.8)
- **Deliverable:** Can read and decompress pages
- **Buffer:** +1 day

##### M1.9b: PLAIN ColumnReader (~2-3 days)
- **ColumnReader for PLAIN encoding**
  - Required columns only (no nulls yet)
  - Batch reading API
  - Value decoding (uses PlainDecoder from M1.7)
- **Testing:**
  - Read int32, int64, double columns
  - Verify correct values

- **Risk:** Medium
- **Dependencies:** M1.9a, PLAIN decoder (M1.7)
- **Deliverable:** Can read PLAIN-encoded required columns
- **Buffer:** +1 day

##### M1.9c: Dictionary ColumnReader (~3-4 days)
- **Minimal RLE decoder implementation** (~200-300 lines)
  - **Scope:** Hybrid RLE/bit-packing for int32 values
  - **Use cases:**
    - Dictionary index packs (data page indices)
    - Definition levels (used in M1.9d)
  - **NOT implemented yet:**
    - Repetition levels (Phase 2)
    - Full RLE for boolean columns (Phase 2)
    - Complex level schemes (Phase 2)
  - Implementation:
    - RLE runs (repeated values)
    - Bit-packed runs (sequences)
    - Width calculation

- **Dictionary page handling**
  - Read dictionary page
  - Decode dictionary values (PLAIN)
  - Store dictionary in memory (hash map or array)

- **Dictionary ColumnReader**
  - Read data page indices (using RLE decoder)
  - Look up values in dictionary
  - Return decoded values

- **Testing:**
  - Read dictionary-encoded int32, string columns
  - Verify values match expected
  - Test with various dictionary sizes

- **Risk:** High (RLE bit manipulation is complex)
- **Dependencies:** M1.9b (PLAIN reader)
- **Deliverable:** Can read DICT-encoded columns
- **Buffer:** +2 days (RLE is tricky, expect edge cases)

##### M1.9d: Optional Column Support (~2-3 days)
- **Definition level handling**
  - Decode definition levels (RLE from M1.9c)
  - Map levels to null/non-null
  - For max_def_level=1 only (optional, not nested)
- **ColumnReader enhancement**
  - Return values + validity bitmap
  - Or return `[T?]` array
- **Testing:**
  - Read optional int32 columns with nulls
  - Verify null positions correct

- **Risk:** Medium
- **Dependencies:** M1.9c (RLE)
- **Deliverable:** Can read optional columns
- **Buffer:** +1 day

**Total M1.9:** 10-14 days + 5 days buffer = **12-16 days actual**

#### M1.10: File Reader API (~2-3 days)
- ParquetFileReader (public API)
- RowGroupReader
- Clean error handling
- Documentation
- **Risk:** Low
- **Dependencies:** Column reader (M1.9)

### Phase 1 Revised Time Estimate

**Milestone breakdown with buffers:**
- M1.0: Planning (done)
- M1.1: Project setup (1 day)
- M1.2: Core types (3 days)
- M1.3: Thrift (9 days with buffer)
- M1.4: Schema (4 days)
- M1.5: I/O layer (3 days)
- M1.6: Metadata (4 days)
- M1.7: PLAIN encoding (4 days)
- M1.8: Compression (4 days)
- M1.9: Column reader (16 days with buffers)
  - M1.9a: Page reading (4 days)
  - M1.9b: PLAIN reader (3 days)
  - M1.9c: Dict reader (6 days)
  - M1.9d: Optional columns (3 days)
- M1.10: File reader API (3 days)

**Total:** ~51 days = **~10-11 weeks** (accounting for buffers)

**Realistic timeline:**
- **Optimistic:** 8 weeks (if RLE and Thrift go smoothly)
- **Expected:** 10 weeks
- **Conservative:** 12 weeks (includes learning curve)

**Risk assessment:**
- High-risk items now have explicit buffers
- Dictionary + optional columns add 3-4 weeks vs. original plan
- But delivers a **practical** reader that handles real files

### Parallelization Opportunities 🔀

Even as a solo developer, some work can overlap:

**Week 1-2 (M1.1-M1.3):**
- While implementing Thrift, set up test harnesses in parallel
- Prepare parquet-testing files and integration test structure
- Document Thrift structures as you implement them

**Week 3-4 (M1.4-M1.6):**
- Schema and Metadata can be developed iteratively
- Write unit tests while coding (TDD approach)
- Set up CI/GitHub Actions in background

**Week 5-6 (M1.7-M1.8):**
- PLAIN encoding and Compression are independent
- Can work on one, test the other
- Start documenting API while implementing

**Week 7-10 (M1.9):**
- Test scaffolding while implementing page readers
- Write examples and documentation during integration
- Performance profiling setup during testing phase

**Continuous parallel work:**
- Documentation (docstrings, guides)
- Test data generation (PyArrow scripts)
- Performance benchmarks setup
- GitHub issues/project tracking

### Phase 1 Questions to Discuss

1. **Revised scope acceptable?**
   - Dictionary + optional columns added
   - 10 weeks instead of 6 weeks
   - Delivers practical reader

2. **Thrift Strategy:**
   - Manual implementation (recommended)
   - Enumerated 15 required structs
   - ~800-1000 lines, 9 days estimate

3. **Compression:**
   - GZIP required (built-in)
   - Snappy best-effort
   - Specific package suggested: SwiftSnappy

4. **RLE Scope:**
   - Minimal implementation for dict indices + levels
   - Full RLE deferred to Phase 2
   - ~200-300 lines for Phase 1

5. **Testing targets:**
   - Pass 10-15 parquet-testing files
   - Include dict-encoded and optional columns
   - Reasonable for 10-week phase?

---

## Phase 2: Full Reader Support - 6-8 weeks

### Overview
Add nested types and remaining encodings for complete reader functionality.

### Success Criteria
- **Nested types** (lists, maps, structs) - the main focus
- Delta encodings
- Full RLE for booleans
- Pass 50+ parquet-testing files including nested schemas

### What's Already Done in Phase 1
- ✅ Dictionary encoding
- ✅ Optional columns (definition levels, max_def_level=1)
- ✅ Minimal RLE (for dict indices and definition levels)

### Milestone Breakdown

#### M2.1: Repetition Levels & Nested Types Foundation (~7-10 days) ⚠️ CRITICAL
- **Repetition level handling**
  - RLE decoding for repetition levels (reuse Phase 1 RLE)
  - Map repetition levels to list boundaries
  - Understand when to start/end nested structures
- **RecordReader architecture** (from C++)
  - Port the pattern from `column_reader.cc:L1800+`
  - Separate level decoding from value decoding
  - Buffer management for nested structures
- **Testing:**
  - Simple repeated fields (lists of primitives)
  - Verify correct list boundaries

- **Risk:** Very High
- **Dependencies:** Phase 1 RLE decoder
- **Note:** This is the hardest part of Parquet

#### M2.2: Full RLE for Booleans (~3-4 days)
- Extend minimal RLE to handle boolean columns
- Bit-level RLE (not just int32 values)
- **Risk:** Medium
- **Dependencies:** Phase 1 minimal RLE
- **Note:** Phase 1 RLE handles int32; this extends to bools

#### M2.3: Delta Encodings (~5-7 days)
- DELTA_BINARY_PACKED
- DELTA_LENGTH_BYTE_ARRAY
- DELTA_BYTE_ARRAY
- **Risk:** Medium-High
- **Note:** Can defer to Phase 3 if time-constrained (less common)

#### M2.4: Nested Type Support (~10-14 days) ⚠️ CRITICAL
- Full definition/repetition level handling
- RecordReader architecture (from C++)
- Structs, lists, maps
- **Risk:** Very High
- **Dependencies:** RLE (M2.2) for level encoding
- **Note:** This is the hardest part of Parquet

#### M2.5: Statistics (~3-4 days)
- Parse statistics from metadata
- Min/max/null count
- **Risk:** Low
- **Note:** Read-only, straightforward

#### M2.6: Comprehensive Testing (~5-7 days)
- Integration tests
- Cross-compatibility verification
- Performance benchmarks
- **Risk:** Medium
- **Note:** Will uncover bugs from all previous milestones

### Phase 2 Time Estimate
- **Expected:** 8-10 weeks
- **Risk:** Nested types (M2.4) could take longer

### Phase 2 Questions to Discuss

1. **Milestone Order:**
   - Should RLE (M2.2) come before Dictionary (M2.1)?
   - RLE is needed for dict indices

2. **Delta Encodings:**
   - Are these essential for Phase 2?
   - Usage stats: DELTA is less common than PLAIN/DICT
   - Could defer to Phase 3?

3. **Nested Types:**
   - This is a 2-week milestone - is that realistic?
   - Should we break it into sub-milestones?

4. **Early Feedback:**
   - Should we release Phase 1 as "alpha" to get user feedback?
   - Before investing 8 weeks in Phase 2?

---

## Phase 3: Writer Support - 8 weeks

### Overview
Enable file writing with feature parity to reader.

### Success Criteria
- Write files readable by PyArrow, DuckDB, Spark
- All encodings supported
- Statistics generation
- Compression

### Milestone Breakdown

#### M3.1: Writer Foundation (~5-7 days)
- Encoder protocol
- PlainEncoder
- PageWriter
- **Risk:** Medium
- **Note:** Mirror of Phase 1 reader work

#### M3.2: All Encodings (~10-12 days)
- DictionaryEncoder
- RLE encoder
- Delta encoders (if implemented in Phase 2)
- **Risk:** High
- **Note:** Encoding is harder than decoding (need to choose encoding)

#### M3.3: Column Writer (~7-10 days)
- ColumnWriter
- Batching
- Statistics generation
- Page management
- **Risk:** High

#### M3.4: File Writer API (~3-5 days)
- ParquetFileWriter
- RowGroupWriter
- Metadata serialization (Thrift)
- **Risk:** Medium

#### M3.5: Nested Type Writing (~10-14 days)
- Level encoding
- Complex type writing
- **Risk:** Very High

#### M3.6: Cross-Compatibility Testing (~5-7 days)
- Verify files with other tools
- Fix compatibility issues
- **Risk:** Medium

### Phase 3 Time Estimate
- **Expected:** 8-10 weeks

### Phase 3 Questions to Discuss

1. **Do we need Phase 3 immediately?**
   - Many use cases only need reading
   - Could defer to Phase 4 and do advanced features first?

2. **Encoding Selection Logic:**
   - How to auto-select best encoding?
   - Or let user specify?

3. **Statistics:**
   - Always generate?
   - Make optional?

---

## Phase 4: Advanced Features - 6 weeks

### Overview
Production-ready features and optimizations.

### Milestone Breakdown

#### M4.1: Bloom Filters (~7-10 days)
- Read and write bloom filters
- Split-block algorithm
- **Risk:** Medium
- **Note:** Useful for queries, not essential

#### M4.2: Page Index (~5-7 days)
- Column index
- Offset index
- Predicate pushdown
- **Risk:** Medium

#### M4.3: Streaming APIs (~5-7 days)
- StreamReader
- StreamWriter
- Row-oriented API
- **Risk:** Medium

#### M4.4: Async I/O (~7-10 days)
- Swift async/await
- Concurrent row group reading
- **Risk:** Medium

#### M4.5: Performance Optimization (~7-10 days)
- Profile and optimize
- SIMD where possible
- Memory efficiency
- **Risk:** Medium

### Phase 4 Time Estimate
- **Expected:** 6-8 weeks

### Phase 4 Questions to Discuss

1. **Priority Order:**
   - What's most valuable first?
   - Streaming? Async? Performance?

2. **SIMD:**
   - Worth the complexity in Swift?
   - Or wait for Swift SIMD to mature?

3. **Can Phase 4 be done incrementally?**
   - Release features as they're ready?
   - Rather than all at once?

---

## Phase 5: Encryption (Optional) - 6 weeks

### Overview
Parquet encryption support (AES-GCM).

### Questions
1. **Is this needed?**
   - Most users don't use encryption
   - Could be community contribution later?

2. **Defer indefinitely?**
   - Focus on core features first

---

## Overall Plan Review: Key Questions

### 1. Phase Ordering
**Current:** 1 → 2 → 3 → 4 → 5

**Alternative A: MVP First**
- 1 (Minimal Reader) → 4 (Streaming, Async) → 2 (Full Reader) → 3 (Writer)
- Rationale: Get usable product faster, defer complexity

**Alternative B: Reader-First**
- 1 (Minimal Reader) → 2 (Full Reader) → 4 (Advanced Read) → 3 (Writer)
- Rationale: Complete reading before writing, get feedback

**Question:** Which ordering makes most sense?

### 2. Scope of Phase 1
**Current:** Flat schema, PLAIN encoding, required columns

**Alternative: Broader Phase 1**
- Include dictionary encoding
- Include optional columns (basic nulls)
- Takes 8 weeks instead of 6

**Question:** Is Phase 1 too minimal? Should we broaden it?

### 3. Critical Path Items

**High-Risk Milestones:**
- M1.3: Thrift integration (5-7 days, medium risk)
- M1.9: Column reader (5-7 days, high risk)
- M2.2: RLE encoding (7-10 days, high risk)
- M2.4: Nested types (10-14 days, very high risk)
- M3.2: Encoder implementation (10-12 days, high risk)
- M3.5: Nested type writing (10-14 days, very high risk)

**Question:** Should we add buffer time for these?

### 4. Dependency Chain

**Critical dependency chain:**
1. Types (M1.2)
2. Thrift (M1.3)
3. Schema (M1.4) [depends on Types, Thrift]
4. Metadata (M1.6) [depends on Thrift, I/O]
5. PLAIN decoder (M1.7) [depends on Types]
6. Column reader (M1.9) [depends on ALL above]

**Question:** Any parallelization opportunities?

### 5. Testing Strategy

**Current plan:**
- Unit tests per milestone
- Integration tests at phase end
- parquet-testing files throughout

**Question:** Should we add:
- Fuzzing (malformed files)?
- Property-based testing?
- Continuous benchmarking?

### 6. Release Strategy

**Option A: Big Bang**
- Release 1.0 after Phase 3 (reading + writing)

**Option B: Incremental**
- 0.1 after Phase 1 (minimal reader)
- 0.5 after Phase 2 (full reader)
- 1.0 after Phase 3 (+ writer)

**Option C: Alpha/Beta**
- Alpha after Phase 1
- Beta after Phase 2
- 1.0 after Phase 3

**Question:** Which release strategy?

### 7. Staffing / Community

**Current assumption:** Single developer (you)

**Questions:**
- Open source from day 1?
- Accept contributions during development?
- Or wait until Phase 1/2 complete?

---

## ✅ REVISED Plan - Key Changes

Based on feedback, here are the finalized adjustments:

### ✅ Change 1: Expanded Phase 1 Scope
**Added:**
- Dictionary encoding (critical for real files)
- Optional column support (null handling)
- Minimal RLE decoder (for dict indices + definition levels)

**Result:**
- Phase 1: 10 weeks (was 6)
- Delivers **practical** reader, not toy example
- Can read 80%+ of real Parquet files

### ✅ Change 2: Split Column Reader (M1.9)
**Broken into 4 sub-milestones:**
- M1.9a: Page reading + decompression (4 days)
- M1.9b: PLAIN column reader (3 days)
- M1.9c: Dictionary column reader (6 days)
- M1.9d: Optional column support (3 days)

**Result:**
- Clear checkpoints every 3-6 days
- Can validate incrementally
- Reduces risk of "lost 2 weeks in column reader"

### ✅ Change 3: Explicit Thrift Implementation
**Enumerated 15 required structs:**
- FileMetaData, SchemaElement, RowGroup, etc.
- Manual implementation, no dependencies
- ~800-1000 lines of Swift

**Result:**
- No dependency hunting
- Controlled complexity
- 9 days with buffer

### ✅ Change 4: Compression Strategy
**GZIP:** Required (Foundation built-in)
**Snappy:** Best-effort with specific package (`SwiftSnappy`)
**Fallback:** Phase 1 can launch with GZIP-only

**Result:**
- No blocker if Snappy is problematic
- Clear integration options documented

### ✅ Change 5: Added Risk Buffers
**High-risk milestones now have explicit buffers:**
- Thrift: +2 days
- M1.9a-d: +5 days total
- Total Phase 1: includes ~15% buffer time

**Result:**
- Realistic estimates
- Accounts for learning curve
- Less likely to slip

### ✅ Change 6: Reorder Phase 2 (Future)
**Current:** Dict → RLE → Delta → Nested → Stats

**Proposed:** Nested → Delta → Stats → Full RLE

**Rationale:**
- Dictionary + minimal RLE already done in Phase 1
- Focus Phase 2 on repetition levels (the hard part)
- Delta encodings are rare, can be last

---

## Summary: Revised Phase 1 Plan

### Phase 1: Practical Reader - 10 weeks

**Scope:**
- ✅ Flat schema (no nested types yet)
- ✅ PLAIN + DICTIONARY encoding
- ✅ Required + Optional columns (nulls via definition levels)
- ✅ GZIP compression (Snappy best-effort)
- ✅ Pass 10-15 parquet-testing files

**Milestones (10 weeks total):**
1. M1.1: Project setup (1 day)
2. M1.2: Core types (3 days)
3. M1.3: Thrift integration (9 days)
4. M1.4: Schema (4 days)
5. M1.5: I/O layer (3 days)
6. M1.6: Metadata parsing (4 days)
7. M1.7: PLAIN encoding (4 days)
8. M1.8: Compression (4 days)
9. M1.9: Column reader (16 days)
   - M1.9a: Page reading (4d)
   - M1.9b: PLAIN reader (3d)
   - M1.9c: Dict reader (6d)
   - M1.9d: Optional columns (3d)
10. M1.10: File reader API (3 days)

**Key Improvements:**
- ✅ Delivers practical reader, not toy
- ✅ Dictionary encoding included
- ✅ Null handling included
- ✅ Column reader split into 4 checkpoints
- ✅ Risk buffers added
- ✅ Thrift structs enumerated
- ✅ Compression fallback plan

**Success Metrics:**
- Can read 80%+ of real Parquet files (flat schema)
- Pass alltypes_plain.parquet and alltypes_dictionary.parquet
- Handle files with optional columns
- API is clean and documented

---

## Next Steps

**If plan approved:**
1. Start M1.1: Create Package.swift and directory structure
2. Set up CI/GitHub Actions
3. Begin M1.2: Implement core types
4. Prepare test harness for parquet-testing files

**If adjustments needed:**
- Discuss specific milestones
- Refine time estimates
- Adjust scope

---

**Ready to proceed with revised Phase 1?** 🚀
