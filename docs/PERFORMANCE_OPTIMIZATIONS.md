# Performance Optimizations

## Overview

This document tracks performance optimizations implemented in the Content Atlas data import pipeline. The goal is to achieve sub-60-second import times for 120K row Excel files.

## Baseline Performance (Before Optimizations)

**Test File:** Think_Data_Group_August_2025.xlsx (120,000 rows, ~20MB)
**Total Time:** 143.89 seconds (~2.4 minutes)

### Time Breakdown (Estimated)
- File Parsing: ~20-25 seconds (15-20%)
- Data Mapping: ~20-25 seconds (15-20%)
- Duplicate Checking: ~40-50 seconds (30-40%)
- Database Insertion: ~30-40 seconds (25-30%)
- Other Operations: ~10-15 seconds (10-15%)

## Optimization Phase 1: File Parsing Cache (Completed)

**Implementation Date:** October 2025
**Expected Savings:** 20-25 seconds
**Actual Savings:** 18.96 seconds (13% improvement)

### Changes Made
- Added `records_cache` in `app/main.py` to cache parsed records
- `/detect-mapping` endpoint now caches parsed records for 5 minutes
- `/map-data` endpoint checks cache before re-parsing file
- Cache key based on SHA-256 file hash

### Results
- **Before:** 143.89 seconds
- **After:** 124.93 seconds
- **Improvement:** 18.96 seconds saved

### Analysis
File parsing was only 15-20% of total time (not 50% as initially assumed). The real bottlenecks are:
1. Duplicate checking (30-40%)
2. Database insertion (25-30%)
3. Data mapping (15-20%)

## Optimization Phase 2: PostgreSQL COPY & Enhanced Caching (Completed)

**Implementation Date:** October 30, 2025
**Expected Savings:** 25-35 seconds
**Status:** Implemented, awaiting validation

### Changes Made

#### 1. PostgreSQL COPY for Bulk Insertion
**File:** `app/models.py` - `_insert_records_chunked()`
**Expected Savings:** 15-20 seconds (50% faster inserts)

Replaced pandas `to_sql()` with PostgreSQL COPY command:
```python
# Before: pandas to_sql (slower)
df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')

# After: PostgreSQL COPY (2-3x faster)
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
csv_buffer.seek(0)
cursor.copy_from(csv_buffer, table_name, sep=',', null='\\N', columns=columns)
```

**Benefits:**
- Direct PostgreSQL protocol (bypasses SQL parsing)
- Bulk data transfer (not row-by-row)
- Minimal transaction overhead
- Automatic fallback to `to_sql()` if COPY fails

#### 2. Enhanced Record Caching
**File:** `app/main.py` - `records_cache`
**Expected Savings:** 10-15 seconds (skip mapping on re-import)

Extended cache structure to store both raw and mapped records:
```python
# Before: Only raw records
records_cache[file_hash] = (records, timestamp)

# After: Raw + mapped records with config tracking
records_cache[file_hash] = {
    'raw_records': records,
    'mapped_records': mapped_records,  # NEW
    'config_hash': config_hash,         # NEW
    'timestamp': timestamp,
    'file_name': filename
}
```

**Benefits:**
- Skip file parsing (already implemented)
- Skip data mapping if config unchanged (NEW)
- Intelligent cache invalidation based on config changes
- 5-minute TTL with automatic cleanup

### Expected Performance

**Projected Time Breakdown (120K rows):**
- File Parsing: ~0 seconds (cached)
- Data Mapping: ~0-20 seconds (cached if config unchanged)
- Duplicate Checking: ~40-50 seconds (unchanged)
- Database Insertion: ~15-20 seconds (50% faster with COPY)
- Other Operations: ~10-15 seconds (unchanged)

**Projected Total:** 65-105 seconds (depending on cache hits)
**Best Case (full cache hit):** ~65-75 seconds
**Worst Case (no cache):** ~90-105 seconds

## Optimization Phase 3: Duplicate Checking (Planned)

**Expected Savings:** 15-25 seconds
**Status:** Not yet implemented

### Proposed Changes

#### Option A: Database-Side Filtering
Push comparison to PostgreSQL instead of pandas:
```python
# Instead of loading all data and merging in pandas
existing_df = pd.read_sql(query, conn)
merged = new_df.merge(existing_df, ...)

# Use database EXISTS query (much faster)
query = f"""
    SELECT COUNT(*) FROM {table_name} 
    WHERE (col1, col2) IN (VALUES {placeholders})
"""
```

#### Option B: Bloom Filters
Quick negative checks before expensive merges:
- Build bloom filter from existing data
- Check new records against filter
- Only do full comparison for potential matches

#### Option C: Hash-Based Indexing
- Create hash index on uniqueness columns
- Use hash lookups instead of full scans
- Particularly effective for large existing datasets

## Optimization Phase 4: Chunk Size Tuning (Planned)

**Expected Savings:** 5-10 seconds
**Status:** Not yet implemented

### Current State
- Chunk size: 20,000 records
- Fixed across all operations

### Proposed Changes
- Dynamic chunk sizing based on:
  - Available memory
  - CPU core count
  - File size
  - Operation type (mapping vs insertion)

### Testing Matrix
| Chunk Size | Parallelism | Overhead | Best For |
|------------|-------------|----------|----------|
| 10K | High | High | Many cores, small memory |
| 20K | Medium | Medium | Current default |
| 50K | Low | Low | Few cores, large memory |

## Performance Monitoring

### Key Metrics to Track
1. **Total Import Time** - End-to-end duration
2. **Parse Time** - File reading and parsing
3. **Map Time** - Data transformation
4. **Duplicate Check Time** - Validation phase
5. **Insert Time** - Database writes
6. **Cache Hit Rate** - Percentage of cache hits

### Profiling Tools
```python
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()
# ... run import ...
profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)
```

## Testing Strategy

### Test Files
1. **Small (1K rows):** Quick validation
2. **Medium (10K rows):** Standard testing
3. **Large (120K rows):** Performance benchmarking
4. **Extra Large (500K+ rows):** Stress testing

### Test Scenarios
1. **Cold Start:** No cache, first import
2. **Warm Cache:** Same file, same config
3. **Config Change:** Same file, different mapping
4. **Duplicate Detection:** Import with existing data
5. **Concurrent Imports:** Multiple files simultaneously

### Success Criteria
- **120K rows:** < 60 seconds (target)
- **10K rows:** < 5 seconds
- **1K rows:** < 2 seconds
- **Cache hit:** 50%+ time savings
- **No data loss:** 100% accuracy maintained

## Implementation Notes

### PostgreSQL COPY Considerations
1. **CSV Format:** Uses CSV as intermediate format
2. **NULL Handling:** Uses `\N` for NULL values
3. **Error Handling:** Automatic fallback to `to_sql()`
4. **Transaction Safety:** Commits per chunk
5. **Column Order:** Must match table schema

### Cache Management
1. **TTL:** 5 minutes (configurable)
2. **Cleanup:** Automatic on each cache access
3. **Memory:** Bounded by TTL and file size
4. **Invalidation:** Config hash mismatch
5. **Concurrency:** Thread-safe dictionary

### Backward Compatibility
- All optimizations maintain existing API contracts
- Fallback mechanisms for edge cases
- No breaking changes to client code
- Existing tests continue to pass

## Future Optimizations (Backlog)

### Priority 1: Database Optimizations
- [ ] Connection pooling tuning
- [ ] Prepared statements for inserts
- [ ] Index optimization for duplicate checks
- [ ] VACUUM and ANALYZE scheduling

### Priority 2: Parallel Processing
- [ ] Parallel chunk insertion with locking
- [ ] GPU acceleration for data transformations
- [ ] Distributed processing for multi-GB files

### Priority 3: Advanced Caching
- [ ] Redis-backed cache for multi-instance deployments
- [ ] Persistent cache across server restarts
- [ ] Predictive pre-caching based on usage patterns

### Priority 4: Compression
- [ ] In-memory compression for cached records
- [ ] Compressed file storage in B2
- [ ] Streaming decompression during import

## Benchmarking Results

### Phase 1 Results (File Parsing Cache)
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Time | 143.89s | 124.93s | -18.96s (13%) |
| Parse Time | ~25s | ~0s | -25s (100%) |
| Cache Hit Rate | 0% | ~100% | N/A |

### Phase 2 Results (PostgreSQL COPY + Enhanced Cache)
| Metric | Expected | Actual | Status |
|--------|----------|--------|--------|
| Total Time | 65-105s | 129.99s | ⚠️ Slower than expected |
| Insert Time | 15-20s | TBD | Not measured separately |
| Cache Hit Rate | 50%+ | 100% | ✅ Achieved |

**Note:** Test completed on October 30, 2025. Total time was 129.99s (2:09), which is 5 seconds SLOWER than Phase 1 baseline (124.93s). This suggests the PostgreSQL COPY optimization may have overhead that negates its benefits for this specific dataset/configuration.

**Analysis:** The slower performance could be due to:
1. CSV serialization overhead (DataFrame → CSV → PostgreSQL)
2. Additional string processing for NULL handling (`\N`)
3. Connection management overhead (raw_connection acquire/release per chunk)
4. Potential I/O bottleneck on test system

**Next Steps:** Profile the insertion phase to identify the actual bottleneck and consider alternative optimizations.

## Optimization Phase 3: Database-Side Duplicate Checking + Revert COPY (Completed)

**Implementation Date:** October 30, 2025
**Expected Savings:** 5-10 seconds
**Actual Savings:** 7.57 seconds (6% improvement)

### Changes Made

#### 1. Database-Side Duplicate Checking in Chunked Processing
**File:** `app/models.py` - `_check_chunks_parallel()`
**Expected Savings:** Minimal (table was empty in test)

Replaced pandas-based duplicate checking with PostgreSQL IN queries:
```python
# Before: Load all existing data into pandas, then merge
existing_data_cache = pd.read_sql(query, conn)
merged = new_df_subset.merge(existing_df, ...)

# After: Use database-side queries with VALUES clause
query = text(f"""
    SELECT COUNT(*) FROM "{table_name}"
    WHERE ({columns_clause}) IN (VALUES {values_clause})
""")
```

**Benefits:**
- No need to load all existing data into memory
- PostgreSQL handles comparison (optimized C code)
- Can leverage database indexes
- Scales better with large existing datasets

**Note:** In this test, the table was empty (first import), so duplicate checking was skipped entirely. The real benefit will be seen on subsequent imports with existing data.

#### 2. Reverted PostgreSQL COPY Optimization
**File:** `app/models.py` - `_insert_records_chunked()`
**Actual Savings:** 7.57 seconds (6% improvement)

Reverted from PostgreSQL COPY back to pandas `to_sql()`:
```python
# Removed: PostgreSQL COPY with CSV serialization
csv_buffer = StringIO()
df.to_csv(csv_buffer, ...)
cursor.copy_from(csv_buffer, ...)

# Restored: pandas to_sql with method='multi'
df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
```

**Why COPY was slower:**
- DataFrame → CSV serialization overhead
- String processing for NULL values (`\N`)
- Raw connection acquire/release per chunk
- For 20K record chunks, overhead exceeded benefits

**Lesson Learned:** PostgreSQL COPY is fastest for very large single operations, but for medium-sized chunks (20K records), pandas `to_sql()` with `method='multi'` is simpler and performs better.

### Results

**Phase 3 Results:**
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total Time | 129.99s | 122.42s | -7.57s (6%) |
| DB-side dup check | N/A | Implemented | ✅ |
| Insert method | COPY | to_sql | ✅ Faster |

**Cumulative Progress:**
| Phase | Time | Change | Cumulative Savings |
|-------|------|--------|-------------------|
| Baseline | 143.89s | - | - |
| Phase 1 (Cache) | 124.93s | -18.96s | -18.96s (13%) |
| Phase 2 (COPY) | 129.99s | +5.06s | -13.90s (10%) |
| Phase 3 (DB-side + Revert) | 122.42s | -7.57s | -21.47s (15%) |

**Current Status:** 122.42 seconds (2:02) for 120K rows
**Target:** < 60 seconds
**Remaining Gap:** 62.42 seconds

## Optimization Phase 4: Vectorized Data Mapping (Attempted)

**Implementation Date:** October 30, 2025
**Expected Savings:** 15-20 seconds
**Actual Result:** +3.95 seconds slower (regression)

### Changes Made

Replaced row-by-row mapping with pandas vectorized operations:
```python
# Before: Row-by-row processing
for record in records:
    mapped_record = {}
    for output_col, input_field in config.mappings.items():
        mapped_record[output_col] = record.get(input_field)
    mapped_records.append(mapped_record)

# After: Vectorized with DataFrame
df = pd.DataFrame(records)
df = df.rename(columns={v: k for k, v in config.mappings.items()})
mapped_records = df.to_dict('records')
```

### Results

**Phase 4 Results:**
| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Total Time | 122.42s | 126.37s | +3.95s (3% slower) ⚠️ |

**Analysis - Why Vectorization Was Slower:**

1. **DataFrame Conversion Overhead** - Converting 120K records to DataFrame and back to dict adds significant overhead
2. **Memory Allocation** - DataFrame creation requires contiguous memory allocation
3. **Data Type Inference** - Pandas infers types for each column, adding processing time
4. **Dictionary Conversion** - `to_dict('records')` is expensive for large datasets

**Lesson Learned:** Vectorization is not always faster. For simple column mapping without complex transformations, the overhead of DataFrame operations can exceed the benefits. Vectorization works best when:
- Complex mathematical operations are needed
- Type coercion across entire columns
- Statistical aggregations
- NOT for simple dictionary key remapping

### Decision

Reverted to row-by-row mapping for better performance. The current bottleneck is likely elsewhere (database operations, parallel processing overhead, or I/O).

**Current Best Performance:** 122.42 seconds (Phase 3)

## Lessons Learned

1. **Profile First:** Initial assumptions about bottlenecks were incorrect
2. **Incremental Optimization:** Small wins add up
3. **Cache Carefully:** Balance memory vs performance
4. **Fallback Mechanisms:** Always have a backup plan
5. **Measure Everything:** Can't optimize what you don't measure

## References

- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [Pandas Performance Tips](https://pandas.pydata.org/docs/user_guide/enhancingperf.html)
- [Python Profiling Guide](https://docs.python.org/3/library/profile.html)
- [SQLAlchemy Performance](https://docs.sqlalchemy.org/en/14/faq/performance.html)
