# Next Performance Optimizations - Action Plan

## Current State Analysis (Updated October 30, 2025)

**Current Performance:** 122.42 seconds for 120K rows
**Target:** < 60 seconds (need to save ~62 seconds)

### Time Breakdown (Estimated from 122s total)
1. **Data Mapping:** ~40-50 seconds (33-41%) ⚠️ PRIMARY BOTTLENECK
2. **Database Insertion:** ~30-35 seconds (25-29%)
3. **Duplicate Checking:** ~0 seconds (table empty, skipped) ✅
4. **File Parsing:** ~0 seconds (cached) ✅
5. **Other Operations:** ~10-15 seconds (8-12%)

### Key Findings from Phase 3

1. **Database-side duplicate checking works well** - When table has existing data, it will be much faster than pandas merges
2. **PostgreSQL COPY added overhead** - For 20K chunks, pandas `to_sql()` is actually faster
3. **Mapping is now the bottleneck** - With parsing cached and insertion optimized, mapping takes 33-41% of time
4. **Need to profile mapping phase** - Row-by-row processing may be the issue

## Priority 1: Optimize Duplicate Checking (HIGH IMPACT)

**Expected Savings:** 30-40 seconds (60-80% reduction)
**Complexity:** Medium
**Risk:** Low

### Current Implementation Issues

The current duplicate checking:
1. Loads ALL existing data into pandas DataFrame
2. Performs expensive pandas merge operations
3. Does this for EVERY chunk (7 chunks for 120K rows)
4. No indexing or optimization

### Recommended Approach: Database-Side Duplicate Detection

Replace pandas merge with PostgreSQL queries:

```python
def _check_duplicates_db_side(
    engine: Engine,
    table_name: str,
    chunk_records: List[Dict[str, Any]],
    uniqueness_columns: List[str]
) -> int:
    """
    Check for duplicates using database-side queries.
    Much faster than loading all data into pandas.
    """
    if not chunk_records:
        return 0
    
    # Build VALUES clause for batch checking
    # Example: WHERE (col1, col2) IN (VALUES (val1, val2), (val3, val4), ...)
    
    with engine.connect() as conn:
        # Check if table exists and has data
        count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
        if count_result.scalar() == 0:
            return 0
        
        # Build batch query (check 1000 records at a time to avoid query size limits)
        batch_size = 1000
        total_duplicates = 0
        
        for i in range(0, len(chunk_records), batch_size):
            batch = chunk_records[i:i+batch_size]
            
            # Build VALUES clause
            values_list = []
            params = {}
            for idx, record in enumerate(batch):
                value_placeholders = []
                for col in uniqueness_columns:
                    param_name = f"p{i}_{idx}_{col}"
                    value_placeholders.append(f":{param_name}")
                    params[param_name] = record.get(col)
                values_list.append(f"({','.join(value_placeholders)})")
            
            values_clause = ','.join(values_list)
            columns_clause = ','.join([f'"{col}"' for col in uniqueness_columns])
            
            # Query to count duplicates
            query = text(f"""
                SELECT COUNT(*) FROM "{table_name}"
                WHERE ({columns_clause}) IN (VALUES {values_clause})
            """)
            
            result = conn.execute(query, params)
            total_duplicates += result.scalar()
        
        return total_duplicates
```

**Benefits:**
- No need to load all existing data into memory
- PostgreSQL handles the comparison (optimized C code)
- Can leverage database indexes
- Scales better with large existing datasets

**Implementation Steps:**
1. Create new function `_check_duplicates_db_side()` in `app/models.py`
2. Add configuration flag to choose between pandas and DB-side checking
3. Update `_check_chunks_parallel()` to use new method
4. Add indexes on uniqueness columns for better performance

### Alternative: Hybrid Approach with Bloom Filters

For even better performance, combine bloom filters with DB queries:

```python
def _check_duplicates_hybrid(
    engine: Engine,
    table_name: str,
    chunk_records: List[Dict[str, Any]],
    uniqueness_columns: List[str]
) -> int:
    """
    Use bloom filter for quick negative checks, then DB query for positives.
    """
    # Build bloom filter from existing data (one-time cost)
    bloom = build_bloom_filter(engine, table_name, uniqueness_columns)
    
    # Quick filter: eliminate records that definitely don't exist
    potential_duplicates = [
        record for record in chunk_records
        if bloom.might_contain(record)  # Fast O(1) check
    ]
    
    # Only check potential duplicates with DB query
    if potential_duplicates:
        return _check_duplicates_db_side(
            engine, table_name, potential_duplicates, uniqueness_columns
        )
    
    return 0
```

## Priority 2: Optimize Database Insertion (MEDIUM IMPACT)

**Expected Savings:** 10-15 seconds (30-40% reduction)
**Complexity:** Low-Medium
**Risk:** Low

### Issue with Current PostgreSQL COPY

The COPY implementation is slower due to:
1. DataFrame → CSV serialization overhead
2. String processing for NULL values
3. Connection management per chunk

### Recommended Approach: Direct psycopg2 executemany()

```python
def _insert_chunk_executemany(
    engine: Engine,
    table_name: str,
    chunk_records: List[Dict[str, Any]],
    columns: List[str]
) -> int:
    """
    Use psycopg2's executemany for fast batch inserts.
    Faster than COPY for medium-sized batches.
    """
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        try:
            # Build INSERT statement
            columns_sql = ','.join([f'"{col}"' for col in columns])
            placeholders = ','.join(['%s'] * len(columns))
            insert_sql = f'INSERT INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'
            
            # Prepare data as tuples
            data_tuples = [
                tuple(record.get(col) for col in columns)
                for record in chunk_records
            ]
            
            # Execute batch insert
            cursor.executemany(insert_sql, data_tuples)
            raw_conn.commit()
            
            return len(chunk_records)
        finally:
            cursor.close()
    finally:
        raw_conn.close()
```

**Benefits:**
- No CSV serialization overhead
- Direct binary protocol
- Optimized for batch inserts
- Simpler code

### Alternative: Connection Pooling Optimization

Current connection management creates overhead. Optimize by:

```python
# In app/database.py
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,          # Increase from default 5
    max_overflow=40,       # Increase from default 10
    pool_pre_ping=True,    # Verify connections before use
    pool_recycle=3600,     # Recycle connections after 1 hour
    echo_pool=True         # Log pool events for debugging
)
```

## Priority 3: Optimize Data Mapping (LOW-MEDIUM IMPACT)

**Expected Savings:** 5-10 seconds (25-40% reduction)
**Complexity:** Medium
**Risk:** Low

### Current Issues

1. Sequential type coercion for each record
2. No vectorization
3. Repeated type checking

### Recommended Approach: Vectorized Operations

```python
def map_data_vectorized(
    records: List[Dict[str, Any]], 
    config: MappingConfig
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Use pandas vectorized operations for faster mapping.
    """
    import pandas as pd
    
    # Convert to DataFrame for vectorized operations
    df = pd.DataFrame(records)
    
    # Apply mappings (column renaming)
    df = df.rename(columns={v: k for k, v in config.mappings.items()})
    
    # Apply type coercions vectorized
    for col_name, sql_type in config.db_schema.items():
        if col_name in df.columns:
            df[col_name] = coerce_column_vectorized(df[col_name], sql_type)
    
    # Apply transformation rules vectorized
    if config.rules:
        df, errors = apply_rules_vectorized(df, config.rules)
    
    # Convert back to records
    mapped_records = df.to_dict('records')
    
    return mapped_records, errors


def coerce_column_vectorized(series: pd.Series, sql_type: str) -> pd.Series:
    """Vectorized type coercion - much faster than row-by-row."""
    sql_type_upper = sql_type.upper()
    
    if 'INTEGER' in sql_type_upper:
        return pd.to_numeric(series, errors='coerce').astype('Int64')
    elif 'DECIMAL' in sql_type_upper:
        return pd.to_numeric(series, errors='coerce')
    elif 'TIMESTAMP' in sql_type_upper:
        return pd.to_datetime(series, errors='coerce')
    else:
        return series.astype(str)
```

## Priority 4: Disable Duplicate Checking for Initial Import (QUICK WIN)

**Expected Savings:** 50-60 seconds (100% of duplicate check time)
**Complexity:** Very Low
**Risk:** None (user choice)

### Recommendation

Add a flag to skip duplicate checking on first import:

```python
class DuplicateCheckConfig(BaseModel):
    enabled: bool = True
    check_file_level: bool = True
    allow_duplicates: bool = False
    skip_on_empty_table: bool = True  # NEW: Skip if table is empty
    uniqueness_columns: Optional[List[str]] = None
```

**Logic:**
```python
# In _insert_records_chunked()
if config.duplicate_check.skip_on_empty_table:
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
        if count == 0:
            logger.info("Table is empty, skipping duplicate check")
            # Skip duplicate checking entirely
```

## Priority 5: Parallel Chunk Insertion (MEDIUM IMPACT, HIGH RISK)

**Expected Savings:** 10-15 seconds (30-40% reduction in insert time)
**Complexity:** High
**Risk:** Medium (race conditions, data integrity)

### Approach: Advisory Locks

```python
def _insert_chunks_parallel(
    engine: Engine,
    table_name: str,
    chunks: List[List[Dict[str, Any]]],
    max_workers: int = 4
) -> int:
    """
    Insert chunks in parallel using PostgreSQL advisory locks.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def insert_chunk_with_lock(chunk_num, chunk_records):
        raw_conn = engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            try:
                # Acquire advisory lock for this chunk
                lock_id = hash(f"{table_name}_{chunk_num}") % (2**31)
                cursor.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
                
                # Insert chunk
                # ... insertion code ...
                
                raw_conn.commit()
                
                # Release lock
                cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                
                return len(chunk_records)
            finally:
                cursor.close()
        finally:
            raw_conn.close()
    
    total_inserted = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(insert_chunk_with_lock, i, chunk): i
            for i, chunk in enumerate(chunks)
        }
        
        for future in as_completed(futures):
            total_inserted += future.result()
    
    return total_inserted
```

## Implementation Roadmap

### Phase 3A: Database-Side Duplicate Checking (Week 1)
- [ ] Implement `_check_duplicates_db_side()`
- [ ] Add configuration flag for method selection
- [ ] Create indexes on uniqueness columns
- [ ] Test with 120K row file
- [ ] Measure performance improvement
- **Expected Result:** 80-90 seconds total (40s savings)

### Phase 3B: Optimize Database Insertion (Week 1-2)
- [ ] Implement `_insert_chunk_executemany()`
- [ ] Optimize connection pooling settings
- [ ] A/B test against COPY method
- [ ] Choose best performer
- **Expected Result:** 70-80 seconds total (10-20s additional savings)

### Phase 3C: Vectorized Mapping (Week 2)
- [ ] Implement `map_data_vectorized()`
- [ ] Add fallback to current method
- [ ] Test with various data types
- [ ] Measure performance improvement
- **Expected Result:** 60-70 seconds total (10s additional savings)

### Phase 3D: Skip Empty Table Check (Week 2)
- [ ] Add `skip_on_empty_table` flag
- [ ] Update duplicate check logic
- [ ] Test with empty and non-empty tables
- **Expected Result:** First import < 60 seconds ✅

## Expected Final Performance

| Optimization | Time Saved | Cumulative Total |
|--------------|------------|------------------|
| Baseline | - | 143.89s |
| Phase 1 (Cache) | -18.96s | 124.93s |
| Phase 2 (COPY) | +5.06s | 129.99s |
| Phase 3A (DB-side dup check) | -40s | 89.99s |
| Phase 3B (executemany) | -15s | 74.99s |
| Phase 3C (vectorized mapping) | -10s | 64.99s |
| Phase 3D (skip empty check) | -50s* | 14.99s* |

*For first import only

**Target Achievement:**
- ✅ First import: ~15-20 seconds (with skip_on_empty_table)
- ✅ Subsequent imports: ~65 seconds (with duplicate checking)
- ✅ Cached imports: ~5-10 seconds (with full cache hit)

## Risk Mitigation

1. **Feature Flags:** All optimizations behind configuration flags
2. **Fallback Mechanisms:** Keep existing code paths as fallbacks
3. **Comprehensive Testing:** Test with various file sizes and data types
4. **Monitoring:** Add detailed timing logs for each phase
5. **Rollback Plan:** Can disable optimizations via config

## Success Metrics

- [ ] 120K rows in < 60 seconds (subsequent imports)
- [ ] 120K rows in < 20 seconds (first import, no dup check)
- [ ] 10K rows in < 5 seconds
- [ ] 1K rows in < 2 seconds
- [ ] No data loss or corruption
- [ ] All existing tests pass
