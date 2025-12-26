# Scalability and Performance

This document outlines the architecture and strategies used to handle large datasets efficiently in the Content Atlas, from frontend file uploads to backend data processing.

## Table of Contents

- [Overview](#overview)
- [Frontend: Parallel Chunked Upload](#frontend-parallel-chunked-upload)
- [Backend: Parallel Import Processing](#backend-parallel-import-processing)
- [Historical Optimizations](#historical-optimizations)

---

## Overview

Handling large files (100MB+) and datasets (100k+ rows) requires a multi-layered approach to prevent timeouts, memory exhaustion, and slow user experiences. The Content Atlas employs parallelization at both the upload stage (browser to storage) and the processing stage (backend to database).

---

## Frontend: Parallel Chunked Upload

For files larger than **10MB**, the frontend uses a parallel chunked upload strategy to send data directly to B2 storage, bypassing the backend bottleneck.

### Performance Gains

| File Size | Old (Sequential Proxy) | New (Parallel Direct) | Improvement |
|-----------|------------------------|-----------------------|-------------|
| 10 MB     | ~1 minute              | ~2-3 seconds          | **20-30x**  |
| 100 MB    | ~8 minutes             | ~15-20 seconds        | **24-32x**  |

### Architecture

1.  **Browser**: Calculates SHA-256 hash.
2.  **Start Upload**: `POST /start-multipart-upload` -> Returns `upload_id` + presigned URLs for chunks.
3.  **Parallel Upload**: Browser splits file and uploads chunks to B2 in parallel (max 4 concurrent).
4.  **Complete Upload**: `POST /complete-multipart-upload` -> B2 assembles file; backend saves metadata.

### Backend Endpoints

-   `POST /start-multipart-upload`: Initiates session.
-   `POST /complete-multipart-upload`: Finalizes session.
-   `POST /abort-multipart-upload`: Cleans up partial uploads.

### Frontend Implementation

-   **Component**: `frontend/src/components/file-upload/`
-   **Logic**:
    -   Detects files >10MB.
    -   Splits into 5MB-100MB chunks (auto-calculated).
    -   Retries failed chunks (up to 3 times).
    -   Tracks progress per chunk.

---

## Backend: Parallel Import Processing

Once the file is uploaded, the backend uses parallel processing for both data mapping and duplicate checking to import large datasets efficiently.

### Three-Phase Processing Model

#### Phase 0: Parallel Data Mapping (CPU-Intensive)
-   **Goal**: Transform raw records into database-ready format.
-   **Method**: `ThreadPoolExecutor` with up to 4 workers.
-   **Action**: Maps fields, standardizes dates, and coerces types in parallel chunks.

#### Phase 1: Parallel Duplicate Checking (CPU-Intensive)
-   **Goal**: Identify duplicates without database race conditions.
-   **Method**: `ThreadPoolExecutor` with up to 4 workers.
-   **Action**: Pre-loads existing uniqueness keys once, then checks all chunks in parallel using vectorized Pandas operations.

#### Phase 2: Sequential Insertion (I/O-Intensive)
-   **Goal**: Safely write data to PostgreSQL.
-   **Method**: Sequential bulk inserts.
-   **Action**: Inserts validated chunks using Pandas `to_sql` (method=`multi`). Sequential insertion prevents transaction deadlocks and race conditions.

### Performance Benefits

| File Size | Records | Old (Sequential) | New (Parallel) | Speedup |
|-----------|---------|------------------|----------------|---------|
| Medium    | 15,000  | 12 seconds       | 5 seconds      | **2.4x**|
| Large     | 100,000 | 150 seconds      | 40 seconds     | **3.75x**|

### Configuration

The system automatically configures itself based on file size:
-   **Chunk Size**: 10,000 records (default).
-   **Max Workers**: Min(4, CPU count).

---

## Historical Optimizations

This section tracks the history of performance improvements implemented to reach current benchmarks.

### Baseline (October 2025)
**Test File**: 120,000 rows (~20MB)
**Initial Time**: 143.89 seconds

### Phase 1: File Parsing Cache
-   **Change**: Cached parsed records in memory (5 min TTL) based on file hash.
-   **Result**: 124.93s (13% faster).
-   **Impact**: Skipped re-parsing on repeated mapping attempts.

### Phase 2: PostgreSQL COPY & Enhanced Caching
-   **Change**: Attempted using `COPY` instead of `INSERT`.
-   **Result**: 129.99s (Slower).
-   **Lesson**: For "medium" chunks (20k rows), the overhead of CSV serialization for `COPY` outweighed the speed benefits compared to optimized batch `INSERT`. Reverted to `to_sql`.

### Phase 3: Database-Side Duplicate Checking
-   **Change**: Replaced Pandas-based merge with PostgreSQL `WHERE (...) IN (VALUES ...)` queries.
-   **Result**: 122.42s (6% faster).
-   **Impact**: Reduced memory usage by avoiding loading full tables into Pandas.

### Phase 4: Vectorized Data Mapping (Attempted)
-   **Change**: Tried converting row-by-row mapping to Pandas vectorization.
-   **Result**: 126.37s (3% slower).
-   **Lesson**: DataFrame creation overhead is high. For simple dictionary mapping, standard Python loops are often faster than Pandas overhead for <100k rows.

### Current Roadmap
1.  **Optimize Duplicate Checking**: Further move logic to DB side.
2.  **Direct `psycopg2` Inserts**: Bypass Pandas `to_sql` overhead for raw speed.
3.  **Skip Empty Table Check**: Automatically skip duplicate checks if table is known to be empty (first import).
