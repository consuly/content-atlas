# Content Atlas — Product Snapshot

Content Atlas is a data ingestion and mapping platform built for teams that need high-fidelity imports without engineering overhead. It unifies multi-format uploads, schema detection, and duplicate protection so every dataset lands cleanly in Postgres and cloud storage.

## Core Capabilities
- Multi-format ingestion: CSV, Excel, JSON, XML, and zipped archives streamed directly from Backblaze B2.
- Dynamic schema creation: Builds target tables from mapping configs; keeps table metadata accessible through the API.
- Flexible mapping engine: Column remapping with transformation rules and structure fingerprinting to reuse mappings across similar files.
- Duplicate defense: File-level and row-level detection with configurable uniqueness rules and explicit error reporting—no silent drops.
- Scalable processing: Chunked imports, parallel duplicate checks, and async jobs for very large files.
- Cloud-native flows: Pull from B2, process in memory, and write back only once per file to minimize bandwidth and storage churn.
- Natural-language console: Ask questions against your imported data via the built-in LLM-powered CLI.
- REST API surface: Endpoints for imports, schema/statistics lookup, archive auto-processing, and async task tracking.

## What You Can Achieve
- Launch high-volume CSV/Excel/JSON/XML imports without bespoke ETL work.
- Keep customer data clean with automatic deduplication and transparent failure reporting.
- Stand up new datasets fast by letting Content Atlas create and manage Postgres tables on the fly.
- Streamline archive handling: upload a ZIP to B2 once, auto-detect schemas for each sheet/file, and keep grouped files mapped together.
- Give analysts and operators a self-serve interface—API, docs, and a natural-language console—for exploring and validating ingested data.

## Proof Points for Promotion
- Reliability: Imports prioritize fidelity with explicit errors, preserving every row the way you mapped it.
- Speed: Chunked, parallel processing keeps large uploads responsive and offloads long runs to background tasks.
- Convenience: One workflow covers uploads, schema detection, storage, deduplication, and querying—no extra glue code required.
