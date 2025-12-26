# AI Assisted Mapping

This document describes the AI-powered file mapping and analysis system, covering both the technical implementation and the user-facing features.

## Table of Contents

- [Overview](#overview)
- [Modes of Operation](#modes-of-operation)
- [How It Works](#how-it-works)
- [API Endpoints](#api-endpoints)
- [Frontend Implementation](#frontend-implementation)
- [Configuration](#configuration)
- [Import Strategies](#import-strategies)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Overview

The Content Atlas uses LangChain agents with Claude Sonnet to automatically analyze uploaded files, determine their structure, and map them to database tables. The system can:

- **Analyze file structure** (columns, data types, data quality)
- **Compare with existing tables** to find semantic matches
- **Recommend strategies** (new table, merge, extend, adapt)
- **Resolve conflicts** (data type mismatches, naming variations)
- **Provide reasoning** for its decisions

---

## Modes of Operation

The feature offers two primary modes for users:

### 1. Auto-Process Mode
- **Description**: Fully automated file analysis and import.
- **Workflow**:
  1. User clicks "Map Now" -> "Auto Process" -> "Process Now".
  2. LLM analyzes file, compares with tables, and executes the best strategy.
  3. Status updates to "mapped" automatically.
- **Use case**: Trusted data sources, speed.

### 2. Interactive Mode
- **Description**: Conversational workflow where the LLM asks clarifying questions.
- **Workflow**:
  1. User clicks "Map Now" -> "Interactive" -> "Start Analysis".
  2. LLM analyzes and asks questions (e.g., "Should I merge this into 'customers'?").
  3. User responds via chat interface.
  4. Process repeats until LLM has enough info to execute.
- **Use case**: Complex data, ambiguous mappings, user control.

---

## How It Works

### Analysis Pipeline

```
1. File Upload
   ↓
2. Smart Sampling (adapts to file size)
   ↓
3. LLM Agent Analysis (max 5 iterations)
   ├─ Analyze file structure
   ├─ Get existing database schema
   ├─ Compare with tables
   └─ Resolve conflicts
   ↓
4. Generate Recommendation (Strategy, Mapping, Confidence)
   ↓
5. Execution (Auto or Manual confirmation)
```

### Smart Sampling

To ensure efficiency, the system samples files based on size:
- **≤ 100 rows**: All rows.
- **100-1,000 rows**: 100 rows (50 start + 50 random).
- **1,000-10,000 rows**: 200 rows.
- **> 10,000 rows**: 500 rows (stratified).

---

## API Endpoints

### `POST /analyze-file`
Analyzes a file and recommends an import strategy. Can trigger auto-execution if configured.

**Parameters**:
- `file` (optional): Direct file upload.
- `file_id` (optional): UUID of previously uploaded file (B2).
- `analysis_mode`: `manual`, `auto_high`, or `auto_always`.
- `conflict_resolution`: `ask_user`, `llm_decide`, or `prefer_flexible`.
- `max_iterations`: Max LLM turns (default 5).

**Response**:
```json
{
  "success": true,
  "recommended_strategy": "merge_exact",
  "confidence": 0.95,
  "reasoning": "Matches 'customers' table...",
  "can_auto_execute": false,
  "llm_response": "..."
}
```

### `POST /analyze-file-interactive`
Handles conversational analysis.

**Parameters**:
```json
{
  "file_id": "uuid",
  "user_message": "Merge into existing table",
  "thread_id": "optional-thread-uuid"
}
```

**Response**:
```json
{
  "success": true,
  "question": "Should I create a new table?",
  "needs_user_input": true,
  "can_execute": false
}
```

### `POST /execute-recommended-import`
Executes a confirmed mapping strategy.

**Parameters**:
```json
{
  "analysis_id": "uuid",
  "confirmed_mapping": { ... }
}
```

### `POST /execute-interactive-import`
Executes the final decision from an interactive session.

**Parameters**:
```json
{
  "file_id": "uuid",
  "thread_id": "thread-uuid"
}
```

---

## Frontend Implementation

### `MappingModal`
Located at `frontend/src/components/mapping-modal/index.tsx`.
- **Tabs**: "Auto Process" and "Interactive".
- **State**: Manages loading, errors, and conversation history.
- **Props**: `fileId`, `fileName`, `onSuccess`.

### File Status Tracking
- **uploaded**: Ready for mapping.
- **mapping**: Analysis in progress.
- **mapped**: Successfully imported.
- **failed**: Error occurred.

---

## Configuration

### Analysis Modes
- `manual`: Always requires user review (default).
- `auto_high`: Auto-execute if confidence ≥ threshold (e.g. 0.9).
- `auto_always`: Always auto-execute (use with caution).

### Conflict Resolution
- `ask_user`: Stop on conflicts.
- `llm_decide`: Let LLM resolve based on context.
- `prefer_flexible`: Default to flexible types (e.g., TEXT).

### Environment Variables
- `ANTHROPIC_API_KEY`: Required for LLM.
- `VITE_API_URL`: Frontend API endpoint.

---

## Import Strategies

1. **NEW_TABLE**: Create a new table. (e.g., unique inventory list)
2. **MERGE_EXACT**: Insert into existing table with matching schema. (e.g., monthly report)
3. **EXTEND_TABLE**: Add columns to existing table. (e.g., customer list with new fields)
4. **ADAPT_DATA**: Transform data to fit schema. (e.g., rename columns)

---

## Usage Examples

### Auto-Process Flow
```typescript
// Frontend
handleMapNow(file);
// User selects "Auto Process" -> "Process Now"
// Backend analyzes, auto-executes if high confidence, returns success.
```

### Interactive Flow
```typescript
// Frontend
handleMapNow(file);
// User selects "Interactive" -> "Start"
// Backend: "Found table 'clients'. Merge or new?"
// User: "Merge"
// Backend: "Done. Ready to execute?" -> User clicks Execute.
```

---

## Best Practices

1. **Start with Manual Mode**: Verify AI decisions before trusting `auto_high`.
2. **Check Iterations**: If `iterations_used` hits max, increase limit or simplify data.
3. **Review Confidence**: Use confidence scores to gate automation.
4. **Data Quality**: The system flags quality issues (nulls, mixed types); review these.
5. **Cost Management**: Limit `max_iterations` to control API usage.

---

## Troubleshooting

- **Analysis Failed**: Check file format (CSV/Excel/JSON/XML) and corruption. Verify `ANTHROPIC_API_KEY`.
- **Low Confidence**: File structure might be unusual or ambiguous. Try interactive mode.
- **Max Iterations Reached**: Increase limit or clean data.
- **White Screen / UI Error**: Check `ErrorLogViewer` in frontend for detailed trace.
