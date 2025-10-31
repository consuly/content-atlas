# File Mapping Feature

## Overview

This document describes the file mapping feature that allows users to map uploaded files to database tables using either automatic processing or interactive Q&A with an LLM.

## Features

### 1. Auto-Process Mode
- **Description**: Fully automated file analysis and import
- **How it works**:
  1. User clicks "Map Now" on an uploaded file
  2. Selects "Auto Process" tab
  3. Clicks "Process Now"
  4. LLM analyzes file, compares with existing tables, and automatically imports
- **Use case**: Fast processing when user trusts the AI to make decisions

### 2. Interactive Mode
- **Description**: Conversational workflow where LLM asks questions before importing
- **How it works**:
  1. User clicks "Map Now" on an uploaded file
  2. Selects "Interactive" tab
  3. Clicks "Start Interactive Analysis"
  4. LLM analyzes and may ask clarifying questions
  5. User responds to questions
  6. Process repeats until LLM has enough information
  7. User clicks "Execute Import" when ready
- **Use case**: When user wants more control or needs to clarify ambiguous situations

## Backend Implementation

### Modified Endpoints

#### `/analyze-file` (POST)
**Changes**:
- Now accepts either `file: UploadFile` OR `file_id: str` parameter
- If `file_id` provided, fetches file from `uploaded_files` table and downloads from B2
- Updates file status to "mapping" when analysis starts
- Updates file status to "mapped" or "failed" based on auto-execution result

**Parameters**:
- `file` (optional): Direct file upload
- `file_id` (optional): UUID of previously uploaded file
- `analysis_mode`: "manual", "auto_high", or "auto_always"
- `conflict_resolution`: "ask_user", "llm_decide", or "prefer_flexible"
- `max_iterations`: Maximum LLM iterations (default: 5)

**Response**:
```json
{
  "success": true,
  "llm_response": "Analysis and execution details...",
  "iterations_used": 3,
  "max_iterations": 5,
  "can_auto_execute": true
}
```

#### `/analyze-file-interactive` (POST)
**Purpose**: Handle conversational file analysis with Q&A

**Parameters**:
```json
{
  "file_id": "uuid",
  "user_message": "optional response to previous question",
  "thread_id": "optional conversation thread ID",
  "max_iterations": 5
}
```

**Response**:
```json
{
  "success": true,
  "thread_id": "conversation-uuid",
  "llm_message": "AI's question or decision",
  "needs_user_input": true,
  "question": "What should I do with...",
  "can_execute": false,
  "llm_decision": null,
  "iterations_used": 2,
  "max_iterations": 5
}
```

#### `/execute-interactive-import` (POST)
**Purpose**: Execute import after interactive conversation completes

**Parameters**:
```json
{
  "file_id": "uuid",
  "thread_id": "conversation-uuid"
}
```

**Response**:
```json
{
  "success": true,
  "message": "Import executed successfully",
  "records_processed": 1500,
  "table_name": "customers"
}
```

### File Status Tracking

The system tracks file status through the import process:

1. **uploaded**: File uploaded to B2, ready for mapping
2. **mapping**: Analysis/import in progress
3. **mapped**: Successfully imported to database
4. **failed**: Import failed

Status updates happen automatically:
- Set to "mapping" when analysis starts
- Set to "mapped" with table name and row count on success
- Set to "failed" on error

## Frontend Implementation

### Components

#### `MappingModal` Component
**Location**: `frontend/src/components/mapping-modal/index.tsx`

**Props**:
```typescript
interface MappingModalProps {
  visible: boolean;
  fileId: string;
  fileName: string;
  onClose: () => void;
  onSuccess: () => void;
}
```

**Features**:
- Two tabs: "Auto Process" and "Interactive"
- Auto tab: Simple one-click processing
- Interactive tab: Chat interface with conversation history
- Error handling and loading states
- Automatic modal cleanup on close

#### Updated `ImportPage`
**Location**: `frontend/src/pages/import/index.tsx`

**Changes**:
- Added `MappingModal` import and state management
- "Map Now" button now opens modal instead of showing "Coming soon"
- Refreshes file list after successful mapping

## Usage Examples

### Example 1: Auto-Process Flow

```typescript
// User clicks "Map Now" button
handleMapNow(file);

// Modal opens, user selects "Auto Process" tab
// User clicks "Process Now"

// Backend:
// 1. Downloads file from B2
// 2. Analyzes structure
// 3. Compares with existing tables
// 4. Makes decision (e.g., merge into existing table)
// 5. Executes import
// 6. Updates file status to "mapped"

// Frontend shows success message and refreshes list
```

### Example 2: Interactive Flow

```typescript
// User clicks "Map Now" button
handleMapNow(file);

// Modal opens, user selects "Interactive" tab
// User clicks "Start Interactive Analysis"

// Backend analyzes and asks:
// "I found a similar table 'customers'. Should I:
//  1. Merge into existing table
//  2. Create new table
//  3. Let me decide column mappings"

// User responds: "Merge into existing table"

// Backend confirms decision and shows execute button
// User clicks "Execute Import"

// Backend executes import and updates status
```

## Testing

### Manual Testing Steps

1. **Test Auto-Process**:
   - Upload a CSV file
   - Click "Map Now"
   - Select "Auto Process" tab
   - Click "Process Now"
   - Verify file status changes to "mapping" then "mapped"
   - Check that table was created/updated correctly

2. **Test Interactive Mode**:
   - Upload a CSV file
   - Click "Map Now"
   - Select "Interactive" tab
   - Click "Start Interactive Analysis"
   - Respond to any questions from the AI
   - Click "Execute Import" when ready
   - Verify import completed successfully

3. **Test Error Handling**:
   - Try processing an invalid file
   - Verify error messages display correctly
   - Verify file status updates to "failed"

### Automated Testing

See `tests/test_llm_sequential_merge.py` for an example of testing the auto-process flow with LLM decision-making.

## Configuration

### Environment Variables

No new environment variables required. Uses existing:
- `ANTHROPIC_API_KEY`: For LLM analysis
- `VITE_API_URL`: Frontend API endpoint

### LLM Settings

Default settings in `/analyze-file` endpoint:
- Model: Claude Haiku 4.5 (fast and cost-effective)
- Max iterations: 5
- Temperature: 0 (deterministic)

## Architecture Decisions

### Why Two Modes?

1. **Auto-Process**: For users who want speed and trust the AI
2. **Interactive**: For users who want control or have complex scenarios

### Why File ID Instead of Re-uploading?

- Avoids duplicate uploads
- Leverages existing B2 storage
- Maintains file metadata and history
- Enables retry without re-upload

### Why Conversation Memory?

- Enables natural back-and-forth dialogue
- LLM can reference previous questions/answers
- Better user experience for complex scenarios

## Future Enhancements

1. **Confidence Scores**: Show LLM's confidence in decisions
2. **Preview Mode**: Show what will be imported before executing
3. **Undo/Rollback**: Ability to undo an import
4. **Batch Processing**: Map multiple files at once
5. **Custom Rules**: User-defined mapping rules
6. **History View**: See past conversations and decisions

## Related Documentation

- [AI File Analysis](./AI_FILE_ANALYSIS.md)
- [Import Tracking](./IMPORT_TRACKING.md)
- [API Reference](./API_REFERENCE.md)
