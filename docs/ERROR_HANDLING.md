# Error Handling Documentation

## Overview

This document describes the comprehensive error handling system implemented across the Content Atlas application. The system is designed to prevent white screen crashes, provide detailed user-friendly error messages, and ensure robust error recovery for both the Query Interface and File Mapping features.

## Global Architecture

### Error Boundary
**Location:** `frontend/src/components/error-boundary/index.tsx`

The application uses a React Error Boundary to catch rendering errors in the component tree.

**Features:**
- Catches React rendering errors
- Prevents white screen crashes
- Provides fallback UI with error details
- Reload and retry options
- Technical details for developers (stack traces)

## Feature-Specific Error Handling

### 1. Query Interface (LLM Chat)

The Query Interface prevents white screen crashes and provides detailed feedback when LLM requests or other operations fail.

#### Architecture

**A. API Layer (`frontend/src/api/query.ts`)**
- **Network Errors**: Connection failures, timeouts, CORS issues.
- **Status Codes**: Specific handling for 401 (Auth), 403 (Permission), 404 (Not Found), 500 (Server), 503 (Service Unavailable).
- **Graceful Handling**: Manages non-JSON error responses and connectivity issues.

**B. Component Layer (`frontend/src/pages/query/QueryPage.tsx`)**
- **State Management**: Robust try-catch blocks and error state hooks.
- **Context Preservation**: Errors are added to conversation history as assistant messages.
- **Feedback**: Immediate alert banners and permanent conversation records.

**C. Display Layer (`frontend/src/pages/query/MessageDisplay.tsx`)**
- **Visuals**: Distinct "Query Failed" badges and red-bordered details boxes.
- **Troubleshooting**: Context-specific tips (e.g., "Check internet connection").
- **Technical Details**: Collapsible sections for debugging info.

#### Error Flow
```
User Query -> API Request -> [Error] -> Caught & Formatted -> State Updated -> Added to Conversation -> Displayed to User
```

#### Message Examples
- **Network**: "Unable to connect to the server. Please check your internet connection..."
- **Auth**: "Authentication failed. Please log in again."

### 2. File Mapping & Imports

The File Mapping feature includes specialized error handling to support complex data processing workflows, utilizing the `ErrorLogViewer` for detailed feedback and retry capabilities.

#### Components

**A. ErrorLogViewer (`frontend/src/components/error-log-viewer/index.tsx`)**
A reusable component for displaying detailed error information with retry functionality.
- **Features**:
    - Collapsible error details panel.
    - Categorization (Execution Failed, Schema Mismatch, etc.).
    - Timeline of error history.
    - Display of LLM decision context (JSON).
    - One-click retry button.

**B. Import Pages (`frontend/src/pages/import/[id].tsx` & `MappingModal`)**
- **Integration**: Uses `ErrorLogViewer` to show rich error context.
- **Success Flow**: Immediate transition to mapped file views (no interim success screens).
- **Error Flow**:
    1. Backend encounters error.
    2. UI displays error message, context, and suggestions via `ErrorLogViewer`.
    3. User reviews history and clicks "Retry" to attempt again with preserved context.

#### Backend Response Structure

**Success Response:**
```json
{
  "success": true,
  "llm_response": "Analysis complete...",
  "iterations_used": 3,
  "max_iterations": 5,
  "can_auto_execute": true
}
```

**Error Response:**
```json
{
  "success": false,
  "error": "Brief error message",
  "error_details": {
    "error_type": "EXECUTION_FAILED",
    "timestamp": "2025-11-01T11:53:00Z",
    "strategy_attempted": "MERGE_EXACT",
    "target_table": "customers",
    "llm_decision_context": { ... },
    "suggestions": ["Try Interactive mode..."],
    "error_history": ["Previous attempt 1 error..."]
  }
}
```

## Testing Error Scenarios

To test the error handling system:

1. **Network Error**: Stop the backend server and submit a query or upload a file.
2. **Authentication Error**: Use an expired or invalid token.
3. **Server Error**: Submit a query that causes a backend exception.
4. **Rendering Error**: Introduce a bug in component rendering (caught by Error Boundary).
5. **Mapping Error**: Upload a file with schema mismatches to trigger `ErrorLogViewer`.

## Related Files

- `frontend/src/api/query.ts` - Query API error handling
- `frontend/src/pages/query/QueryPage.tsx` - Query component logic
- `frontend/src/components/error-boundary/index.tsx` - Global Error Boundary
- `frontend/src/components/error-log-viewer/index.tsx` - Import error visualization
- `frontend/src/pages/import/[id].tsx` - Import page error integration
