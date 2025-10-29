/**
 * TypeScript interfaces for the Query Database feature
 */

export interface QueryMessage {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  executedSql?: string;
  dataCsv?: string;
  executionTime?: number;
  rowsReturned?: number;
  error?: string;
}

export interface QueryRequest {
  prompt: string;
  thread_id?: string;
  max_rows?: number;
}

export interface QueryResponse {
  success: boolean;
  response: string;
  executed_sql?: string;
  data_csv?: string;
  execution_time_seconds?: number;
  rows_returned?: number;
  error?: string;
}

export interface ConversationState {
  threadId: string;
  messages: QueryMessage[];
  isLoading: boolean;
}
