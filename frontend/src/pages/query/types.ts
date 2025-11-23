/**
 * TypeScript interfaces for the Query Database feature
 */

export type ChartType = 'bar' | 'line' | 'pie';

export interface ChartDataset {
  label: string;
  data: number[];
  backgroundColor?: string[] | string;
  borderColor?: string[] | string;
  fill?: boolean;
}

export interface ChartSpec {
  type: ChartType;
  labels: string[];
  datasets: ChartDataset[];
  options?: Record<string, unknown>;
}

export interface ChartSuggestion {
  should_display: boolean;
  reason: string;
  spec?: ChartSpec | null;
}

export interface QueryMessage {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  executedSql?: string;
  dataCsv?: string;
  executionTime?: number;
  rowsReturned?: number;
  chartSuggestion?: ChartSuggestion;
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
  thread_id?: string;
  executed_sql?: string;
  data_csv?: string;
  execution_time_seconds?: number;
  rows_returned?: number;
  chart_suggestion?: ChartSuggestion;
  error?: string;
}

export interface QueryConversationMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp?: string;
  executed_sql?: string;
  data_csv?: string;
  execution_time_seconds?: number;
  rows_returned?: number;
  chart_suggestion?: ChartSuggestion;
  error?: string;
}

export interface QueryConversation {
  thread_id: string;
  messages: QueryConversationMessage[];
  updated_at?: string;
  created_at?: string;
}

export interface QueryConversationResponse {
  success: boolean;
  conversation?: QueryConversation | null;
  error?: string;
}

export interface QueryConversationSummary {
  thread_id: string;
  created_at?: string;
  updated_at?: string;
  first_user_prompt?: string;
}

export interface QueryConversationListResponse {
  success: boolean;
  conversations: QueryConversationSummary[];
}

export interface ConversationState {
  threadId: string;
  messages: QueryMessage[];
  isLoading: boolean;
}
