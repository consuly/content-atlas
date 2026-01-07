export interface UploadedFile {
  id: string;
  file_name: string;
  b2_file_id: string;
  b2_file_path: string;
  file_size: number;
  content_type?: string;
  upload_date?: string;
  status: string;
  mapped_table_name?: string;
  mapped_date?: string;
  mapped_rows?: number;
  error_message?: string;
  active_job_id?: string;
  active_job_status?: string;
  active_job_stage?: string;
  active_job_progress?: number;
  active_job_started_at?: string;
}

export interface ProcessingResult {
  success: boolean;
  table_name?: string;
  rows_imported?: number;
  execution_time?: number;
  error?: string;
}

export interface TableData {
  data: Array<Record<string, unknown>>;
  total_rows: number;
}

export interface ImportHistory {
  import_id: string;
  import_timestamp: string;
  table_name: string;
  import_strategy?: string;
  status: string;
  total_rows_in_file?: number;
  rows_inserted?: number;
  duplicates_found?: number;
  data_validation_errors?: number;
  mapping_errors_count?: number;
  duration_seconds?: number;
  mapping_config?: Record<string, unknown>;
}

export interface DuplicateRowData {
  id: number;
  record_number?: number | null;
  record: Record<string, unknown>;
  detected_at?: string | null;
  resolved_at?: string | null;
  resolved_by?: string | null;
  resolution_details?: Record<string, unknown> | null;
}

export interface DuplicateRowsState {
  rows: DuplicateRowData[];
  total: number;
}

export interface ValidationFailureRow {
  id: number;
  record_number?: number | null;
  record: Record<string, unknown>;
  validation_errors: Array<{
    column: string;
    error_type: string;
    error_message: string;
  }>;
  detected_at?: string | null;
  resolved_at?: string | null;
  resolved_by?: string | null;
  resolution_action?: string | null;
  resolution_details?: Record<string, unknown> | null;
}

export interface ValidationFailuresState {
  rows: ValidationFailureRow[];
  total: number;
}

export interface DuplicateExistingRow {
  row_id: number;
  record: Record<string, unknown>;
}

export interface DuplicateDetail {
  duplicate: DuplicateRowData;
  existing_row: DuplicateExistingRow | null;
  uniqueness_columns: string[];
}

export type AutoRecoveryOutcome =
  | { recovered: true }
  | {
      recovered: false;
      reason: 'no_plan' | 'analysis_failed' | 'execution_failed' | 'exception';
      errorMessage?: string;
    };

export interface ImportJobInfo {
  id: string;
  file_id: string;
  status: string;
  stage?: string | null;
  progress?: number | null;
  error_message?: string | null;
  trigger_source?: string | null;
  analysis_mode?: string | null;
  conflict_mode?: string | null;
  retry_attempt?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  completed_at?: string | null;
  metadata?: Record<string, unknown> | null;
  result_metadata?: Record<string, unknown> | null;
}

export type ArchiveFileStatus = 'processed' | 'failed' | 'skipped';

export interface ArchiveFileResult {
  archive_path: string;
  stored_file_name?: string | null;
  uploaded_file_id?: string | null;
  sheet_name?: string | null;
  status: ArchiveFileStatus;
  table_name?: string | null;
  records_processed?: number | null;
  duplicates_skipped?: number | null;
  validation_errors?: number | null;
  import_id?: string | null;
  auto_retry_used?: boolean;
  message?: string | null;
}

export interface ArchiveAutoProcessResult {
  success: boolean;
  total_files: number;
  processed_files: number;
  failed_files: number;
  skipped_files: number;
  results: ArchiveFileResult[];
  job_id?: string | null;
}

export type ArchiveJobCompletedEntry = {
  archive_path: string;
  status: ArchiveFileStatus;
};

export type ArchiveJobMetadata = {
  source?: string;
  files_in_archive?: number;
  remaining_files?: string[];
  completed_files?: ArchiveJobCompletedEntry[];
  current_file?: string | null;
  processed?: number;
  failed?: number;
  skipped?: number;
  total?: number;
};

export type ArchiveResultMetadata = {
  files_total?: number;
  processed_files?: number;
  failed_files?: number;
  skipped_files?: number;
  results?: ArchiveFileResult[];
};

export interface ArchiveHistorySummary {
  job: ImportJobInfo;
  result: ArchiveAutoProcessResult;
}

export interface InstructionOption {
  id: string;
  title: string;
  content: string;
}
