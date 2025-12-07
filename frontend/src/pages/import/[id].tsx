import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router';
import { App as AntdApp, Card, Tabs, Button, Space, Alert, Spin, Typography, Result, Statistic, Row, Col, Breadcrumb, Descriptions, Table, Tag, Divider, Modal, Switch, Input, Progress, Select, Checkbox, Collapse } from 'antd';
import type { BreadcrumbProps, DescriptionsProps } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ThunderboltOutlined, MessageOutlined, CheckCircleOutlined, ArrowLeftOutlined, HomeOutlined, FileOutlined, DatabaseOutlined, InfoCircleOutlined, EyeOutlined, MergeCellsOutlined } from '@ant-design/icons';
import axios, { AxiosError } from 'axios';
import { ErrorLogViewer } from '../../components/error-log-viewer';
import { formatUserFacingError } from '../../utils/errorMessages';
import { API_URL } from '../../config';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;
const DUPLICATE_PREVIEW_LIMIT = 20;

interface UploadedFile {
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

interface ProcessingResult {
  success: boolean;
  table_name?: string;
  rows_imported?: number;
  execution_time?: number;
  error?: string;
}

interface TableData {
  data: Array<Record<string, unknown>>;
  total_rows: number;
}

interface ImportHistory {
  import_id: string;
  import_timestamp: string;
  table_name: string;
  import_strategy?: string;
  status: string;
  total_rows_in_file?: number;
  rows_inserted?: number;
  duplicates_found?: number;
  data_validation_errors?: number;
  duration_seconds?: number;
  mapping_config?: Record<string, unknown>;
}

interface DuplicateRowData {
  id: number;
  record_number?: number | null;
  record: Record<string, unknown>;
  detected_at?: string | null;
  resolved_at?: string | null;
  resolved_by?: string | null;
  resolution_details?: Record<string, unknown> | null;
}

interface DuplicateRowsState {
  rows: DuplicateRowData[];
  total: number;
}

interface DuplicateExistingRow {
  row_id: number;
  record: Record<string, unknown>;
}

interface DuplicateDetail {
  duplicate: DuplicateRowData;
  existing_row: DuplicateExistingRow | null;
  uniqueness_columns: string[];
}

type AutoRecoveryOutcome =
  | { recovered: true }
  | {
      recovered: false;
      reason: 'no_plan' | 'analysis_failed' | 'execution_failed' | 'exception';
      errorMessage?: string;
    };

interface ImportJobInfo {
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

type ArchiveFileStatus = 'processed' | 'failed' | 'skipped';

interface ArchiveFileResult {
  archive_path: string;
  stored_file_name?: string | null;
  uploaded_file_id?: string | null;
  sheet_name?: string | null;
  status: ArchiveFileStatus;
  table_name?: string | null;
  records_processed?: number | null;
  duplicates_skipped?: number | null;
  import_id?: string | null;
  auto_retry_used?: boolean;
  message?: string | null;
}

interface ArchiveAutoProcessResult {
  success: boolean;
  total_files: number;
  processed_files: number;
  failed_files: number;
  skipped_files: number;
  results: ArchiveFileResult[];
  job_id?: string | null;
}

type ArchiveJobCompletedEntry = {
  archive_path: string;
  status: ArchiveFileStatus;
};

type ArchiveJobMetadata = {
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

type ArchiveResultMetadata = {
  files_total?: number;
  processed_files?: number;
  failed_files?: number;
  skipped_files?: number;
  results?: ArchiveFileResult[];
};

interface ArchiveHistorySummary {
  job: ImportJobInfo;
  result: ArchiveAutoProcessResult;
}

export const ImportMappingPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { message: messageApi } = AntdApp.useApp();
  
  const [file, setFile] = useState<UploadedFile | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<string>('auto');
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ProcessingResult | null>(null);
  const [jobInfo, setJobInfo] = useState<ImportJobInfo | null>(null);
  const [archiveProcessing, setArchiveProcessing] = useState(false);
  const [archiveResumeLoading, setArchiveResumeLoading] = useState(false);
  const [archiveResult, setArchiveResult] = useState<ArchiveAutoProcessResult | null>(null);
  const [archiveHistorySummary, setArchiveHistorySummary] = useState<ArchiveHistorySummary | null>(null);
  const [archiveJobDetails, setArchiveJobDetails] = useState<ImportJobInfo | null>(null);
  const [useSharedTable, setUseSharedTable] = useState(false);
  const [sharedTableName, setSharedTableName] = useState('');
  const [sharedTableMode, setSharedTableMode] = useState<'existing' | 'new'>('new');
  const [skipFileDuplicateCheck, setSkipFileDuplicateCheck] = useState(false);
  const [existingTables, setExistingTables] = useState<Array<{ table_name: string; row_count: number }>>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [sheetNames, setSheetNames] = useState<string[]>([]);
  const [selectedSheets, setSelectedSheets] = useState<string[]>([]);
  const [interactiveSheet, setInteractiveSheet] = useState<string | undefined>(undefined);
  const [llmInstruction, setLlmInstruction] = useState('');
  const [instructionTitle, setInstructionTitle] = useState('');
  const [saveInstruction, setSaveInstruction] = useState(false);
  const [instructionOptions, setInstructionOptions] = useState<
    { id: string; title: string; content: string }[]
  >([]);
  const [selectedInstructionId, setSelectedInstructionId] = useState<string | null>(null);
  const [loadingInstructions, setLoadingInstructions] = useState(false);
  const [instructionActionLoading, setInstructionActionLoading] = useState(false);
  
  // Interactive mode state
  const [threadId, setThreadId] = useState<string | null>(null);
  const [conversation, setConversation] = useState<Array<{ role: 'user' | 'assistant'; content: string }>>([]);
  const [userInput, setUserInput] = useState('');
  const [canExecute, setCanExecute] = useState(false);
  const [needsUserInput, setNeedsUserInput] = useState(true);
  const [showInteractiveRetry, setShowInteractiveRetry] = useState(false);
  const quickActions = [
    { label: 'Approve Plan', prompt: 'CONFIRM IMPORT' },
    {
      label: 'Request New Table',
      prompt: 'Could we create a brand new table for this import instead? Outline the recommended schema.',
    },
    {
      label: 'Adjust Column Mapping',
      prompt: 'Please walk me through adjusting the column mapping. Suggest columns that should be renamed or remapped.',
    },
    {
      label: 'Review Duplicates',
      prompt: 'Explain how duplicate detection is configured. Are there better uniqueness rules we should consider?',
    },
  ];

  const fetchInstructions = useCallback(async () => {
    setLoadingInstructions(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.get<{ success: boolean; instructions: { id: string; title: string; content: string }[] }>(
        `${API_URL}/llm-instructions`,
        {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );
      if (response.data?.success && Array.isArray(response.data.instructions)) {
        setInstructionOptions(response.data.instructions);
      }
    } catch (error) {
      console.error('Failed to load instructions', error);
    } finally {
      setLoadingInstructions(false);
    }
  }, []);

  const isArchiveFile = file?.file_name?.toLowerCase().endsWith('.zip') ?? false;
  const isArchive = isArchiveFile;
  const isExcelFile =
    file?.file_name?.toLowerCase().endsWith('.xlsx') ||
    file?.file_name?.toLowerCase().endsWith('.xls') ||
    false;
  const hasMultipleSheets = isExcelFile && sheetNames.length > 1;

  const effectiveArchiveResult = archiveResult ?? archiveHistorySummary?.result ?? null;

  const failedArchiveResults = useMemo(
    () => effectiveArchiveResult?.results.filter((item) => item.status === 'failed') ?? [],
    [effectiveArchiveResult]
  );

  const archiveFailureSummary = useMemo(() => {
    if (!isArchiveFile || !effectiveArchiveResult) {
      return null;
    }

    const failedFiles =
      typeof effectiveArchiveResult.failed_files === 'number' && effectiveArchiveResult.failed_files > 0
        ? effectiveArchiveResult.failed_files
        : 0;
    const processedFiles =
      typeof effectiveArchiveResult.processed_files === 'number'
        ? effectiveArchiveResult.processed_files
        : 0;
    const skippedFiles =
      typeof effectiveArchiveResult.skipped_files === 'number'
        ? effectiveArchiveResult.skipped_files
        : 0;
    const resultCount = Array.isArray(effectiveArchiveResult.results)
      ? effectiveArchiveResult.results.length
      : 0;
    const totalFilesFromResult =
      typeof effectiveArchiveResult.total_files === 'number'
        ? effectiveArchiveResult.total_files
        : resultCount;
    const derivedTotal =
      totalFilesFromResult ||
      processedFiles + failedFiles + skippedFiles ||
      resultCount;
    const totalFiles = Math.max(derivedTotal, 0);
    const successfulFiles = Math.max(0, totalFiles - failedFiles - skippedFiles);
    const hasPartialFailure =
      failedFiles > 0 && totalFiles > 0 && failedFiles < totalFiles;

    return {
      totalFiles,
      failedFiles,
      successfulFiles,
      skippedFiles,
      hasPartialFailure,
    };
  }, [effectiveArchiveResult, isArchiveFile]);

  const hasPartialArchiveFailure = !!archiveFailureSummary?.hasPartialFailure;
  const archiveFailedFileCount = archiveFailureSummary?.failedFiles ?? 0;
  const archiveTotalFileCount = archiveFailureSummary?.totalFiles ?? 0;
  const archiveSuccessfulFileCount = archiveFailureSummary?.successfulFiles ?? 0;
  const archiveSkippedFileCount = archiveFailureSummary?.skippedFiles ?? 0;
  const archiveTotalForDisplay =
    archiveTotalFileCount ||
    archiveSuccessfulFileCount + archiveFailedFileCount + archiveSkippedFileCount;
  const shouldHideJobAlert = isArchiveFile && hasPartialArchiveFailure;

  const archiveAggregates = useMemo(() => {
    if (!effectiveArchiveResult) {
      return null;
    }

    const aggregate = effectiveArchiveResult.results.reduce(
      (acc, item) => {
        if (item.status === 'processed') {
          acc.totalRecords += item.records_processed ?? 0;
          acc.totalDuplicates += item.duplicates_skipped ?? 0;
          if (item.table_name) {
            acc.tableNames.add(item.table_name);
          }
        }
        return acc;
      },
      { totalRecords: 0, totalDuplicates: 0, tableNames: new Set<string>() }
    );

    return {
      totalRecords: aggregate.totalRecords,
      totalDuplicates: aggregate.totalDuplicates,
      tablesTouched: aggregate.tableNames.size,
    };
  }, [effectiveArchiveResult]);

  const displayJobInfo = useMemo<ImportJobInfo | null>(() => {
    const baseMetadata = jobInfo?.metadata ?? null;
    const baseResultMetadata = jobInfo?.result_metadata ?? null;

    if (jobInfo) {
      return jobInfo;
    }

    if (!file) {
      return null;
    }

    const hasActiveState =
      file.active_job_id || file.active_job_status || file.active_job_stage || file.active_job_progress;

    if (!hasActiveState) {
      return null;
    }

    return {
      id: file.active_job_id ?? 'untracked-job',
      file_id: file.id,
      status: file.active_job_status ?? 'running',
      stage: file.active_job_stage,
      progress: file.active_job_progress ?? undefined,
      retry_attempt: 1,
      error_message: file.error_message,
      trigger_source: isArchiveFile ? 'archive_auto_process' : undefined,
      analysis_mode: undefined,
      conflict_mode: undefined,
      created_at: file.active_job_started_at ?? undefined,
      updated_at: undefined,
      completed_at: undefined,
      metadata: baseMetadata,
      result_metadata: baseResultMetadata,
    };
  }, [file, isArchiveFile, jobInfo]);

  const archiveJobProgress = useMemo(() => {
    if (!displayJobInfo || displayJobInfo.trigger_source !== 'archive_auto_process') {
      return null;
    }

    const metadata = (displayJobInfo.metadata || {}) as ArchiveJobMetadata;
    const remainingRaw = Array.isArray(metadata.remaining_files)
      ? metadata.remaining_files
      : [];
    const remaining = remainingRaw.filter((value): value is string => typeof value === 'string');

    const completedRaw = Array.isArray(metadata.completed_files)
      ? metadata.completed_files
      : [];
    const completed = completedRaw
      .map((entry) => {
        if (!entry) {
          return null;
        }
        const path = (entry as ArchiveJobCompletedEntry).archive_path;
        const status = (entry as ArchiveJobCompletedEntry).status;
        if (typeof path !== 'string') {
          return null;
        }
        if (status !== 'processed' && status !== 'failed' && status !== 'skipped') {
          return null;
        }
        return { archive_path: path, status };
      })
      .filter((entry): entry is ArchiveJobCompletedEntry => !!entry);

    const currentFile =
      typeof metadata.current_file === 'string' ? (metadata.current_file as string) : null;

    return {
      currentFile,
      remaining,
      completed,
    };
  }, [displayJobInfo]);

  const normalizeJobStatus = (status?: string | null) =>
    (status || '').toLowerCase().trim() || null;
  const effectiveJobStatus = normalizeJobStatus(
    displayJobInfo?.status ?? file?.active_job_status ?? null
  );
  const hasJobMetadata =
    !!displayJobInfo ||
    !!file?.active_job_id ||
    !!file?.active_job_stage ||
    typeof file?.active_job_progress === 'number';
  const mappingJobActive =
    (!!effectiveJobStatus &&
      effectiveJobStatus !== 'succeeded' &&
      effectiveJobStatus !== 'failed' &&
      effectiveJobStatus !== 'completed' &&
      effectiveJobStatus !== 'cancelled' &&
      effectiveJobStatus !== 'canceled') ||
    (!effectiveJobStatus && hasJobMetadata);

  const isArchiveMappingActive = isArchive && mappingJobActive;
  const archiveProgressPercent =
    displayJobInfo?.progress ?? file?.active_job_progress ?? 0;
  const archiveProgressStatus: 'success' | 'exception' | 'active' =
    effectiveJobStatus === 'failed'
      ? 'exception'
      : effectiveJobStatus === 'succeeded'
        ? 'success'
        : 'active';
  const disableMappingActions =
    mappingJobActive ||
    file?.status === 'mapping' ||
    processing ||
    archiveProcessing ||
    archiveResumeLoading;
  const isMappingInProgress =
    (processing || file?.status === 'mapping' || mappingJobActive) &&
    effectiveJobStatus !== 'failed' &&
    file?.status !== 'failed';
  const mappingStageLabel = displayJobInfo?.stage ?? file?.active_job_stage ?? file?.active_job_status ?? null;
  const mappingProgress =
    typeof displayJobInfo?.progress === 'number'
      ? displayJobInfo.progress
      : typeof file?.active_job_progress === 'number'
        ? file.active_job_progress
        : null;
  const mappingChunkProgress = useMemo(() => {
    const metadata = displayJobInfo?.metadata;
    if (!metadata || typeof metadata !== 'object') {
      return null;
    }

    const asRecord = metadata as Record<string, unknown>;
    const toNumber = (value: unknown): number | null => {
      if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
      }
      if (typeof value === 'string') {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
      }
      return null;
    };

    const totalChunks =
      toNumber(asRecord.total_chunks) ??
      toNumber((asRecord as Record<string, unknown>).chunks_total) ??
      toNumber((asRecord as Record<string, unknown>).totalChunks) ??
      null;
    const chunksCompleted =
      toNumber(asRecord.chunks_completed) ??
      toNumber((asRecord as Record<string, unknown>).chunksCompleted) ??
      null;

    if (totalChunks === null && chunksCompleted === null) {
      return null;
    }

    const normalizedTotal = totalChunks && totalChunks > 0 ? totalChunks : null;
    const normalizedCompleted = Math.max(0, chunksCompleted ?? 0);
    const percent =
      normalizedTotal !== null && normalizedTotal > 0
        ? Math.min(100, Math.floor((normalizedCompleted / normalizedTotal) * 100))
        : null;

    return {
      totalChunks: normalizedTotal,
      chunksCompleted: normalizedCompleted,
      percent,
    };
  }, [displayJobInfo?.metadata]);
  const normalizedMappingProgress =
    typeof mappingProgress === 'number' && Number.isFinite(mappingProgress)
      ? Math.min(100, Math.max(0, mappingProgress))
      : mappingChunkProgress?.percent ?? null;
  const chunkProgressLabel = useMemo(() => {
    if (!mappingChunkProgress) {
      return null;
    }
    if (mappingChunkProgress.totalChunks) {
      const clampedCurrent = Math.min(mappingChunkProgress.chunksCompleted, mappingChunkProgress.totalChunks);
      return `Chunk ${clampedCurrent}/${mappingChunkProgress.totalChunks}`;
    }
    if (mappingChunkProgress.chunksCompleted > 0) {
      return `Chunks completed: ${mappingChunkProgress.chunksCompleted}`;
    }
    return null;
  }, [mappingChunkProgress]);
  const progressDisplayPercent = normalizedMappingProgress;
  const renderProgressLabel = useCallback(
    (percent?: number) => {
      if (typeof percent === 'number' && chunkProgressLabel) {
        return `${Math.round(percent)}% • ${chunkProgressLabel}`;
      }
      if (typeof percent === 'number') {
        return `${Math.round(percent)}%`;
      }
      return chunkProgressLabel ?? undefined;
    },
    [chunkProgressLabel]
  );

  const instructionField = (
    <div style={{ width: '100%' }}>
      <Text strong>LLM instruction (optional)</Text>
      <Paragraph type="secondary" style={{ marginBottom: 8 }}>
        This note is passed to the AI for every file in this upload (including archives and workbooks).
      </Paragraph>
      <Space direction="vertical" size="small" style={{ width: '100%' }}>
        <Select
          allowClear
          showSearch
          placeholder="Select a saved instruction"
          value={selectedInstructionId || undefined}
          onChange={(value) => {
            setSelectedInstructionId(value || null);
            const selected = instructionOptions.find((option) => option.id === value);
            if (selected) {
              setLlmInstruction(selected.content);
              setInstructionTitle(selected.title);
            }
          }}
          options={instructionOptions.map((option) => ({
            value: option.id,
            label: option.title,
          }))}
          loading={loadingInstructions}
          style={{ width: '100%' }}
        />
        <TextArea
          value={llmInstruction}
          onChange={(e) => {
            setLlmInstruction(e.target.value);
            if (selectedInstructionId) {
              setSelectedInstructionId(null);
            }
          }}
          placeholder="Example: Keep phone numbers as text and do not drop rows when the address is missing."
          autoSize={{ minRows: 2, maxRows: 4 }}
        />
        <Space align="start">
          <Checkbox
            checked={saveInstruction}
            onChange={(e) => setSaveInstruction(e.target.checked)}
          >
            Save this instruction for future imports
          </Checkbox>
          {saveInstruction && (
            <Input
              value={instructionTitle}
              onChange={(e) => setInstructionTitle(e.target.value)}
              placeholder="Instruction name (e.g., Marketing Cleanup Rules)"
              style={{ minWidth: 240 }}
            />
          )}
        </Space>
        {selectedInstructionId && (
          <Space>
            <Button
              size="small"
              onClick={async () => {
                if (!selectedInstructionId) return;
                const title = instructionTitle.trim() || 'Saved import instruction';
                setInstructionActionLoading(true);
                try {
                  const token = localStorage.getItem('refine-auth');
                  await axios.patch(
                    `${API_URL}/llm-instructions/${selectedInstructionId}`,
                    { title, content: llmInstruction },
                    {
                      headers: {
                        'Content-Type': 'application/json',
                        ...(token && { Authorization: `Bearer ${token}` }),
                      },
                    }
                  );
                  messageApi.success('Instruction updated');
                  await fetchInstructions();
                } catch (err) {
                  console.error('Unable to update instruction', err);
                  messageApi.error('Unable to update instruction');
                } finally {
                  setInstructionActionLoading(false);
                }
              }}
              loading={instructionActionLoading}
              disabled={disableMappingActions}
            >
              Update selected
            </Button>
            <Button
              size="small"
              danger
              onClick={async () => {
                if (!selectedInstructionId) return;
                setInstructionActionLoading(true);
                try {
                  const token = localStorage.getItem('refine-auth');
                  await axios.delete(`${API_URL}/llm-instructions/${selectedInstructionId}`, {
                    headers: {
                      ...(token && { Authorization: `Bearer ${token}` }),
                    },
                  });
                  messageApi.success('Instruction deleted');
                  setSelectedInstructionId(null);
                  setLlmInstruction('');
                  setInstructionTitle('');
                  await fetchInstructions();
                } catch (err) {
                  console.error('Unable to delete instruction', err);
                  messageApi.error('Unable to delete instruction');
                } finally {
                  setInstructionActionLoading(false);
                }
              }}
              loading={instructionActionLoading}
              disabled={disableMappingActions}
            >
              Delete selected
            </Button>
          </Space>
        )}
      </Space>
    </div>
  );

  const showActiveJobWarning = useCallback(() => {
    messageApi.warning(
      'A mapping job is already queued or running for this file. Please wait for it to finish before starting another.'
    );
  }, [messageApi]);

  const ensureJobIsAvailable = () => {
    // Only block if there's an active job AND the file isn't in a failed state
    // This allows retries after failures even if file.status is stuck at 'mapping'
    if (mappingJobActive && !processing && file?.status !== 'failed') {
      showActiveJobWarning();
      return false;
    }
    return true;
  };

  // Mapped file details state
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [importHistory, setImportHistory] = useState<ImportHistory | null>(null);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [duplicateData, setDuplicateData] = useState<DuplicateRowsState | null>(null);
  const [loadingDuplicates, setLoadingDuplicates] = useState(false);
  const [mergeModalVisible, setMergeModalVisible] = useState(false);
  const [mergeDetail, setMergeDetail] = useState<DuplicateDetail | null>(null);
  const [mergeSelections, setMergeSelections] = useState<Record<string, boolean>>({});
  const [mergeNote, setMergeNote] = useState('');
  const [mergeDetailLoading, setMergeDetailLoading] = useState(false);
  const [selectedDuplicateRowIds, setSelectedDuplicateRowIds] = useState<number[]>([]);
  const [bulkMergeLoading, setBulkMergeLoading] = useState(false);
  const [mergeLoading, setMergeLoading] = useState(false);

  const fetchFileDetails = useCallback(async () => {
    if (!id) return;
    
    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.get(`${API_URL}/uploaded-files/${id}`, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        setFile(response.data.file);
      } else {
        setError('Failed to load file details');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Failed to load file';
      setError(errorMsg);
    } finally {
      setLoading(false);
    }
  }, [id]);

  const fetchJobDetails = useCallback(
    async (jobId: string): Promise<ImportJobInfo | null> => {
      try {
        const token = localStorage.getItem('refine-auth');
        const response = await axios.get(`${API_URL}/import-jobs/${jobId}`, {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });

        if (response.data?.success && response.data.job) {
          const job: ImportJobInfo = response.data.job;
          setJobInfo(job);
          return job;
        }
      } catch (error) {
        console.error('Failed to fetch job info', error);
      }
      return null;
    },
    []
  );

  const fetchLatestJobForFile = useCallback(
    async (fileId: string): Promise<ImportJobInfo | null> => {
      try {
        const token = localStorage.getItem('refine-auth');
        const response = await axios.get(`${API_URL}/import-jobs`, {
          params: { file_id: fileId, limit: 1 },
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });

        if (
          response.data?.success &&
          Array.isArray(response.data.jobs) &&
          response.data.jobs.length > 0
        ) {
          const job: ImportJobInfo = response.data.jobs[0];
          setJobInfo(job);
          return job;
        }
      } catch (error) {
        console.error('Failed to fetch latest job for file', error);
      }
      return null;
    },
    []
  );

  const fetchMappedFileDetails = useCallback(async (fileMeta: UploadedFile) => {
    if (!fileMeta.mapped_table_name) return;

    const tableName = fileMeta.mapped_table_name;
    setLoadingDetails(true);
    setDuplicateData(null);
    try {
      const token = localStorage.getItem('refine-auth');

      // Prefer matching import history using original file metadata so
      // duplicates/summary reflect the specific upload even when multiple
      // files target the same table.
      const importParams: Record<string, unknown> = {
        table_name: tableName,
        file_name: fileMeta.file_name,
        limit: 1,
      };
      if (typeof fileMeta.file_size === 'number') {
        importParams.file_size_bytes = fileMeta.file_size;
      }

      let historyResponse = await axios.get(`${API_URL}/import-history`, {
        params: importParams,
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (
        (!historyResponse.data.success || historyResponse.data.imports.length === 0) &&
        fileMeta.mapped_table_name
      ) {
        // Fallback to table-level lookup so we still show something even if
        // file-specific filters miss (e.g. legacy records without metadata).
        historyResponse = await axios.get(`${API_URL}/import-history`, {
          params: { table_name: tableName, limit: 1 },
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });
      }

      if (historyResponse.data.success && historyResponse.data.imports.length > 0) {
        setImportHistory(historyResponse.data.imports[0]);
        
        // Fetch table data preview filtered by import_id to show only imported rows
        const importId = historyResponse.data.imports[0].import_id;
        const tableResponse = await axios.get(`${API_URL}/tables/${tableName}`, {
          params: { limit: 10, offset: 0, import_id: importId },
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });

        if (tableResponse.data.success) {
          const rawData = tableResponse.data.data as Record<string, unknown>[];
          const dataWithKeys = rawData.map((row, index) => {
            const existingKey =
              (row.id ?? row.ID ?? row.Id ?? row.uuid ?? row.UUID) as
                | string
                | number
                | undefined;
            const key =
              existingKey !== undefined
                ? String(existingKey)
                : `${tableName}-${index}`;

            return {
              __rowKey: key,
              ...row,
            };
          });
          setTableData({
            data: dataWithKeys,
            total_rows: tableResponse.data.total_rows,
          });
        }
      } else {
        setImportHistory(null);
      }
    } catch (err) {
      console.error('Error fetching mapped file details:', err);
    } finally {
      setLoadingDetails(false);
    }
  }, []);

  const fetchDuplicateRows = useCallback(async (importId: string) => {
    setLoadingDuplicates(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.get(`${API_URL}/import-history/${importId}/duplicates`, {
        params: { limit: DUPLICATE_PREVIEW_LIMIT },
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        setDuplicateData({
          rows: response.data.duplicates as DuplicateRowData[],
          total: response.data.total_count ?? (response.data.duplicates?.length ?? 0),
        });
      } else {
        setDuplicateData(null);
      }
    } catch (err) {
      console.error('Error fetching duplicate rows:', err);
      messageApi.warning('Duplicates were detected, but we could not retrieve the preview.');
      setDuplicateData(null);
    } finally {
      setLoadingDuplicates(false);
    }
  }, [messageApi]);

  const resetMergeState = () => {
    setMergeDetail(null);
    setMergeSelections({});
    setMergeNote('');
  };

  const openMergeModal = useCallback(
    async (duplicateId: number) => {
      if (!importHistory) return;
      setMergeDetailLoading(true);
      resetMergeState();
      try {
        const token = localStorage.getItem('refine-auth');
        const response = await axios.get(
          `${API_URL}/import-history/${importHistory.import_id}/duplicates/${duplicateId}`,
          {
            headers: {
              ...(token && { Authorization: `Bearer ${token}` }),
            },
          }
        );

        if (response.data.success) {
          const detailData = response.data as {
            duplicate: DuplicateRowData;
            existing_row: DuplicateExistingRow | null;
            uniqueness_columns: string[];
          };

          const defaultSelections: Record<string, boolean> = {};
          const duplicateRecord = detailData.duplicate.record;
          const existingRecord = detailData.existing_row?.record ?? {};
          Object.keys(duplicateRecord).forEach((column) => {
            if (column.startsWith('_')) {
              defaultSelections[column] = false;
              return;
            }
            const incomingValue = duplicateRecord[column];
            const existingValue = existingRecord[column];
            defaultSelections[column] =
              existingValue !== incomingValue &&
              !(existingValue === null && incomingValue === undefined);
          });

          setMergeDetail({
            duplicate: detailData.duplicate,
            existing_row: detailData.existing_row,
            uniqueness_columns: detailData.uniqueness_columns,
          });
          setMergeSelections(defaultSelections);
          setMergeModalVisible(true);
        } else {
          messageApi.error('Failed to load duplicate details');
        }
      } catch (error) {
        console.error('Error loading duplicate detail:', error);
        messageApi.error('Failed to load duplicate details');
      } finally {
        setMergeDetailLoading(false);
      }
    },
    [importHistory, messageApi]
  );

  const handleMergeSelectionChange = (column: string, checked: boolean) => {
    setMergeSelections((prev) => ({
      ...prev,
      [column]: checked,
    }));
  };

  const handleMergeSubmit = async () => {
    if (!mergeDetail || !importHistory) return;
    const selectedUpdates: Record<string, unknown> = {};
    Object.entries(mergeSelections).forEach(([column, useIncoming]) => {
      if (useIncoming) {
        selectedUpdates[column] = mergeDetail.duplicate.record[column];
      }
    });

    setMergeLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/import-history/${importHistory.import_id}/duplicates/${mergeDetail.duplicate.id}/merge`,
        {
          updates: selectedUpdates,
          note: mergeNote || undefined,
        },
        {
          headers: {
            'Content-Type': 'application/json',
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      if (response.data.success) {
        messageApi.success('Duplicate merged successfully');
        setMergeModalVisible(false);
        resetMergeState();
        if (file) {
          await fetchMappedFileDetails(file);
        } else if (importHistory.import_id) {
          await fetchDuplicateRows(importHistory.import_id);
        }
      } else {
        messageApi.error('Failed to merge duplicate');
      }
    } catch (error) {
      console.error('Error merging duplicate:', error);
      messageApi.error('Failed to merge duplicate');
    } finally {
      setMergeLoading(false);
    }
  };

  const handleSelectAllDuplicates = useCallback(() => {
    if (!duplicateData?.rows) return;
    const selectableIds = duplicateData.rows
      .filter((row) => typeof row.id === 'number' && !row.resolved_at)
      .map((row) => row.id as number);
    setSelectedDuplicateRowIds(selectableIds);
  }, [duplicateData]);

  const handleClearDuplicateSelection = useCallback(() => {
    setSelectedDuplicateRowIds([]);
  }, []);

  const handleBulkDuplicateMerge = useCallback(async () => {
    if (!importHistory || selectedDuplicateRowIds.length === 0 || !duplicateData?.rows) return;

    setBulkMergeLoading(true);
    const token = localStorage.getItem('refine-auth');
    let successCount = 0;

    for (const duplicateId of selectedDuplicateRowIds) {
      const row = duplicateData.rows.find(
        (duplicate) => duplicate.id === duplicateId && !duplicate.resolved_at
      );
      if (!row) {
        continue;
      }

      const updates: Record<string, unknown> = {};
      Object.entries(row.record || {}).forEach(([column, value]) => {
        if (!column.startsWith('_')) {
          updates[column] = value;
        }
      });

      if (Object.keys(updates).length === 0) {
        continue;
      }

      try {
        await axios.post(
          `${API_URL}/import-history/${importHistory.import_id}/duplicates/${duplicateId}/merge`,
          { updates },
          {
            headers: {
              'Content-Type': 'application/json',
              ...(token && { Authorization: `Bearer ${token}` }),
            },
          }
        );
        successCount += 1;
      } catch (error) {
        console.error('Error merging duplicate:', error);
        messageApi.error(
          `Failed to map duplicate ${row.record_number ?? duplicateId}. Aborting remaining merges.`
        );
        break;
      }
    }

    if (successCount > 0) {
      messageApi.success(
        `Mapped ${successCount} duplicate${successCount > 1 ? 's' : ''} successfully`
      );
      if (file) {
        await fetchMappedFileDetails(file);
      }
      await fetchDuplicateRows(importHistory.import_id);
    }

    setSelectedDuplicateRowIds([]);
    setBulkMergeLoading(false);
  }, [
    duplicateData,
    fetchDuplicateRows,
    fetchMappedFileDetails,
    file,
    importHistory,
    messageApi,
    selectedDuplicateRowIds,
  ]);

  useEffect(() => {
    fetchFileDetails();
  }, [fetchFileDetails]);

  useEffect(() => {
    void fetchInstructions();
  }, [fetchInstructions]);

  const fetchExistingTables = useCallback(async () => {
    setLoadingTables(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.get<{ success: boolean; tables: Array<{ table_name: string; row_count: number }> }>(
        `${API_URL}/tables`,
        {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );
      if (response.data?.success && Array.isArray(response.data.tables)) {
        setExistingTables(response.data.tables);
      }
    } catch (error) {
      console.error('Failed to load existing tables', error);
    } finally {
      setLoadingTables(false);
    }
  }, []);

  useEffect(() => {
    void fetchExistingTables();
  }, [fetchExistingTables]);

  useEffect(() => {
    const loadSheets = async () => {
      if (!file?.id || !isExcelFile) {
        setSheetNames([]);
        setSelectedSheets([]);
        setInteractiveSheet(undefined);
        return;
      }

      try {
        const token = localStorage.getItem('refine-auth');
        const response = await axios.get<{ success: boolean; sheets: string[] }>(
          `${API_URL}/workbooks/${file.id}/sheets`,
          {
            headers: {
              ...(token && { Authorization: `Bearer ${token}` }),
            },
          }
        );
        if (response.data?.success) {
          setSheetNames(response.data.sheets);
          setSelectedSheets(response.data.sheets);
          setInteractiveSheet((prev) => prev ?? response.data.sheets[0]);
        }
      } catch (err) {
        const error = err as AxiosError<{ detail?: string }>;
        const msg = error.response?.data?.detail || error.message || 'Unable to load workbook sheets';
        messageApi.warning(msg);
        setSheetNames([]);
        setSelectedSheets([]);
        setInteractiveSheet(undefined);
      }
    };

    void loadSheets();
  }, [file?.id, isExcelFile, messageApi]);

  // Reset result state when file details are fetched and file is mapped
  useEffect(() => {
    if (file?.status === 'mapped' && result) {
      setResult(null);
    }
  }, [file, result]);

  useEffect(() => {
    if (file?.status === 'mapped' && file.mapped_table_name) {
      fetchMappedFileDetails(file);
    }
  }, [file, fetchMappedFileDetails]);

  useEffect(() => {
    if (importHistory?.import_id && (importHistory.duplicates_found ?? 0) > 0) {
      fetchDuplicateRows(importHistory.import_id);
    } else {
      setDuplicateData(null);
    }
  }, [importHistory?.import_id, importHistory?.duplicates_found, fetchDuplicateRows]);

  useEffect(() => {
    setSelectedDuplicateRowIds([]);
  }, [duplicateData]);

  useEffect(() => {
    setArchiveResult(null);
  }, [id]);

  useEffect(() => {
    if (!file?.id || !isArchiveFile) {
      setArchiveHistorySummary(null);
      return;
    }

    const allowFetch = file.status === 'mapped' || file.status === 'failed';
    if (archiveResult || !allowFetch) {
      setArchiveHistorySummary(null);
      return;
    }

    let cancelled = false;

    const fetchArchiveSummary = async () => {
      try {
        const token = localStorage.getItem('refine-auth');
        const response = await axios.get(`${API_URL}/import-jobs`, {
          params: { file_id: file.id, limit: 1 },
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });

        if (
          !response.data?.success ||
          !Array.isArray(response.data.jobs) ||
          response.data.jobs.length === 0
        ) {
          if (!cancelled) {
            setArchiveHistorySummary(null);
          }
          return;
        }

        const job: ImportJobInfo = response.data.jobs[0];
        const metadata = job.result_metadata as ArchiveResultMetadata | null;
        const resultList = Array.isArray(metadata?.results)
          ? (metadata?.results as ArchiveFileResult[])
          : [];

        if (resultList.length === 0) {
          if (!cancelled) {
            setArchiveHistorySummary(null);
          }
          return;
        }

        const processedFiles =
          typeof metadata?.processed_files === 'number'
            ? metadata.processed_files
            : resultList.filter((entry) => entry.status === 'processed').length;
        const failedFiles =
          typeof metadata?.failed_files === 'number'
            ? metadata.failed_files
            : resultList.filter((entry) => entry.status === 'failed').length;
        const skippedFiles =
          typeof metadata?.skipped_files === 'number'
            ? metadata.skipped_files
            : resultList.filter((entry) => entry.status === 'skipped').length;
        const supportedFiles =
          typeof metadata?.files_total === 'number' ? metadata.files_total : resultList.length;

        const normalizedResult: ArchiveAutoProcessResult = {
          success: job.status === 'succeeded' && failedFiles === 0,
          total_files: supportedFiles + skippedFiles,
          processed_files: processedFiles,
          failed_files: failedFiles,
          skipped_files: skippedFiles,
          results: resultList,
          job_id: job.id,
        };

        if (!cancelled) {
          setArchiveHistorySummary({ job, result: normalizedResult });
        }
      } catch (summaryError) {
        console.error('Failed to load archive summary', summaryError);
        if (!cancelled) {
          setArchiveHistorySummary(null);
        }
      }
    };

    fetchArchiveSummary();

    return () => {
      cancelled = true;
    };
  }, [archiveResult, file?.id, file?.status, isArchiveFile]);

  useEffect(() => {
    if (!file?.id) {
      return;
    }

    if (file.active_job_id) {
      return;
    }

    if (
      file.status === 'mapping' ||
      (isArchiveFile &&
        (file.status === 'mapped' || file.status === 'failed'))
    ) {
      fetchLatestJobForFile(file.id);
    }
  }, [fetchLatestJobForFile, file?.active_job_id, file?.id, file?.status, isArchiveFile]);

  useEffect(() => {
    if (!isArchiveFile) {
      setArchiveJobDetails(null);
      return;
    }

    if (archiveHistorySummary?.job) {
      setArchiveJobDetails(archiveHistorySummary.job);
      return;
    }

    if (!archiveResult?.job_id) {
      setArchiveJobDetails(null);
      return;
    }

    let cancelled = false;

    const fetchJobDetailsOnce = async () => {
      try {
        const token = localStorage.getItem('refine-auth');
        const response = await axios.get(`${API_URL}/import-jobs/${archiveResult.job_id}`, {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });

        if (!cancelled && response.data?.success) {
          setArchiveJobDetails(response.data.job);
        }
      } catch (jobError) {
        console.error('Failed to load archive job details', jobError);
        if (!cancelled) {
          setArchiveJobDetails(null);
        }
      }
    };

    fetchJobDetailsOnce();

    return () => {
      cancelled = true;
    };
  }, [archiveHistorySummary?.job, archiveResult?.job_id, isArchiveFile]);

  useEffect(() => {
    if (file && file.status !== 'failed' && showInteractiveRetry) {
      setShowInteractiveRetry(false);
    }
  }, [file, showInteractiveRetry]);

  useEffect(() => {
    if (!file?.active_job_id) {
      setJobInfo(null);
      return;
    }

    let cancelled = false;

    const pollJob = async () => {
      const job = await fetchJobDetails(file.active_job_id!);
      if (!job || cancelled) {
        return;
      }

      if (job.status === 'succeeded' || job.status === 'failed') {
        clearInterval(intervalId);
        await fetchFileDetails();
      }
    };

    const intervalId = setInterval(pollJob, 5000);
    void pollJob();

    return () => {
      cancelled = true;
      clearInterval(intervalId);
    };
  }, [file?.active_job_id, fetchJobDetails, fetchFileDetails]);

  const appendSharedTableFormData = (formData: FormData) => {
    if (useSharedTable && sharedTableName.trim()) {
      formData.append('target_table_name', sharedTableName.trim());
      formData.append('target_table_mode', sharedTableMode);
    }
    if (skipFileDuplicateCheck) {
      formData.append('skip_file_duplicate_check', 'true');
    }
  };

  const appendInstructionFormData = (formData: FormData) => {
    const instruction = llmInstruction.trim();
    const title = instructionTitle.trim();
    if (instruction) {
      formData.append('llm_instruction', instruction);
    }
    if (selectedInstructionId) {
      formData.append('llm_instruction_id', selectedInstructionId);
    }
    if (saveInstruction && instruction) {
      formData.append('save_llm_instruction', 'true');
      if (title) {
        formData.append('llm_instruction_title', title);
      }
    }
  };

  const appendInstructionPayload = useCallback(
    (payload: Record<string, unknown>) => {
      const instruction = llmInstruction.trim();
      const title = instructionTitle.trim();
      if (instruction) {
        payload.llm_instruction = instruction;
      }
      if (selectedInstructionId) {
        payload.llm_instruction_id = selectedInstructionId;
      }
      if (saveInstruction && instruction) {
        payload.save_llm_instruction = true;
        if (title) {
          payload.llm_instruction_title = title;
        }
      }
    },
    [instructionTitle, llmInstruction, saveInstruction, selectedInstructionId]
  );

  const attemptAutoRecoveryWithLLM = useCallback(
    async (failureMessage: string): Promise<AutoRecoveryOutcome> => {
      if (!id) {
        return {
          recovered: false,
          reason: 'analysis_failed',
          errorMessage: failureMessage,
        };
      }

      const token = localStorage.getItem('refine-auth');
      messageApi.info('Auto Process failed. Asking the AI assistant for a fix...');

      try {
        const payload: Record<string, unknown> = {
          file_id: id,
          max_iterations: 5,
          previous_error_message: failureMessage,
        };
        appendInstructionPayload(payload);

        const analysisResponse = await axios.post(
          `${API_URL}/analyze-file-interactive`,
          payload,
          {
            headers: {
              'Content-Type': 'application/json',
              ...(token && { Authorization: `Bearer ${token}` }),
            },
          }
        );

        if (!analysisResponse.data.success) {
          const fallbackMessage =
            analysisResponse.data.error || 'Interactive recovery failed';
          messageApi.error(fallbackMessage);
          return {
            recovered: false,
            reason: 'analysis_failed',
            errorMessage: fallbackMessage,
          };
        }

        if (!analysisResponse.data.can_execute) {
          messageApi.warning(
            'The AI assistant could not determine an automatic recovery plan.'
          );
          return { recovered: false, reason: 'no_plan' };
        }

        messageApi.info('AI assistant proposed a fix. Executing automatically...');

        const executeResponse = await axios.post(
          `${API_URL}/execute-interactive-import`,
          {
            file_id: id,
            thread_id: analysisResponse.data.thread_id,
          },
          {
            headers: {
              'Content-Type': 'application/json',
              ...(token && { Authorization: `Bearer ${token}` }),
            },
          }
        );

        if (executeResponse.data.success) {
          setResult({
            success: true,
            table_name: executeResponse.data.table_name,
            rows_imported: executeResponse.data.rows_imported,
            execution_time: executeResponse.data.execution_time,
          });
          await fetchFileDetails();
          messageApi.success('AI recovery completed the import.');
          return { recovered: true };
        }

        const executionMessage =
          executeResponse.data.message || 'AI recovery execution failed';
        messageApi.error(executionMessage);
        return {
          recovered: false,
          reason: 'execution_failed',
          errorMessage: executionMessage,
        };
      } catch (err) {
        const error = err as AxiosError<{ detail?: string }>;
        const errorMsg =
          error.response?.data?.detail || error.message || 'AI recovery failed';
        messageApi.error(errorMsg);
        return {
          recovered: false,
          reason: 'exception',
          errorMessage: errorMsg,
        };
      }
    },
    [appendInstructionPayload, fetchFileDetails, id, messageApi]
  );

  const validateSharedTableName = () => {
    if (useSharedTable && !sharedTableName.trim()) {
      messageApi.error('Enter a table name to reuse for all mapped files.');
      return false;
    }
    return true;
  };

  const handleAutoProcess = async () => {
    if (!id) return;
    if (!ensureJobIsAvailable()) return;
    if (!validateSharedTableName()) return;
    
    setProcessing(true);
    setError(null);
    setResult(null);
    setArchiveResult(null);
    let autoError: string | null = null;

    try {
      const token = localStorage.getItem('refine-auth');
      if (hasMultipleSheets) {
        const formData = new FormData();
        formData.append('file_id', id);
        formData.append('analysis_mode', 'auto_always');
        formData.append('conflict_resolution', 'llm_decide');
        formData.append('max_iterations', '5');
        if (selectedSheets.length && selectedSheets.length < sheetNames.length) {
          formData.append('sheet_names', JSON.stringify(selectedSheets));
        }
        appendSharedTableFormData(formData);
        appendInstructionFormData(formData);

        const response = await axios.post(`${API_URL}/auto-process-workbook`, formData, {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });

        const payload: ArchiveAutoProcessResult = response.data;
        if (payload.job_id) {
          const job = await fetchJobDetails(payload.job_id);
          if (job) {
            setArchiveJobDetails(job);
          }
          messageApi.success('Workbook processing started in the background.');
        } else {
          messageApi.warning('Workbook processing started, but job tracking is unavailable.');
        }
        await fetchFileDetails();
        setProcessing(false);
        return;
      }

      const formData = new FormData();
      formData.append('file_id', id);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');
      if (skipFileDuplicateCheck) {
        formData.append('skip_file_duplicate_check', 'true');
      }
      appendSharedTableFormData(formData);
      appendInstructionFormData(formData);

      const response = await axios.post(`${API_URL}/analyze-file`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        setResult({
          success: true,
          table_name: response.data.table_name,
          rows_imported: response.data.rows_imported,
          execution_time: response.data.execution_time,
        });
        // Refetch file details to get updated status and trigger detailed view
        await fetchFileDetails();
        setProcessing(false);
        return;
      } else {
        autoError = response.data.error || 'Processing failed';
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Processing failed';
      autoError = errorMsg;
    }

    if (autoError) {
      const recoveryOutcome = await attemptAutoRecoveryWithLLM(autoError);
      if (recoveryOutcome.recovered) {
        setProcessing(false);
        return;
      }

      const recoverySuffix =
        recoveryOutcome.reason === 'no_plan'
          ? ' The AI assistant could not determine a recovery plan.'
          : recoveryOutcome.errorMessage
            ? ` AI recovery attempt failed: ${recoveryOutcome.errorMessage}`
            : '';

      setResult({
        success: false,
        error: `${autoError}${recoverySuffix}`,
      });
    }

    setProcessing(false);
  };

  const handleArchiveAutoProcess = async () => {
    if (!id) {
      return;
    }
    if (!ensureJobIsAvailable()) {
      return;
    }
    if (!validateSharedTableName()) {
      return;
    }
    setArchiveProcessing(true);
    setProcessing(false);
    setResult(null);
    setError(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const formData = new FormData();
      formData.append('file_id', id);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');
      appendSharedTableFormData(formData);
      appendInstructionFormData(formData);

      const response = await axios.post(`${API_URL}/auto-process-archive`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      const payload: ArchiveAutoProcessResult = response.data;
      setArchiveResult(null);
      setArchiveHistorySummary(null);

      if (payload.job_id) {
        const job = await fetchJobDetails(payload.job_id);
        if (job) {
          setArchiveJobDetails(job);
        }
        messageApi.success('Archive processing started. You can close this page and come back later.');
      } else {
        messageApi.warning('Archive processing started, but job tracking is unavailable.');
      }
      await fetchFileDetails();
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Archive processing failed';
      messageApi.error(errorMsg);
    } finally {
      setArchiveProcessing(false);
    }
  };

  const handleArchiveResume = async (options?: { resumeAll?: boolean }) => {
    if (!id) {
      return;
    }
    if (!ensureJobIsAvailable()) {
      return;
    }
    if (!validateSharedTableName()) {
      return;
    }

    const sourceJobId = archiveJobDetails?.id ?? archiveHistorySummary?.job?.id ?? jobInfo?.id;
    if (!sourceJobId) {
      messageApi.error('No previous archive job found to resume.');
      return;
    }

    setArchiveResumeLoading(true);
    setProcessing(false);
    setResult(null);
    setError(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const formData = new FormData();
      formData.append('file_id', id);
      formData.append('from_job_id', sourceJobId);
      formData.append('resume_failed_entries_only', options?.resumeAll ? 'false' : 'true');
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');
      appendSharedTableFormData(formData);
      appendInstructionFormData(formData);

      const response = await axios.post(`${API_URL}/auto-process-archive/resume`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      const payload: ArchiveAutoProcessResult = response.data;
      setArchiveResult(null);
      setArchiveHistorySummary(null);

      if (payload.job_id) {
        const job = await fetchJobDetails(payload.job_id);
        if (job) {
          setArchiveJobDetails(job);
        }
        messageApi.success(
          options?.resumeAll
            ? 'Archive reprocessing started. We will rebuild every supported file.'
            : 'Retrying failed archive files. Stay on this page for updates.'
        );
      } else {
        messageApi.warning('Archive resume started, but job tracking is unavailable.');
      }

      await fetchFileDetails();
      setActiveTab('auto');
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Resume failed';
      messageApi.error(errorMsg);
    } finally {
      setArchiveResumeLoading(false);
    }
  };

  const handleInteractiveStart = async (options?: { previousError?: string }) => {
    if (!id) return;
    if (!ensureJobIsAvailable()) return;
    
    setProcessing(true);
    setError(null);
    setConversation([]);
    setNeedsUserInput(true);
    setResult(null);
    setThreadId(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const payload: Record<string, unknown> = {
        file_id: id,
        max_iterations: 5,
      };
      if (interactiveSheet) {
        payload.sheet_name = interactiveSheet;
      } else if (hasMultipleSheets && sheetNames.length > 0) {
        payload.sheet_name = sheetNames[0];
        setInteractiveSheet(sheetNames[0]);
      }

      if (options?.previousError) {
        payload.previous_error_message = options.previousError;
      }
      appendInstructionPayload(payload);

      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      if (response.data.success) {
        setThreadId(response.data.thread_id);
        setConversation([
          { role: 'assistant', content: response.data.llm_message },
        ]);
        setCanExecute(response.data.can_execute);
        setNeedsUserInput(response.data.needs_user_input ?? true);
      } else {
        setError(response.data.error || 'Analysis failed');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Analysis failed';
      setError(errorMsg);
      messageApi.error(formatUserFacingError(errorMsg).summary);
    } finally {
      setProcessing(false);
    }
  };

  const handleRetryInteractive = async () => {
    if (!id || processing) return;

    // Switch to interactive tab instead of opening a modal
    setActiveTab('interactive');
    setShowInteractiveRetry(true);

    const cleanedError = file?.error_message?.trim();
    await handleInteractiveStart({
      previousError: cleanedError && cleanedError.length > 0 ? cleanedError : undefined,
    });
  };

  const sendInteractiveMessage = async (messageToSend: string) => {
    if (!threadId || !id) return;
    const trimmed = messageToSend.trim();
    if (!trimmed) return;

    setProcessing(true);
    setError(null);

    setConversation((prev) => [...prev, { role: 'user', content: trimmed }]);
    setUserInput('');

    try {
      const token = localStorage.getItem('refine-auth');
      const payload: Record<string, unknown> = {
        file_id: id,
        user_message: trimmed,
        thread_id: threadId,
        max_iterations: 5,
      };
      appendInstructionPayload(payload);

      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      if (response.data.success) {
        setConversation((prev) => [
          ...prev,
          { role: 'assistant', content: response.data.llm_message },
        ]);
        setCanExecute(response.data.can_execute);
        setNeedsUserInput(response.data.needs_user_input ?? true);
      } else {
        const fallback = response.data.error || 'Analysis failed';
        setError(fallback);
        messageApi.error(formatUserFacingError(fallback).summary);
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Analysis failed';
      setError(errorMsg);
      messageApi.error(formatUserFacingError(errorMsg).summary);
    } finally {
      setProcessing(false);
    }
  };

  const handleInteractiveSend = async () => {
    if (!userInput.trim()) return;
    await sendInteractiveMessage(userInput);
  };

  const handleQuickAction = async (prompt: string) => {
    if (!prompt || processing) return;
    await sendInteractiveMessage(prompt);
  };

  const handleInteractiveExecute = async () => {
    if (!threadId || !id) return;

    setProcessing(true);
    setError(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/execute-interactive-import`,
        {
          file_id: id,
          thread_id: threadId,
        },
        {
          headers: {
            'Content-Type': 'application/json',
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      if (response.data.success) {
        setConversation((prev) => [
          ...prev,
          {
            role: 'assistant',
            content: `✅ Import executed successfully into ${response.data.table_name}.`,
          },
        ]);
        setResult({
          success: true,
          table_name: response.data.table_name,
          rows_imported: response.data.rows_imported,
          execution_time: response.data.execution_time,
        });
        // Refetch file details to get updated status and trigger detailed view
        await fetchFileDetails();
        setCanExecute(false);
        setNeedsUserInput(false);
        setThreadId(null);
      } else {
        const failureMessage = response.data.message || 'Import execution failed';
        setError(failureMessage);
        setConversation((prev) => {
          const next: Array<{ role: 'user' | 'assistant'; content: string }> = [
            ...prev,
            { role: 'assistant', content: `⚠️ ${failureMessage}` },
          ];
          if (response.data.llm_followup) {
            next.push({ role: 'assistant', content: response.data.llm_followup });
          }
          return next;
        });
        setCanExecute(response.data.can_execute ?? false);
        setNeedsUserInput(response.data.needs_user_input ?? true);
        if (response.data.thread_id) {
          setThreadId(response.data.thread_id);
        }
        messageApi.error(formatUserFacingError(failureMessage).summary);
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Import execution failed';
      setError(errorMsg);
      messageApi.error(formatUserFacingError(errorMsg).summary);
    } finally {
      setProcessing(false);
    }
  };

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const formatDate = (dateString?: string): string => {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString();
  };

  const renderDuplicateValue = (value: unknown): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Text type="secondary">-</Text>;
    }
    if (Array.isArray(value) || typeof value === 'object') {
      try {
        const asJson = JSON.stringify(value);
        return (
          <Text code style={{ maxWidth: 220 }} ellipsis={{ tooltip: asJson }}>
            {asJson}
          </Text>
        );
      } catch (err) {
        console.error('Failed to render duplicate value as JSON', err);
        return String(value);
      }
    }
    const textValue = String(value);
    return (
      <Text style={{ maxWidth: 200 }} ellipsis={{ tooltip: textValue }}>
        {textValue}
      </Text>
    );
  };

  const renderMappedFileView = () => {
    if (!file || file.status !== 'mapped') return null;

    if (isArchiveFile) {
      const summaryResult = effectiveArchiveResult;
      const jobId = archiveJobDetails?.id ?? archiveResult?.job_id ?? null;
      const jobSource = archiveJobDetails?.trigger_source || 'Auto Process Archive';
      const jobCompletedAt = archiveJobDetails?.completed_at ?? file.mapped_date;
      const filesInArchiveMeta =
        archiveJobDetails?.metadata && typeof archiveJobDetails.metadata['files_in_archive'] === 'number'
          ? (archiveJobDetails.metadata['files_in_archive'] as number)
          : undefined;
      const filesInArchiveCount = filesInArchiveMeta ?? summaryResult?.total_files;
      const summaryTagColor = summaryResult
        ? summaryResult.failed_files > 0
          ? 'orange'
          : 'green'
        : 'default';
      const summaryTagText = summaryResult
        ? summaryResult.failed_files > 0
          ? 'Completed with warnings'
          : 'Completed'
        : 'Awaiting summary';

      const archiveSummaryItems: DescriptionsProps['items'] = [
        {
          key: 'archive-name',
          label: 'Archive',
          children: <Text>{file.file_name}</Text>,
        },
        {
          key: 'file-size',
          label: 'File Size',
          children: formatBytes(file.file_size),
        },
        {
          key: 'uploaded',
          label: 'Uploaded',
          children: formatDate(file.upload_date),
        },
        {
          key: 'last-processed',
          label: 'Last Processed',
          children: formatDate(jobCompletedAt),
        },
        {
          key: 'job-id',
          label: 'Import Job',
          children: jobId ? <Text code>{jobId}</Text> : '-',
        },
        {
          key: 'trigger',
          label: 'Trigger Source',
          children: jobSource,
        },
        {
          key: 'files-total',
          label: 'Files in Archive',
          children:
            typeof filesInArchiveCount === 'number'
              ? filesInArchiveCount.toLocaleString()
              : '-',
        },
        {
          key: 'status',
          label: 'Status',
          children: <Tag color={summaryTagColor}>{summaryTagText}</Tag>,
        },
      ];

      const archiveAlertType =
        summaryResult && summaryResult.failed_files > 0
          ? 'warning'
          : summaryResult
            ? 'success'
            : 'info';
      const archiveAlertDescription = summaryResult
        ? summaryResult.failed_files > 0
          ? 'Some files in this archive failed to import. Review the table below for details.'
          : 'All supported files in this archive were imported successfully.'
        : 'We could not find a previous auto-process summary for this archive.';

      return (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <Descriptions
            title="Archive Details"
            bordered
            size="middle"
            column={2}
            items={archiveSummaryItems}
          />
          <Alert
            type={archiveAlertType}
            message="Archive Import Summary"
            description={archiveAlertDescription}
            showIcon
          />
          {summaryResult ? (
            <>
              <Row gutter={16}>
                <Col span={6}>
                  <Statistic title="Processed" value={summaryResult.processed_files} />
                </Col>
                <Col span={6}>
                  <Statistic title="Failed" value={summaryResult.failed_files} />
                </Col>
                <Col span={6}>
                  <Statistic title="Skipped" value={summaryResult.skipped_files} />
                </Col>
                <Col span={6}>
                  <Statistic title="Total Files" value={summaryResult.total_files} />
                </Col>
              </Row>
              {archiveAggregates && (
                <Row gutter={16} style={{ marginTop: 16 }}>
                  <Col span={8}>
                    <Statistic title="Rows Inserted" value={archiveAggregates.totalRecords} />
                  </Col>
                  <Col span={8}>
                    <Statistic
                      title="Duplicates Skipped"
                      value={archiveAggregates.totalDuplicates}
                    />
                  </Col>
                  <Col span={8}>
                    <Statistic title="Tables Updated" value={archiveAggregates.tablesTouched} />
                  </Col>
                </Row>
              )}
              <Card
                title="Files in Archive"
                size="small"
                style={{ marginTop: 16 }}
              >
                <Table
                  dataSource={archiveResultRows}
                  columns={archiveResultsColumns}
                  pagination={false}
                  size="small"
                  scroll={{ x: 'max-content' }}
                />
              </Card>
            </>
          ) : (
            <Alert
              type="warning"
              showIcon
              message="Archive summary not available"
              description="This ZIP was marked as mapped, but we couldn't locate a completed Auto Process Archive job. Run Auto Process Archive again to rebuild the summary."
            />
          )}
        </Space>
      );
    }

    const summaryItems: DescriptionsProps['items'] = [
      {
        key: 'table-name',
        label: 'Table Name',
        children: <Tag color="blue">{file.mapped_table_name}</Tag>,
      },
      {
        key: 'mapped-date',
        label: 'Mapped Date',
        children: formatDate(file.mapped_date),
      },
      {
        key: 'rows-imported',
        label: 'Rows Imported',
        children: <Text strong>{file.mapped_rows?.toLocaleString() || 0}</Text>,
      },
      {
        key: 'file-size',
        label: 'File Size',
        children: formatBytes(file.file_size),
      },
      {
        key: 'upload-date',
        label: 'Upload Date',
        children: formatDate(file.upload_date),
      },
      {
        key: 'status',
        label: 'Status',
        children: <Tag color="success">Mapped</Tag>,
      },
    ];

    const historyItems: DescriptionsProps['items'] = importHistory
      ? [
          ...(importHistory.import_strategy
            ? [
                {
                  key: 'import-strategy',
                  label: 'Import Strategy',
                  children: <Tag>{importHistory.import_strategy}</Tag>,
                  span: 2,
                } as const,
              ]
            : []),
          {
            key: 'total-rows',
            label: 'Total Rows in File',
            children:
              importHistory.total_rows_in_file?.toLocaleString() || '-',
          },
          {
            key: 'rows-inserted',
            label: 'Rows Inserted',
            children: importHistory.rows_inserted?.toLocaleString() || '-',
          },
          ...(importHistory.duplicates_found !== undefined &&
          importHistory.duplicates_found > 0
            ? [
                {
                  key: 'duplicates-found',
                  label: 'Duplicates Found',
                  children: (
                    <Text type="warning">
                      {importHistory.duplicates_found.toLocaleString()}
                    </Text>
                  ),
                  span: 2,
                } as const,
              ]
            : []),
          ...(importHistory.data_validation_errors !== undefined &&
          importHistory.data_validation_errors > 0
            ? [
                {
                  key: 'validation-errors',
                  label: 'Validation Errors',
                  children: (
                    <Text type="danger">
                      {importHistory.data_validation_errors.toLocaleString()}
                    </Text>
                  ),
                  span: 2,
                } as const,
              ]
            : []),
          ...(importHistory.duration_seconds
            ? [
                {
                  key: 'processing-time',
                  label: 'Processing Time',
                  children: `${importHistory.duration_seconds.toFixed(2)}s`,
                  span: 2,
                } as const,
              ]
            : []),
          {
            key: 'import-id',
            label: 'Import ID',
            children: (
              <Text code style={{ fontSize: '11px' }}>
                {importHistory.import_id}
              </Text>
            ),
            span: 2,
          },
        ]
      : [];

    const duplicateRows = duplicateData?.rows ?? [];
    const duplicateKeys = new Set<string>();
    duplicateRows.forEach((row) => {
      Object.keys(row.record || {}).forEach((key) => {
        if (!key.startsWith('_')) {
          duplicateKeys.add(key);
        }
      });
    });

    const resolvedDuplicateIds = new Set(
      duplicateRows
        .filter((row) => typeof row.id === 'number' && !!row.resolved_at)
        .map((row) => row.id as number)
    );
    const selectableDuplicateIds = duplicateRows
      .filter((row) => typeof row.id === 'number' && !row.resolved_at)
      .map((row) => row.id as number);

    const duplicateTableData = duplicateRows.map((row, index) => ({
      key: row.id ?? `duplicate-${index}`,
      duplicate_id: row.id,
      record_number: row.record_number ?? '-',
      detected_at: row.detected_at,
      record: row.record || {},
    }));

    const duplicateTableColumns =
      duplicateRows.length > 0
        ? [
            {
              title: 'Actions',
              key: 'actions',
              fixed: 'left' as const,
              width: 120,
              render: (_: unknown, row: (typeof duplicateTableData)[number]) => (
                <Button
                  type="link"
                  icon={<MergeCellsOutlined />}
                  onClick={() => openMergeModal(row.duplicate_id)}
                >
                  Merge
                </Button>
              ),
            },
            {
              title: '#',
              dataIndex: 'record_number',
              key: 'record_number',
              width: 70,
            },
            ...Array.from(duplicateKeys).map((key) => ({
              title: key,
              key,
              ellipsis: true,
              width: 180,
              render: (_: unknown, row: (typeof duplicateTableData)[number]) =>
                renderDuplicateValue(row.record?.[key]),
            })),
            {
              title: 'Detected At',
              dataIndex: 'detected_at',
              key: 'detected_at',
              width: 200,
              render: (value: string | null | undefined) =>
                value ? formatDate(value) : '-',
            },
          ]
        : [];

    const duplicateRowSelection =
      duplicateTableColumns.length > 0
        ? {
            selectedRowKeys: duplicateTableData
              .filter(
                (row) =>
                  typeof row.duplicate_id === 'number' &&
                  selectedDuplicateRowIds.includes(row.duplicate_id)
              )
              .map((row) => row.key),
            onChange: (
              _selectedRowKeys: React.Key[],
              selectedRows: (typeof duplicateTableData)[number][]
            ) => {
              const ids = selectedRows
                .map((row) => row.duplicate_id)
                .filter((id): id is number => typeof id === 'number');
              setSelectedDuplicateRowIds(ids);
            },
            getCheckboxProps: (record: (typeof duplicateTableData)[number]) => ({
              disabled:
                !record.duplicate_id ||
                resolvedDuplicateIds.has(record.duplicate_id) ||
                bulkMergeLoading,
            }),
          }
        : undefined;

    const duplicatesTotal = duplicateData?.total ?? importHistory?.duplicates_found ?? 0;

    return (
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Alert
          message="File Already Mapped"
          description="This file has been successfully imported into the database. View the details below."
          type="success"
          showIcon
          icon={<CheckCircleOutlined />}
        />

        {/* Import Summary */}
        <Card title={<><InfoCircleOutlined /> Import Summary</>} size="small">
          <Descriptions column={2} bordered size="small" items={summaryItems} />
        </Card>

        {/* Import Details */}
        {importHistory && (
          <Card title={<><DatabaseOutlined /> Import Details</>} size="small" loading={loadingDetails}>
            <Descriptions
              column={2}
              bordered
              size="small"
              items={historyItems}
            />
          </Card>
        )}

        {duplicatesTotal > 0 && (
          <Card
            title={
              <>
                <InfoCircleOutlined /> Duplicate Rows Skipped
              </>
            }
            size="small"
            loading={loadingDuplicates}
          >
            {duplicateTableColumns.length > 0 ? (
              <>
                <Space style={{ marginBottom: 12 }} wrap>
                  <Button
                    onClick={handleSelectAllDuplicates}
                    disabled={selectableDuplicateIds.length === 0 || bulkMergeLoading}
                  >
                    Select All
                  </Button>
                  <Button
                    onClick={handleClearDuplicateSelection}
                    disabled={selectedDuplicateRowIds.length === 0 || bulkMergeLoading}
                  >
                    Clear Selection
                  </Button>
                  <Button
                    type="primary"
                    onClick={handleBulkDuplicateMerge}
                    disabled={selectedDuplicateRowIds.length === 0}
                    loading={bulkMergeLoading}
                  >
                    Map Selected ({selectedDuplicateRowIds.length})
                  </Button>
                </Space>
                <Table
                  dataSource={duplicateTableData}
                  columns={duplicateTableColumns}
                  rowSelection={duplicateRowSelection}
                  pagination={false}
                  size="small"
                  scroll={{ x: 'max-content' }}
                />
                <Divider />
                <Text type="secondary">
                  Showing {duplicateTableData.length} of {duplicatesTotal}{' '}
                  duplicate rows
                </Text>
              </>
            ) : (
              <Text type="secondary">
                Duplicate rows were detected, but no preview data is available.
              </Text>
            )}
          </Card>
        )}
        <Modal
          open={mergeModalVisible}
          title={
            <Space>
              <MergeCellsOutlined />
              <span>Merge Duplicate Row</span>
            </Space>
          }
          onCancel={() => {
            setMergeModalVisible(false);
            resetMergeState();
          }}
          onOk={handleMergeSubmit}
          okButtonProps={{
            loading: mergeLoading,
            disabled: mergeDetailLoading || !mergeDetail || !mergeDetail.existing_row,
          }}
          cancelButtonProps={{
            disabled: mergeLoading,
          }}
          width={780}
        >
          {mergeDetailLoading ? (
            <div style={{ textAlign: 'center', padding: '24px 0' }}>
              <Spin />
            </div>
          ) : mergeDetail ? (
            <>
              {!mergeDetail.existing_row && (
                <Alert
                  type="warning"
                  message="Matching row not found"
                  description="We could not find a matching row in the destination table for this duplicate. No merge is possible."
                  style={{ marginBottom: 16 }}
                />
              )}
              {mergeDetail.existing_row && (
                <>
                  <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
                    Matching row identified using uniqueness columns:{' '}
                    {mergeDetail.uniqueness_columns.join(', ')}
                  </Text>
                  <Text strong style={{ marginBottom: 12, display: 'block' }}>
                    Select which values to apply from the duplicate row:
                  </Text>
                  <Table
                    dataSource={Object.keys(mergeDetail.duplicate.record)
                      .filter((column) => !column.startsWith('_'))
                      .map((column) => ({
                        key: column,
                        column,
                        existing: mergeDetail.existing_row?.record?.[column],
                        incoming: mergeDetail.duplicate.record[column],
                        selected: mergeSelections[column] ?? false,
                      }))}
                    pagination={false}
                    size="small"
                    rowKey="column"
                    columns={[
                      {
                        title: 'Column',
                        dataIndex: 'column',
                        key: 'column',
                        width: 160,
                      },
                      {
                        title: 'Existing Value',
                        dataIndex: 'existing',
                        key: 'existing',
                        render: (value: unknown) => renderDuplicateValue(value),
                      },
                      {
                        title: 'Incoming Value',
                        dataIndex: 'incoming',
                        key: 'incoming',
                        render: (value: unknown) => renderDuplicateValue(value),
                      },
                      {
                        title: 'Use Incoming',
                        key: 'selected',
                        width: 140,
                        render: (_: unknown, row: { column: string; selected: boolean }) => (
                          <Switch
                            checked={!!mergeSelections[row.column]}
                            onChange={(checked) => handleMergeSelectionChange(row.column, checked)}
                          />
                        ),
                      },
                    ]}
                  />
                  <Divider />
                  <TextArea
                    value={mergeNote}
                    onChange={(event) => setMergeNote(event.target.value)}
                    placeholder="Optional note about this merge"
                    rows={3}
                  />
                </>
              )}
            </>
          ) : (
            <Text type="secondary">Select a duplicate row to merge.</Text>
          )}
        </Modal>

        {/* Data Preview */}
        {tableData && tableData.data.length > 0 && (
          <Card 
            title={<><EyeOutlined /> Imported Data Preview</>} 
            size="small"
            loading={loadingDetails}
            extra={
              <Button
                type="link"
                onClick={() =>
                  file?.mapped_table_name &&
                  navigate(`/tables/${encodeURIComponent(file.mapped_table_name)}`)
                }
                disabled={!file?.mapped_table_name}
              >
                View Full Table
              </Button>
            }
          >
            <Table
              dataSource={tableData.data}
              columns={Object.keys(tableData.data[0] || {})
                .filter((key) => key !== "__rowKey")
                .map((key) => ({
                  title: key,
                  dataIndex: key,
                  key,
                  ellipsis: true,
                  width: 150,
                }))}
              pagination={false}
              scroll={{ x: 'max-content' }}
              size="small"
              rowKey="__rowKey"
            />
            <Divider />
            <Text type="secondary">
              Showing {tableData.data.length} of {tableData.total_rows.toLocaleString()} rows from this import
            </Text>
          </Card>
        )}

        {/* Action Buttons */}
        <Space>
          <Button 
            type="primary" 
            icon={<DatabaseOutlined />}
            onClick={() => navigate(`/query`)}
          >
            Query This Data
          </Button>
          <Button
            icon={<EyeOutlined />}
            onClick={() =>
              file?.mapped_table_name &&
              navigate(`/tables/${encodeURIComponent(file.mapped_table_name)}`)
            }
            disabled={!file?.mapped_table_name}
          >
            View Full Table
          </Button>
          <Button 
            icon={<ArrowLeftOutlined />}
            onClick={() => navigate('/import')}
          >
            Back to Import List
          </Button>
        </Space>
      </Space>
    );
  };

  if (loading) {
    return (
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16 }}>Loading file details...</div>
      </div>
    );
  }

  if (error && !file) {
    return (
      <div style={{ padding: '24px' }}>
        <Result
          status="error"
          title="Failed to Load File"
          subTitle={error}
          extra={
            <Button type="primary" onClick={() => navigate('/import')}>
              Back to Import List
            </Button>
          }
        />
      </div>
    );
  }

  if (!file) {
    return (
      <div style={{ padding: '24px' }}>
        <Result
          status="404"
          title="File Not Found"
          subTitle="The file you're looking for doesn't exist."
          extra={
            <Button type="primary" onClick={() => navigate('/import')}>
              Back to Import List
            </Button>
          }
        />
      </div>
    );
  }

  const breadcrumbItems: BreadcrumbProps['items'] = [
    {
      key: 'import',
      title: (
        <span
          style={{ cursor: 'pointer' }}
          onClick={() => navigate('/import')}
        >
          <HomeOutlined />
          <span style={{ marginLeft: 8 }}>Import</span>
        </span>
      ),
    },
    {
      key: 'file',
      title: (
        <span>
          <FileOutlined />
          <span style={{ marginLeft: 8 }}>{file.file_name}</span>
        </span>
      ),
    },
  ];

  const suppressArchiveFailureAlert =
    file?.status === 'failed' && !!archiveFailureSummary?.hasPartialFailure;

  const archiveResultsColumns: ColumnsType<ArchiveFileResult & { key: string }> = [
    {
      title: 'Actions',
      key: 'actions',
      fixed: 'left',
      width: 60,
      render: (_: unknown, record) =>
        record.uploaded_file_id ? (
          <Button
            type="link"
            size="small"
            onClick={() => navigate(`/import/${record.uploaded_file_id}`)}
          >
            View
          </Button>
        ) : null,
    },
    {
      title: 'Archive Path',
      dataIndex: 'archive_path',
      key: 'archive_path',
      width: 250,
      ellipsis: true,
      render: (text: string) => <Text code>{text}</Text>,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (value: ArchiveFileStatus) => {
        const color =
          value === 'processed' ? 'green' : value === 'failed' ? 'red' : 'default';
        return <Tag color={color}>{value}</Tag>;
      },
    },
    {
      title: 'Table',
      dataIndex: 'table_name',
      key: 'table_name',
      width: 180,
      ellipsis: true,
      render: (value?: string | null) => value || '-',
    },
    {
      title: 'Records',
      dataIndex: 'records_processed',
      key: 'records_processed',
      width: 120,
      render: (value?: number | null) =>
        typeof value === 'number' ? value.toLocaleString() : '-',
    },
    {
      title: 'Duplicates',
      dataIndex: 'duplicates_skipped',
      key: 'duplicates_skipped',
      width: 120,
      render: (value?: number | null) =>
        typeof value === 'number' ? value.toLocaleString() : '-',
    },
  ];

  const archiveResultRows = effectiveArchiveResult
    ? effectiveArchiveResult.results.map((item, index) => ({
        ...item,
        key: `${item.archive_path}-${index}`,
      }))
    : [];

  const archiveResultsPanel = effectiveArchiveResult ? (
    <Card title="Archive Results" style={{ marginTop: 24 }}>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Statistic title="Processed" value={effectiveArchiveResult.processed_files} />
        </Col>
        <Col span={6}>
          <Statistic title="Failed" value={effectiveArchiveResult.failed_files} />
        </Col>
        <Col span={6}>
          <Statistic title="Skipped" value={effectiveArchiveResult.skipped_files} />
        </Col>
        <Col span={6}>
          <Statistic title="Total Files" value={effectiveArchiveResult.total_files} />
        </Col>
      </Row>
      {archiveAggregates && (
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={8}>
            <Statistic
              title="Rows Inserted"
              value={archiveAggregates.totalRecords}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Duplicates Skipped"
              value={archiveAggregates.totalDuplicates}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Tables Updated"
              value={archiveAggregates.tablesTouched}
            />
          </Col>
        </Row>
      )}
      {effectiveArchiveResult.failed_files > 0 && !suppressArchiveFailureAlert && (
        <>
          <Alert
            type="error"
            showIcon
            style={{ marginBottom: 12 }}
            message={`We could not import ${effectiveArchiveResult.failed_files} file${
              effectiveArchiveResult.failed_files === 1 ? '' : 's'
            } from this archive.`}
            description={
              failedArchiveResults.length > 0
                ? `First failure: ${failedArchiveResults[0].archive_path} — ${failedArchiveResults[0].message || 'No details reported.'}`
                : undefined
            }
          />
          <Space style={{ marginBottom: 12 }} wrap>
            <Button
              type="primary"
              onClick={() => handleArchiveResume({ resumeAll: false })}
              disabled={disableMappingActions || archiveResumeLoading}
              loading={archiveResumeLoading}
            >
              Retry Failed Files
            </Button>
            <Button
              onClick={() => handleArchiveResume({ resumeAll: true })}
              disabled={disableMappingActions || archiveResumeLoading}
              loading={archiveResumeLoading}
            >
              Reprocess Entire Archive
            </Button>
          </Space>
        </>
      )}
      <Table
        dataSource={archiveResultRows}
        columns={archiveResultsColumns}
        pagination={false}
        size="small"
        scroll={{ x: 'max-content' }}
      />
    </Card>
  ) : null;

  const autoTabContent = (
    <div style={{ padding: '24px 0' }}>
      <Alert
        message="Automatic Processing"
        description="The AI will analyze your file, compare it with existing tables, and automatically import the data without asking questions. This is the fastest option."
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />
      {isArchive && (
        <Alert
          message="Archive detected"
          description="Auto Process Archive will unpack every CSV/XLSX in this ZIP file and run the auto mapper on each one sequentially."
          type="warning"
          showIcon
          style={{ marginBottom: 24 }}
        />
      )}

      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

      {!result && (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div>
            <Text strong>File: </Text>
            <Text>{file.file_name}</Text>
          </div>
        <div>
          <Text strong>Size: </Text>
          <Text>{formatBytes(file.file_size)}</Text>
        </div>
        {instructionField}
        {isExcelFile && (
          <div style={{ maxWidth: 480 }}>
            <Text strong>Workbook tabs</Text>
            <Paragraph type="secondary" style={{ marginBottom: 8 }}>
              Auto processing will create one import per selected tab using the workbook name plus the sheet name.
              </Paragraph>
              <Select
                mode="multiple"
                style={{ width: '100%' }}
                placeholder={sheetNames.length ? 'Select sheets to process' : 'No sheets found'}
                value={selectedSheets}
                onChange={(values) => setSelectedSheets(values)}
                options={sheetNames.map((sheet) => ({ label: sheet, value: sheet }))}
                disabled={!sheetNames.length}
              />
            </div>
          )}
            <div>
            <Space align="start">
              <Switch checked={skipFileDuplicateCheck} onChange={(checked) => setSkipFileDuplicateCheck(checked)} />
              <div>
                <Text strong>Skip duplicate row detection</Text>
                <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                  By default, duplicate rows are detected and skipped based on unique columns. Enable this to import all rows without checking for duplicates.
                </Paragraph>
              </div>
            </Space>
          </div>
          <div>
            <Space align="start">
              <Switch checked={useSharedTable} onChange={(checked) => setUseSharedTable(checked)} />
              <div>
                <Text strong>Use a single table for this import</Text>
                <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                  Map {isArchive ? 'every file in this archive' : 'this file'} into one table.
                </Paragraph>
                {useSharedTable && (
                  <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                    <Select
                      value={sharedTableMode}
                      style={{ width: 240 }}
                      onChange={(value) => {
                        setSharedTableMode(value as 'existing' | 'new');
                        setSharedTableName('');
                      }}
                      options={[
                        { value: 'new', label: 'Create new table' },
                        { value: 'existing', label: 'Use existing table' },
                      ]}
                    />
                    {sharedTableMode === 'new' ? (
                      <Input
                        value={sharedTableName}
                        placeholder="Enter new table name"
                        onChange={(e) => setSharedTableName(e.target.value)}
                        style={{ width: 360 }}
                      />
                    ) : (
                      <Select
                        showSearch
                        value={sharedTableName || undefined}
                        placeholder="Select an existing table"
                        onChange={(value) => setSharedTableName(value)}
                        loading={loadingTables}
                        style={{ width: 360 }}
                        options={existingTables.map((table) => ({
                          value: table.table_name,
                          label: `${table.table_name} (${table.row_count.toLocaleString()} rows)`,
                        }))}
                        filterOption={(input, option) =>
                          (option?.label?.toString() ?? '').toLowerCase().includes(input.toLowerCase())
                        }
                      />
                    )}
                  </Space>
                )}
              </div>
            </Space>
          </div>

          {!isArchive && (
            <Button
              type="primary"
              size="large"
              icon={<ThunderboltOutlined />}
              onClick={handleAutoProcess}
              loading={processing}
              disabled={disableMappingActions}
              block
            >
              {processing ? 'Processing...' : 'Process Now'}
            </Button>
          )}

          {isArchive && (
            <Button
              type="primary"
              size="large"
              icon={<ThunderboltOutlined />}
              onClick={handleArchiveAutoProcess}
              loading={archiveProcessing}
              disabled={disableMappingActions}
              block
            >
              {archiveProcessing ? 'Processing Archive...' : 'Auto Process Archive'}
            </Button>
          )}
        </Space>
      )}

        {archiveResultsPanel}
    </div>
  );

  const interactiveTabContent = (
    <div style={{ padding: '24px 0' }}>
      <Alert
        message="Interactive Processing"
        description="The AI will ask you questions to better understand how to import your data. This gives you more control over the process."
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

      <div style={{ marginBottom: 16 }}>{instructionField}</div>

      {!result && conversation.length === 0 && (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div>
            <Text strong>File: </Text>
            <Text>{file.file_name}</Text>
          </div>
          <div>
            <Text strong>Size: </Text>
            <Text>{formatBytes(file.file_size)}</Text>
          </div>
          {isExcelFile && sheetNames.length > 0 && (
            <div style={{ maxWidth: 360 }}>
              <Text strong>Choose a tab to review</Text>
              <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                Interactive mode works one sheet at a time. Pick a tab or leave the default to start with the first sheet.
              </Paragraph>
              <Select
                style={{ width: '100%' }}
                placeholder="Select a sheet"
                value={interactiveSheet}
                onChange={(value) => setInteractiveSheet(value)}
                options={sheetNames.map((sheet) => ({ label: sheet, value: sheet }))}
              />
            </div>
          )}

          {processing ? (
            <div style={{ textAlign: 'center', padding: '24px 0' }}>
              <Spin size="large" />
            </div>
          ) : (
            <Button
              type="primary"
              size="large"
              icon={<MessageOutlined />}
              onClick={() => {
                const previousError =
                  file.status === 'failed' && showInteractiveRetry
                    ? (file.error_message || '').trim()
                    : '';
                handleInteractiveStart(previousError ? { previousError } : undefined);
              }}
              disabled={disableMappingActions}
              block
            >
              Start Interactive Analysis
            </Button>
          )}
        </Space>
      )}

      {!result && conversation.length > 0 && (
        <div>
          <div
            style={{
              maxHeight: '400px',
              overflowY: 'auto',
              marginBottom: 16,
              padding: 16,
              border: '1px solid #d9d9d9',
              borderRadius: 4,
              backgroundColor: '#fafafa',
            }}
          >
            {conversation.map((msg, idx) => (
              <div
                key={idx}
                style={{
                  marginBottom: 16,
                  padding: 12,
                  backgroundColor: msg.role === 'user' ? '#e6f7ff' : '#fff',
                  borderRadius: 4,
                  border: '1px solid',
                  borderColor: msg.role === 'user' ? '#91d5ff' : '#d9d9d9',
                }}
              >
                <Text strong style={{ display: 'block', marginBottom: 8 }}>
                  {msg.role === 'user' ? 'You:' : 'AI:'}
                </Text>
                <Paragraph style={{ marginBottom: 0, whiteSpace: 'pre-wrap' }}>
                  {msg.content}
                </Paragraph>
              </div>
            ))}
            {processing && (
              <div style={{ textAlign: 'center', padding: 16 }}>
                <Spin />
              </div>
            )}
          </div>

          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Alert
              type={canExecute ? 'success' : 'info'}
              message={
                canExecute
                  ? 'Mapping confirmed. Execute when ready or ask for additional adjustments below.'
                  : needsUserInput
                    ? 'The assistant is waiting for your direction. Ask for changes or confirm when the plan looks right.'
                    : 'Processing... the assistant will respond shortly.'
              }
              showIcon
            />

            <Button
              type="primary"
              size="large"
              icon={<CheckCircleOutlined />}
              onClick={handleInteractiveExecute}
              loading={processing}
              disabled={!canExecute || processing}
              block
            >
              {processing ? 'Executing...' : 'Execute Import'}
            </Button>

            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <Space.Compact style={{ width: '100%' }}>
                <input
                  type="text"
                  value={userInput}
                  onChange={(e) => setUserInput(e.target.value)}
                  onKeyPress={(e) => {
                    if (e.key === 'Enter' && !processing) {
                      handleInteractiveSend();
                    }
                  }}
                  placeholder="Ask for changes, confirmations, or next steps..."
                  disabled={processing || !threadId}
                  style={{
                    flex: 1,
                    padding: '8px 12px',
                    border: '1px solid #d9d9d9',
                    borderRadius: '4px 0 0 4px',
                    fontSize: 14,
                  }}
                />
                <Button
                  type="primary"
                  onClick={handleInteractiveSend}
                  loading={processing}
                  disabled={
                    !userInput.trim() || processing || !threadId
                  }
                  style={{ borderRadius: '0 4px 4px 0' }}
                >
                  Send
                </Button>
              </Space.Compact>

              <Space wrap>
                {quickActions.map(({ label, prompt }) => (
                  <Button
                    key={label}
                    size="small"
                    type={label === 'Approve Plan' ? 'primary' : 'default'}
                    disabled={!threadId || processing}
                    onClick={() => handleQuickAction(prompt)}
                  >
                    {label}
                  </Button>
                ))}
              </Space>
            </Space>
          </Space>
        </div>
      )}
    </div>
  );

  const tabItems = [
    {
      key: 'auto',
      label: (
        <span>
          <ThunderboltOutlined /> Auto Process
        </span>
      ),
      children: autoTabContent,
    },
    {
      key: 'interactive',
      label: (
        <span>
          <MessageOutlined /> Interactive
        </span>
      ),
      children: interactiveTabContent,
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Breadcrumb style={{ marginBottom: 16 }} items={breadcrumbItems} />

      <Button
        icon={<ArrowLeftOutlined />}
        onClick={() => navigate('/import')}
        style={{ marginBottom: 16 }}
      >
        Back to Import List
      </Button>

      {displayJobInfo && !shouldHideJobAlert && (
        <Alert
          type={
            displayJobInfo.status === 'failed'
              ? 'error'
              : displayJobInfo.status === 'succeeded'
                ? 'success'
                : 'info'
          }
          showIcon
          message={`Import job: ${displayJobInfo.status}`}
          description={
            <Space direction="vertical" size={4}>
              <Text>
                {displayJobInfo.stage
                  ? `Stage: ${displayJobInfo.stage.replace(/_/g, ' ')}`
                  : 'Processing in progress'}
              </Text>
              {mappingJobActive && (
                <Text type="secondary">
                  Mapping actions are temporarily disabled while this job runs.
                </Text>
              )}
              {displayJobInfo.error_message && (
                <Text type="secondary">Last error: {displayJobInfo.error_message}</Text>
              )}
            </Space>
          }
          style={{ marginBottom: 16 }}
        />
      )}

      {isArchiveMappingActive && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Space align="start" size={16} style={{ width: '100%' }}>
            <Spin size="large" />
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Text strong>Archive mapping progress</Text>
              <Progress
                percent={archiveProgressPercent}
                status={archiveProgressStatus}
                size="small"
              />
              {displayJobInfo?.stage && (
                <Text type="secondary">
                  Stage: {displayJobInfo.stage.replace(/_/g, ' ')}
                </Text>
              )}
              {archiveJobProgress?.currentFile && (
                <Text>
                  Currently processing: <Text code>{archiveJobProgress.currentFile}</Text>
                </Text>
              )}
              {archiveJobProgress?.completed.length ? (
                <Space direction="vertical" size={4} style={{ width: '100%' }}>
                  <Text strong>Completed files</Text>
                  <Space wrap size={[4, 4]}>
                    {archiveJobProgress.completed.slice(0, 8).map((item) => (
                      <Tag
                        key={`done-${item.archive_path}`}
                        color={
                          item.status === 'processed'
                            ? 'green'
                            : item.status === 'failed'
                              ? 'red'
                              : 'default'
                        }
                      >
                        {item.archive_path}
                      </Tag>
                    ))}
                    {archiveJobProgress.completed.length > 8 && (
                      <Tag>+{archiveJobProgress.completed.length - 8} more</Tag>
                    )}
                  </Space>
                </Space>
              ) : null}
              {archiveJobProgress?.remaining.length ? (
                <Space direction="vertical" size={4} style={{ width: '100%' }}>
                  <Text strong>Remaining files</Text>
                  <Space wrap size={[4, 4]}>
                    {archiveJobProgress.remaining.slice(0, 8).map((name) => (
                      <Tag key={`pending-${name}`}>{name}</Tag>
                    ))}
                    {archiveJobProgress.remaining.length > 8 && (
                      <Tag>+{archiveJobProgress.remaining.length - 8} more</Tag>
                    )}
                  </Space>
                </Space>
              ) : null}
            </Space>
          </Space>
        </Card>
      )}

      {!isArchiveMappingActive && isMappingInProgress && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Space align="start" size={16}>
            <Spin size="large" />
            <Space direction="vertical" size={4}>
              <Text strong>Mapping in progress</Text>
              <Text type="secondary">
                We are mapping {file.file_name}. You can stay on this page to see live updates.
              </Text>
              {mappingStageLabel && (
                <Text type="secondary">Stage: {mappingStageLabel.replace(/_/g, ' ')}</Text>
              )}
              {progressDisplayPercent !== null && (
                <Progress
                  percent={progressDisplayPercent}
                  status="active"
                  size="small"
                  style={{ width: 260 }}
                  format={renderProgressLabel}
                />
              )}
            </Space>
          </Space>
        </Card>
      )}

      {file.status === 'failed' && !showInteractiveRetry ? (
        <Card title={`Failed Mapping: ${file.file_name}`}>
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Alert
              message={
                hasPartialArchiveFailure
                  ? `${archiveFailedFileCount} file${archiveFailedFileCount === 1 ? '' : 's'} didn't map properly`
                  : 'Mapping failed'
              }
              description={
                hasPartialArchiveFailure
                  ? `We imported ${archiveSuccessfulFileCount} of ${archiveTotalForDisplay || archiveSuccessfulFileCount + archiveFailedFileCount} files from this archive, but some need attention. Review the archive paths below.`
                  : 'We couldn’t finish mapping this file. Follow the guidance below or open the technical details for more context.'
              }
              type={hasPartialArchiveFailure ? 'warning' : 'error'}
              showIcon
            />

            {hasPartialArchiveFailure && failedArchiveResults.length > 0 && (
              <Card
                size="small"
                type="inner"
                title="Files needing attention"
              >
                <Table
                  dataSource={failedArchiveResults.map((entry, index) => ({
                    key: `${entry.archive_path}-${index}`,
                    archive_path: entry.archive_path,
                    status: entry.status,
                  }))}
                  columns={[
                    {
                      title: 'Archive Path',
                      dataIndex: 'archive_path',
                      key: 'archive_path',
                      render: (value: string) => <Text code>{value}</Text>,
                    },
                    {
                      title: 'Status',
                      dataIndex: 'status',
                      key: 'status',
                      render: (value: ArchiveFileStatus) => (
                        <Tag color={value === 'failed' ? 'red' : 'default'}>{value}</Tag>
                      ),
                    },
                  ]}
                  pagination={false}
                  size="small"
                  scroll={{ x: 'max-content' }}
                />
              </Card>
            )}

            <Card title="What to do next" size="small" type="inner">
              <Space direction="vertical" size="small" style={{ width: '100%' }}>
                <Paragraph style={{ marginBottom: 8 }}>
                  Retry the import with the AI assistant to adjust mappings, or return to the import list if you need to update the source file first.
                </Paragraph>
                <Space wrap>
                  <Button 
                    type="primary"
                    onClick={handleRetryInteractive}
                    disabled={processing}
                    loading={processing}
                  >
                    {processing ? 'Starting retry...' : 'Retry with AI assistant'}
                  </Button>
                  <Button 
                    icon={<ArrowLeftOutlined />}
                    onClick={() => navigate('/import')}
                  >
                    Back to Import List
                  </Button>
                </Space>
              </Space>
            </Card>

            <Collapse bordered={false}>
              <Collapse.Panel header="Technical details (optional)" key="technical-details">
                {file.error_message ? (
                  <Card title="Error Details" size="small" type="inner" style={{ marginBottom: 12 }}>
                    <ErrorLogViewer error={file.error_message} showRetry={false} />
                  </Card>
                ) : (
                  <Text type="secondary">No error details reported for this job.</Text>
                )}
                {isArchiveFile && archiveResultsPanel}
              </Collapse.Panel>
            </Collapse>
          </Space>
        </Card>
      ) : file.status === 'mapped' ? (
        <Card title={`Mapped File: ${file.file_name}`}>
          {renderMappedFileView()}
        </Card>
      ) : result ? (
        <Card>
          {result.success ? (
            <Result
              status="success"
              title="File Mapped Successfully!"
              subTitle={`Your data has been imported into the database.`}
              extra={[
                <Button type="primary" key="list" onClick={() => navigate('/import')}>
                  Back to Import List
                </Button>,
                <Button key="query" onClick={() => navigate('/query')}>
                  Query Data
                </Button>,
              ]}
            >
              <Row gutter={16} style={{ marginTop: 24 }}>
                <Col span={8}>
                  <Statistic
                    title="Table Name"
                    value={result.table_name || 'N/A'}
                  />
                </Col>
                <Col span={8}>
                  <Statistic
                    title="Rows Imported"
                    value={result.rows_imported || 0}
                  />
                </Col>
                <Col span={8}>
                  <Statistic
                    title="Execution Time"
                    value={result.execution_time || 0}
                    suffix="s"
                    precision={2}
                  />
                </Col>
              </Row>
            </Result>
          ) : (
            <Result
              status="error"
              title="Import Failed"
              subTitle={result.error || 'An error occurred during import'}
              extra={[
                <Button type="primary" key="retry" onClick={() => setResult(null)}>
                  Try Again
                </Button>,
                <Button key="list" onClick={() => navigate('/import')}>
                  Back to Import List
                </Button>,
              ]}
            />
          )}
        </Card>
      ) : (
        <Card title={`Map File: ${file.file_name}`}>
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            items={tabItems}
          />
        </Card>
      )}
    </div>
  );
};

export default ImportMappingPage;
