import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router';
import { App as AntdApp, Card, Tabs, Button, Space, Alert, Spin, Typography, Result, Statistic, Row, Col, Breadcrumb, Descriptions, Table, Tag, Divider, Modal, Switch, Input } from 'antd';
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
}

type ArchiveFileStatus = 'processed' | 'failed' | 'skipped';

interface ArchiveFileResult {
  archive_path: string;
  stored_file_name?: string | null;
  uploaded_file_id?: string | null;
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
  const [archiveResult, setArchiveResult] = useState<ArchiveAutoProcessResult | null>(null);
  const [archiveHistorySummary, setArchiveHistorySummary] = useState<ArchiveHistorySummary | null>(null);
  const [archiveJobDetails, setArchiveJobDetails] = useState<ImportJobInfo | null>(null);
  
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

  const isArchiveFile = file?.file_name?.toLowerCase().endsWith('.zip') ?? false;

  const effectiveArchiveResult = archiveResult ?? archiveHistorySummary?.result ?? null;

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

  const fetchMappedFileDetails = useCallback(async (fileMeta: UploadedFile) => {
    if (!fileMeta.mapped_table_name) return;

    const tableName = fileMeta.mapped_table_name;
    setLoadingDetails(true);
    setDuplicateData(null);
    try {
      const token = localStorage.getItem('refine-auth');

      // Fetch table data preview
      const tableResponse = await axios.get(`${API_URL}/tables/${tableName}`, {
        params: { limit: 10, offset: 0 },
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
    [API_URL, importHistory, messageApi]
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
    API_URL,
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

    if (archiveResult || file.status !== 'mapped') {
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
  }, [API_URL, archiveResult, file?.id, file?.status, isArchiveFile]);

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
  }, [API_URL, archiveHistorySummary?.job, archiveResult?.job_id, isArchiveFile]);

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
    let interval: ReturnType<typeof setInterval> | undefined;

    const pollJob = async () => {
      const job = await fetchJobDetails(file.active_job_id!);
      if (!job || cancelled) {
        return;
      }

      if (job.status === 'succeeded' || job.status === 'failed') {
        if (interval) {
          clearInterval(interval);
        }
        await fetchFileDetails();
      }
    };

    pollJob();
    interval = setInterval(pollJob, 5000);

    return () => {
      cancelled = true;
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [file?.active_job_id, fetchJobDetails, fetchFileDetails]);

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
        const analysisResponse = await axios.post(
          `${API_URL}/analyze-file-interactive`,
          {
            file_id: id,
            max_iterations: 5,
            previous_error_message: failureMessage,
          },
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
    [fetchFileDetails, id, messageApi]
  );

  const handleAutoProcess = async () => {
    if (!id) return;
    
    setProcessing(true);
    setError(null);
    setResult(null);
    setArchiveResult(null);
    let autoError: string | null = null;

    try {
      const token = localStorage.getItem('refine-auth');
      const formData = new FormData();
      formData.append('file_id', id);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');

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

      const response = await axios.post(`${API_URL}/auto-process-archive`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      const payload: ArchiveAutoProcessResult = response.data;
      setArchiveResult(payload);
      if (payload.success) {
        messageApi.success('Archive processed successfully');
      } else {
        messageApi.warning('Archive processed with some errors');
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

  const handleInteractiveStart = async (options?: { previousError?: string }) => {
    if (!id) return;
    
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

      if (options?.previousError) {
        payload.previous_error_message = options.previousError;
      }

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

    setShowInteractiveRetry(true);
    setActiveTab('interactive');

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
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: id,
          user_message: trimmed,
          thread_id: threadId,
          max_iterations: 5,
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
            title={<><EyeOutlined /> Data Preview (First 10 Rows)</>} 
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
              Showing 10 of {tableData.total_rows.toLocaleString()} total rows
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

  const isArchive = isArchiveFile;

  const archiveResultsColumns: ColumnsType<ArchiveFileResult & { key: string }> = [
    {
      title: 'Archive Path',
      dataIndex: 'archive_path',
      key: 'archive_path',
      render: (text: string) => <Text code>{text}</Text>,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
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
      render: (value?: string | null) => value || '-',
    },
    {
      title: 'Records',
      dataIndex: 'records_processed',
      key: 'records_processed',
      render: (value?: number | null) =>
        typeof value === 'number' ? value.toLocaleString() : '-',
    },
    {
      title: 'Duplicates',
      dataIndex: 'duplicates_skipped',
      key: 'duplicates_skipped',
      render: (value?: number | null) =>
        typeof value === 'number' ? value.toLocaleString() : '-',
    },
    {
      title: 'Message',
      dataIndex: 'message',
      key: 'message',
      render: (value?: string | null) => value || '-',
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: unknown, record) =>
        record.uploaded_file_id ? (
          <Button
            type="link"
            size="small"
            onClick={() => navigate(`/import/${record.uploaded_file_id}`)}
          >
            View File
          </Button>
        ) : null,
    },
  ];

  const archiveResultRows = effectiveArchiveResult
    ? effectiveArchiveResult.results.map((item, index) => ({
        ...item,
        key: `${item.archive_path}-${index}`,
      }))
    : [];

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

          {!isArchive && (
            <Button
              type="primary"
              size="large"
              icon={<ThunderboltOutlined />}
              onClick={handleAutoProcess}
              loading={processing}
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
              block
            >
              {archiveProcessing ? 'Processing Archive...' : 'Auto Process Archive'}
            </Button>
          )}
        </Space>
      )}

        {effectiveArchiveResult && (
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
            <Table
              dataSource={archiveResultRows}
              columns={archiveResultsColumns}
              pagination={false}
              size="small"
              scroll={{ x: 'max-content' }}
            />
          </Card>
        )}
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

            {canExecute && (
              <Button
                type="primary"
                size="large"
                icon={<CheckCircleOutlined />}
                onClick={handleInteractiveExecute}
                loading={processing}
                block
              >
                {processing ? 'Executing...' : 'Execute Import'}
              </Button>
            )}

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
                  disabled={!userInput.trim() || processing || !threadId}
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

      {jobInfo && (
        <Alert
          type={
            jobInfo.status === 'failed'
              ? 'error'
              : jobInfo.status === 'succeeded'
                ? 'success'
                : 'info'
          }
          showIcon
          message={`Import job: ${jobInfo.status}`}
          description={
            <Space direction="vertical" size={4}>
              <Text>
                {jobInfo.stage
                  ? `Stage: ${jobInfo.stage.replace(/_/g, ' ')}`
                  : 'Processing in progress'}
              </Text>
              {jobInfo.error_message && (
                <Text type="secondary">Last error: {jobInfo.error_message}</Text>
              )}
            </Space>
          }
          style={{ marginBottom: 16 }}
        />
      )}

      {file.status === 'failed' ? (
        <Card title={`Failed Mapping: ${file.file_name}`}>
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Alert
              message="Mapping Failed"
              description="The file mapping process encountered an error. Please review the details below and try again."
              type="error"
              showIcon
            />

            {file.error_message && (
              <Card title="Error Details" size="small" type="inner">
                <ErrorLogViewer error={file.error_message} showRetry={false} />
              </Card>
            )}
            <Space>
              {!showInteractiveRetry && (
                <Button 
                  type="primary"
                  onClick={handleRetryInteractive}
                  disabled={processing}
                >
                  {processing ? 'Starting...' : 'Try Again'}
                </Button>
              )}
              <Button 
                icon={<ArrowLeftOutlined />}
                onClick={() => navigate('/import')}
              >
                Back to Import List
              </Button>
            </Space>

            {showInteractiveRetry && (
              <Card 
                title="Retry with AI Assistant" 
                size="small" 
                type="inner"
              >
                {interactiveTabContent}
              </Card>
            )}
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
