import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router';
import { App as AntdApp, Card, Table, Tabs, Badge, Button, Space, Modal, Checkbox, Tag } from 'antd';
import { ReloadOutlined, DeleteOutlined, EyeOutlined, ThunderboltOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { FileUpload } from '../../components/file-upload';
import axios from 'axios';
import { API_URL } from '../../config';
import type { Key } from 'react';

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

interface ImportSummary {
  import_id?: string;
  duplicates_found?: number;
}

interface ImportHistoryRecord {
  import_id?: string;
  source_path?: string;
  file_name?: string;
  file_size_bytes?: number;
  duplicates_found?: number;
}
export const ImportPage: React.FC = () => {
  const navigate = useNavigate();
  const [files, setFiles] = useState<Array<UploadedFile & ImportSummary>>([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<string>('all');
  const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const [tabCounts, setTabCounts] = useState<Record<string, number>>({
    all: 0,
    uploaded: 0,
    mapped: 0,
    needs_mapping: 0,
  });
  const [importSummaryCache, setImportSummaryCache] = useState<Record<string, ImportSummary>>({});
  const { message: messageApi } = AntdApp.useApp();

  const normalizeJobStatus = useCallback(
    (status?: string | null) => (status || '').toLowerCase().trim(),
    []
  );

  const isJobActive = useCallback((file: UploadedFile) => {
    const normalized = normalizeJobStatus(file.active_job_status);
    const hasJobMetadata = file.active_job_id || file.active_job_stage || file.active_job_progress;
    const isTerminal =
      normalized === 'succeeded' ||
      normalized === 'failed' ||
      normalized === 'completed' ||
      normalized === 'cancelled' ||
      normalized === 'canceled';

    return (
      file.status === 'mapping' ||
      (!!normalized && !isTerminal) ||
      (!normalized && !!hasJobMetadata)
    );
  }, [normalizeJobStatus]);

  const isJobQueued = (file: UploadedFile) => {
    const normalized = normalizeJobStatus(file.active_job_status);
    return normalized === 'queued' || normalized === 'pending';
  };

  const attachImportSummaries = useCallback(async (
    fileList: UploadedFile[],
    token: string | null
  ): Promise<Array<UploadedFile & ImportSummary>> => {
    if (!fileList.length) {
      return fileList;
    }

    const headers = {
      ...(token && { Authorization: `Bearer ${token}` }),
    };

    const mappedFiles = fileList.filter((file) => file.status === 'mapped');
    // If we already cached summaries for all mapped files, skip the network trip.
    const missingSummaries = mappedFiles.filter((file) => !importSummaryCache[file.id]);
    let mergedCache = importSummaryCache;

    if (missingSummaries.length > 0) {
      try {
        // Fetch a single page of import history and match locally by source path,
        // falling back to filename + size. The endpoint is ordered by newest first.
        const historyLimit = Math.max(missingSummaries.length, 200);
        const response = await axios.get(`${API_URL}/import-history`, {
          params: {
            limit: historyLimit,
            offset: 0,
          },
          headers,
        });

        const history: ImportHistoryRecord[] = response.data?.imports || [];
        const updates: Record<string, ImportSummary> = {};

        missingSummaries.forEach((file) => {
          const match = history.find((record) => {
            const recordSource = record.source_path;
            const recordName = record.file_name;
            const recordSize = record.file_size_bytes;
            return (
              (recordSource && recordSource === file.b2_file_path) ||
              (recordName === file.file_name && recordSize === file.file_size)
            );
          }) as { import_id?: string; duplicates_found?: number } | undefined;

          if (match) {
            updates[file.id] = {
              import_id: match.import_id,
              duplicates_found: match.duplicates_found ?? 0,
            };
          } else {
            updates[file.id] = { duplicates_found: undefined };
          }
        });

        mergedCache = { ...importSummaryCache, ...updates };
        setImportSummaryCache(mergedCache);
      } catch (err) {
        console.error('Error fetching import history batch:', err);
      }
    }

    return fileList.map((file) => ({
      ...file,
      ...(mergedCache[file.id] || {}),
    }));
  }, [importSummaryCache]);

  const statusGroups: Record<string, string[]> = useMemo(
    () => ({
      needs_mapping: ['uploaded', 'mapping', 'failed'],
    }),
    []
  );

  const fetchTabCounts = useCallback(async () => {
    try {
      const token = localStorage.getItem('refine-auth');
      const headers = {
        ...(token && { Authorization: `Bearer ${token}` }),
      };

      const statusRequests = [
        { key: 'all', params: { limit: 1, offset: 0 } },
        { key: 'uploaded', params: { status: 'uploaded', limit: 1, offset: 0 } },
        { key: 'mapped', params: { status: 'mapped', limit: 1, offset: 0 } },
        { key: 'mapping', params: { status: 'mapping', limit: 1, offset: 0 } },
        { key: 'failed', params: { status: 'failed', limit: 1, offset: 0 } },
      ];

      const responses = await Promise.all(
        statusRequests.map(({ params }) =>
          axios.get(`${API_URL}/uploaded-files`, {
            params,
            headers,
          })
        )
      );

      const totals = responses.reduce<Record<string, number>>((acc, response, index) => {
        const key = statusRequests[index].key;
        acc[key] = response?.data?.total_count ?? 0;
        return acc;
      }, {});

      setTabCounts({
        all: totals.all ?? 0,
        uploaded: totals.uploaded ?? 0,
        mapped: totals.mapped ?? 0,
        needs_mapping: (totals.uploaded ?? 0) + (totals.mapping ?? 0) + (totals.failed ?? 0),
      });
    } catch (err) {
      console.error('Error fetching tab counts:', err);
    }
  }, []);

  const fetchFiles = useCallback(
    async (
      status: string = activeTab,
      page: number = currentPage,
      size: number = pageSize
    ) => {
      setLoading(true);
      try {
        const token = localStorage.getItem('refine-auth');
        const headers = {
          ...(token && { Authorization: `Bearer ${token}` }),
        };
        const groupedStatuses = statusGroups[status];
        let fetchedFiles: UploadedFile[] = [];
        let nextTotal = 0;

        if (groupedStatuses) {
          const limit = page * size;
          const responses = await Promise.all(
            groupedStatuses.map((groupStatus) =>
              axios.get(`${API_URL}/uploaded-files`, {
                params: {
                  status: groupStatus,
                  limit,
                  offset: 0,
                },
                headers,
              })
            )
          );

          const combinedFiles = responses
            .filter((response) => response.data.success)
            .flatMap((response) => response.data.files as UploadedFile[]);
          nextTotal = responses.reduce(
            (sum, response) => sum + (response?.data?.total_count ?? 0),
            0
          );

          combinedFiles.sort((a, b) => {
            const aDate = a.upload_date ? new Date(a.upload_date).getTime() : 0;
            const bDate = b.upload_date ? new Date(b.upload_date).getTime() : 0;
            return bDate - aDate;
          });

          const sliceStart = (page - 1) * size;
          fetchedFiles = combinedFiles.slice(sliceStart, sliceStart + size);
        } else {
          const params: { status?: string; limit: number; offset: number } = {
            limit: size,
            offset: (page - 1) * size,
          };
          if (status && status !== 'all') {
            params.status = status;
          }

          const response = await axios.get(`${API_URL}/uploaded-files`, {
            params,
            headers,
          });

          if (response.data.success) {
            fetchedFiles = response.data.files;
            nextTotal = response.data.total_count;
          }
        }

        const filesWithImports = await attachImportSummaries(fetchedFiles, token);
        setFiles(filesWithImports);
        setTotalCount(nextTotal);
        setTabCounts((prev) => ({ ...prev, [status || 'all']: nextTotal }));
        fetchTabCounts();
        setSelectedRowKeys((prev) =>
          prev.filter((key) =>
            filesWithImports.some(
              (file: UploadedFile) => file.id === key && !isJobActive(file)
            )
          )
        );
      } catch (error) {
        messageApi.error('Failed to fetch files');
        console.error('Error fetching files:', error);
      } finally {
        setLoading(false);
      }
    },
    [
      activeTab,
      attachImportSummaries,
      currentPage,
      fetchTabCounts,
      isJobActive,
      messageApi,
      pageSize,
      statusGroups,
    ]
  );

  useEffect(() => {
    fetchFiles(activeTab, currentPage, pageSize);
    
    // Auto-refresh every 30 seconds to reduce load on the history endpoint
    const interval = setInterval(() => {
      fetchFiles(activeTab, currentPage, pageSize);
    }, 30000);

    return () => clearInterval(interval);
  }, [activeTab, currentPage, pageSize, fetchFiles]);

  const handleDelete = async (fileId: string, fileName: string, deleteTableData = false) => {
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.delete(`${API_URL}/uploaded-files/${fileId}`, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
        params: {
          delete_table_data: deleteTableData,
        },
      });

      if (response.data.success) {
        const removedRows = response.data.rows_removed || 0;
        const tableName = response.data.table_name;
        const extra = deleteTableData && removedRows
          ? ` Removed ${removedRows} row${removedRows === 1 ? '' : 's'}${tableName ? ` from ${tableName}` : ''}.`
          : '';
        messageApi.success(`${fileName} deleted successfully.${extra}`);
        fetchFiles(activeTab, currentPage, pageSize);
      }
    } catch (error) {
      messageApi.error(`Failed to delete ${fileName}`);
      console.error('Error deleting file:', error);
    }
  };

  const handleMapNow = (file: UploadedFile) => {
    if (isJobActive(file)) {
      messageApi.warning('This file already has a job queued or running. Please wait for it to finish.');
      return;
    }
    navigate(`/import/${file.id}`);
  };

  const handleView = (file: UploadedFile) => {
    navigate(`/import/${file.id}`);
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

  const handlePaginationChange = (page: number, nextPageSize?: number) => {
    setCurrentPage(page);
    if (nextPageSize && nextPageSize !== pageSize) {
      setPageSize(nextPageSize);
    }
  };

  const handlePageSizeChange = (_: number, nextPageSize: number) => {
    setCurrentPage(1);
    if (nextPageSize !== pageSize) {
      setPageSize(nextPageSize);
    }
  };

  const selectedFiles = files.filter((file) => selectedRowKeys.includes(file.id));
  const mappableFiles = selectedFiles.filter(
    (file) => ['uploaded', 'failed'].includes(file.status) && !isJobActive(file)
  );
  const remappableFiles = selectedFiles.filter(
    (file) => file.status === 'mapped' && !isJobActive(file)
  );

  const handleBulkOpen = (targets: UploadedFile[], actionLabel: string) => {
    if (!targets.length) {
      messageApi.info(`Select files that can be ${actionLabel}.`);
      return;
    }

    const busySelections = selectedFiles.filter(isJobActive);
    if (busySelections.length > 0) {
      messageApi.warning(
        `Wait for queued/running jobs to finish before mapping: ${busySelections
          .map((file) => file.file_name)
          .join(', ')}`
      );
      return;
    }

    targets.forEach((file, index) => {
      const path = `/import/${file.id}`;
      if (index === 0) {
        navigate(path);
      } else if (typeof window !== 'undefined') {
        window.open(path, '_blank', 'noopener');
      }
    });

    const skipped = selectedFiles.length - targets.length;
    if (skipped > 0) {
      messageApi.warning(`Skipped ${skipped} file(s) not eligible to ${actionLabel}.`);
    }
  };

  const handleBulkDelete = async (deleteTableData = false) => {
    if (!selectedFiles.length) {
      messageApi.info('Select at least one file to delete.');
      return;
    }
    setLoading(true);
    const token = localStorage.getItem('refine-auth');
    let deleted = 0;
    let failed = 0;
    let totalRowsRemoved = 0;

    for (const file of selectedFiles) {
      try {
        const response = await axios.delete(`${API_URL}/uploaded-files/${file.id}`, {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
          params: {
            delete_table_data: deleteTableData,
          },
        });
        if (response.data.success) {
          deleted += 1;
          totalRowsRemoved += response.data.rows_removed || 0;
        } else {
          failed += 1;
        }
      } catch (error) {
        failed += 1;
        console.error(`Error deleting file ${file.file_name}:`, error);
      }
    }

    if (deleted) {
      const extra = deleteTableData && totalRowsRemoved
        ? ` Removed ${totalRowsRemoved} imported row${totalRowsRemoved === 1 ? '' : 's'}.`
        : '';
      messageApi.success(`Deleted ${deleted} file(s).${extra}`);
    }
    if (failed) {
      messageApi.error(`Failed to delete ${failed} file(s).`);
    }

    setSelectedRowKeys([]);
    await fetchFiles(activeTab, currentPage, pageSize);
    setLoading(false);
  };

  const confirmSingleDelete = (file: UploadedFile) => {
    let deleteTableData = false;
    Modal.confirm({
      title: 'Delete file',
      content: (
        <div>
          <p>Are you sure you want to delete {file.file_name}?</p>
          {file.mapped_table_name ? (
            <Checkbox onChange={(event) => { deleteTableData = event.target.checked; }}>
              Also remove imported rows from <strong>{file.mapped_table_name}</strong>
            </Checkbox>
          ) : (
            <p style={{ marginBottom: 0, color: '#888' }}>
              This upload is not mapped to a table, so only the file will be removed.
            </p>
          )}
        </div>
      ),
      okText: 'Delete',
      cancelText: 'Cancel',
      okButtonProps: { danger: true },
      onOk: () => handleDelete(file.id, file.file_name, deleteTableData),
    });
  };

  const confirmBulkDelete = () => {
    if (!selectedFiles.length) {
      messageApi.info('Select at least one file to delete.');
      return;
    }

    const mappedTableNames = Array.from(
      new Set(
        selectedFiles
          .map((file) => file.mapped_table_name)
          .filter((name): name is string => Boolean(name))
      )
    );

    let deleteTableData = false;
    Modal.confirm({
      title: `Delete ${selectedFiles.length} file${selectedFiles.length === 1 ? '' : 's'}`,
      content: (
        <div>
          <p>Files will be removed from storage and the uploads list.</p>
          {mappedTableNames.length > 0 && (
            <Checkbox onChange={(event) => { deleteTableData = event.target.checked; }}>
              Also remove imported rows from mapped tables ({mappedTableNames.join(', ')})
            </Checkbox>
          )}
        </div>
      ),
      okText: 'Delete',
      cancelText: 'Cancel',
      okButtonProps: { danger: true },
      onOk: () => handleBulkDelete(deleteTableData),
    });
  };

  const handleBulkProcess = async () => {
    const blocked = selectedFiles.filter(isJobActive);
    if (blocked.length > 0) {
      messageApi.warning(
        `These files already have jobs queued or running: ${blocked
          .map((file) => file.file_name)
          .join(', ')}`
      );
      return;
    }

    if (!mappableFiles.length) {
      messageApi.info('Select uploads that can be auto-processed.');
      return;
    }

    setLoading(true);
    const token = localStorage.getItem('refine-auth');
    let processed = 0;
    const failures: string[] = [];

    for (const file of mappableFiles) {
      const formData = new FormData();
      formData.append('file_id', file.id);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');

      try {
        const response = await axios.post(`${API_URL}/analyze-file`, formData, {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });
        if (response.data.success) {
          processed += 1;
        } else {
          failures.push(`${file.file_name}: ${response.data.error || 'Processing failed'}`);
        }
      } catch (error: unknown) {
        const errorMsg =
          (axios.isAxiosError(error) && error.response?.data?.detail) ||
          (error instanceof Error ? error.message : null) ||
          'Processing failed';
        failures.push(`${file.file_name}: ${errorMsg}`);
      }
    }

    if (processed) {
      messageApi.success(`Auto-processed ${processed} file(s).`);
    }
    if (failures.length) {
      messageApi.error(`Failed to process ${failures.length} file(s).\n${failures.join('\n')}`);
    }

    setSelectedRowKeys([]);
    await fetchFiles(activeTab, currentPage, pageSize);
    setLoading(false);
  };

  const getStatusBadge = (file: UploadedFile) => {
    const { status } = file;
    const statusConfig: Record<string, { status: 'success' | 'processing' | 'error' | 'default'; text: string }> = {
      uploaded: { status: 'processing', text: 'Uploaded' },
      mapping: { status: 'processing', text: 'Mapping' },
      mapped: { status: 'success', text: 'Mapped' },
      failed: { status: 'error', text: 'Failed' },
    };

    const config = statusConfig[status] || { status: 'default' as const, text: status };
    const jobLabel = file.active_job_status
      ? (file.active_job_stage ?? file.active_job_status)?.replace(/_/g, ' ')
      : null;

    return (
      <Space size={4}>
        <Badge status={config.status} text={config.text} />
        {isJobActive(file) && (
          <Tag color={isJobQueued(file) ? 'gold' : 'blue'}>
            {jobLabel ? jobLabel : 'Processing'}
          </Tag>
        )}
      </Space>
    );
  };

  const columns: ColumnsType<UploadedFile & ImportSummary> = [
    {
      title: 'File Name',
      dataIndex: 'file_name',
      key: 'file_name',
      width: 250,
      ellipsis: true,
      sorter: (a, b) => (a.file_name || '').localeCompare(b.file_name || ''),
    },
    {
      title: 'Size',
      dataIndex: 'file_size',
      key: 'file_size',
      width: 110,
      sorter: (a, b) => (a.file_size || 0) - (b.file_size || 0),
      render: (size: number) => formatBytes(size),
    },
    {
      title: 'Upload Date',
      dataIndex: 'upload_date',
      key: 'upload_date',
      width: 190,
      sorter: (a, b) => {
        const aDate = a.upload_date ? new Date(a.upload_date).getTime() : 0;
        const bDate = b.upload_date ? new Date(b.upload_date).getTime() : 0;
        return aDate - bDate;
      },
      defaultSortOrder: 'descend',
      render: (date: string) => formatDate(date),
    },
    {
      title: 'Status',
      key: 'status',
      width: 180,
      filters: [
        { text: 'Uploaded', value: 'uploaded' },
        { text: 'Mapping', value: 'mapping' },
        { text: 'Failed', value: 'failed' },
        { text: 'Mapped', value: 'mapped' },
      ],
      onFilter: (value, record) => record.status === value,
      sorter: (a, b) => (a.status || '').localeCompare(b.status || ''),
      render: (_: string, record: UploadedFile & ImportSummary) => getStatusBadge(record),
    },
    {
      title: 'Table',
      dataIndex: 'mapped_table_name',
      key: 'mapped_table_name',
      width: 170,
      sorter: (a, b) => (a.mapped_table_name || '').localeCompare(b.mapped_table_name || ''),
      filters: [
        { text: 'Mapped', value: 'mapped' },
        { text: 'Not mapped', value: 'unmapped' },
      ],
      onFilter: (value, record) =>
        value === 'mapped'
          ? !!record.mapped_table_name
          : !record.mapped_table_name,
      render: (tableName?: string) => tableName || '-',
    },
    {
      title: 'Rows',
      dataIndex: 'mapped_rows',
      key: 'mapped_rows',
      width: 110,
      sorter: (a, b) => (a.mapped_rows || 0) - (b.mapped_rows || 0),
      render: (rows?: number) => rows?.toLocaleString() || '-',
    },
    {
      title: 'Duplicates',
      dataIndex: 'duplicates_found',
      key: 'duplicates_found',
      width: 150,
      sorter: (a, b) => (a.duplicates_found ?? 0) - (b.duplicates_found ?? 0),
      filters: [
        { text: 'Has duplicates', value: 'has' },
        { text: 'No duplicates', value: 'none' },
      ],
      onFilter: (value, record) => {
        const duplicates = record.duplicates_found ?? 0;
        return value === 'has' ? duplicates > 0 : duplicates === 0;
      },
      render: (_: number | undefined, record: UploadedFile & ImportSummary) => {
        if (record.status !== 'mapped') {
          return '-';
        }
        const duplicates = record.duplicates_found;
        if (duplicates === undefined) {
          return <Badge status="default" text="Unknown" />;
        }
        const label = `${duplicates.toLocaleString()} duplicate${duplicates === 1 ? '' : 's'}`;
        return <Badge status={duplicates > 0 ? 'warning' : 'success'} text={label} />;
      },
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 220,
      render: (_: unknown, record: UploadedFile & ImportSummary) => (
        <Space size="small">
          {record.status === 'uploaded' && (
            <Button
              type="primary"
              size="small"
              icon={<ThunderboltOutlined />}
              onClick={() => handleMapNow(record)}
              disabled={isJobActive(record)}
            >
              Map Now
            </Button>
          )}
          <Button
            size="small"
            icon={<EyeOutlined />}
            onClick={() => handleView(record)}
          >
            View
          </Button>
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => confirmSingleDelete(record)}
          />
        </Space>
      ),
    },
  ];

  const tabItems = [
    {
      key: 'all',
      label: `All (${tabCounts.all ?? totalCount})`,
    },
    {
      key: 'needs_mapping',
      label: `Needs Mapping (${tabCounts.needs_mapping ?? 0})`,
    },
    {
      key: 'uploaded',
      label: `Uploaded (${tabCounts.uploaded ?? 0})`,
    },
    {
      key: 'mapped',
      label: `Mapped (${tabCounts.mapped ?? 0})`,
    },
  ];

  return (
    <div className="p-6">
      <Card
        title={<span className="text-lg font-bold text-slate-800 dark:text-white">Upload Files</span>}
        className="glass-panel mb-6"
        bordered={false}
      >
        <FileUpload
          onUploadSuccess={() => {
            messageApi.success('File uploaded successfully! Refreshing list...');
            setTimeout(() => fetchFiles(activeTab, currentPage, pageSize), 1000);
          }}
          multiple={true}
        />
      </Card>

      <Card
        title={<span className="text-lg font-bold text-slate-800 dark:text-white">Uploaded Files</span>}
        className="glass-panel"
        bordered={false}
        extra={
          <Button
            icon={<ReloadOutlined />}
            onClick={() => fetchFiles(activeTab, currentPage, pageSize)}
            loading={loading}
            type="text"
            className="hover:text-brand-500"
          >
            Refresh
          </Button>
        }
      >
        <Space style={{ marginBottom: 12 }} wrap>
          <Button
            type="primary"
            disabled={!mappableFiles.length || loading}
            onClick={() => handleBulkProcess()}
          >
            Auto-process selected
          </Button>
          <Button
            disabled={!mappableFiles.length}
            onClick={() => handleBulkOpen(mappableFiles, 'map')}
          >
            Map selected
          </Button>
          <Button
            disabled={!remappableFiles.length}
            onClick={() => handleBulkOpen(remappableFiles, 'remap')}
          >
            Remap selected
          </Button>
          <Button danger disabled={!selectedFiles.length} onClick={confirmBulkDelete}>
            Delete selected
          </Button>
          {selectedFiles.length > 0 && (
            <Button onClick={() => setSelectedRowKeys([])}>Clear selection</Button>
          )}
          <span style={{ marginLeft: 8, color: '#888' }}>
            {selectedFiles.length ? `${selectedFiles.length} selected` : 'No files selected'}
          </span>
        </Space>

        <Tabs
          activeKey={activeTab}
          onChange={(key) => {
            setActiveTab(key);
            setCurrentPage(1);
            setSelectedRowKeys([]);
          }}
          items={tabItems}
          style={{ marginBottom: '16px' }}
        />

        <Table<UploadedFile & ImportSummary>
          columns={columns}
          dataSource={files}
          rowKey="id"
          loading={loading}
          rowSelection={{
            selectedRowKeys,
            onChange: (nextKeys) => setSelectedRowKeys(nextKeys),
            getCheckboxProps: (record) => ({
              disabled: isJobActive(record as UploadedFile),
              title: isJobActive(record as UploadedFile)
                ? 'Processing is already in progress for this file'
                : undefined,
            }),
            preserveSelectedRowKeys: true,
          }}
          pagination={{
            current: currentPage,
            pageSize,
            total: totalCount,
            showSizeChanger: true,
            pageSizeOptions: ['20', '50', '100'],
            showTotal: (total) => `Total ${total} files`,
            onChange: handlePaginationChange,
            onShowSizeChange: handlePageSizeChange,
          }}
        />
      </Card>
    </div>
  );
};

export default ImportPage;
