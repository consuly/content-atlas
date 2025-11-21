import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { useNavigate } from 'react-router';
import { App as AntdApp, Card, Table, Tabs, Badge, Button, Space, Popconfirm } from 'antd';
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

    const mappedFilesNeedingSummary = fileList.filter(
      (file) => file.status === 'mapped' && !importSummaryCache[file.id]
    );
    let mergedCache = importSummaryCache;

    if (mappedFilesNeedingSummary.length > 0) {
      const summaries = await Promise.all(
        mappedFilesNeedingSummary.map(async (file) => {
          try {
            const params: Record<string, unknown> = { file_name: file.file_name, limit: 1 };
            if (typeof file.file_size === 'number') {
              params.file_size_bytes = file.file_size;
            }
            if (file.b2_file_path) {
              params.source_path = file.b2_file_path;
            }

            let response = await axios.get(`${API_URL}/import-history`, {
              params,
              headers,
            });

            let importRecord = response.data.success && response.data.imports.length > 0
              ? response.data.imports[0]
              : null;

            if (!importRecord && file.mapped_table_name) {
              response = await axios.get(`${API_URL}/import-history`, {
                params: { table_name: file.mapped_table_name, limit: 1 },
                headers,
              });
              importRecord =
                response.data.success && response.data.imports.length > 0
                  ? response.data.imports[0]
                  : null;
            }

            if (importRecord) {
              return {
                fileId: file.id,
                summary: {
                  import_id: importRecord.import_id,
                  duplicates_found: importRecord.duplicates_found ?? 0,
                },
              };
            }
          } catch (err) {
            console.error(`Error fetching import history for ${file.file_name}:`, err);
          }

          return { fileId: file.id, summary: { duplicates_found: undefined } };
        })
      );

      const updates: Record<string, ImportSummary> = {};
      summaries.forEach(({ fileId, summary }) => {
        updates[fileId] = summary;
      });

      mergedCache = { ...importSummaryCache, ...updates };
      setImportSummaryCache(mergedCache);
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
        setSelectedRowKeys((prev) =>
          prev.filter((key) => filesWithImports.some((file: UploadedFile) => file.id === key))
        );
      } catch (error) {
        messageApi.error('Failed to fetch files');
        console.error('Error fetching files:', error);
      } finally {
        setLoading(false);
      }
    },
    [activeTab, attachImportSummaries, currentPage, messageApi, pageSize, statusGroups]
  );

  useEffect(() => {
    fetchFiles(activeTab, currentPage, pageSize);
    
    // Auto-refresh every 10 seconds
    const interval = setInterval(() => {
      fetchFiles(activeTab, currentPage, pageSize);
    }, 10000);

    return () => clearInterval(interval);
  }, [activeTab, currentPage, pageSize, fetchFiles]);

  const handleDelete = async (fileId: string, fileName: string) => {
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.delete(`${API_URL}/uploaded-files/${fileId}`, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        messageApi.success(`${fileName} deleted successfully`);
        fetchFiles(activeTab, currentPage, pageSize);
      }
    } catch (error) {
      messageApi.error(`Failed to delete ${fileName}`);
      console.error('Error deleting file:', error);
    }
  };

  const handleMapNow = (file: UploadedFile) => {
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
  const mappableFiles = selectedFiles.filter((file) =>
    ['uploaded', 'failed', 'mapping'].includes(file.status)
  );
  const remappableFiles = selectedFiles.filter((file) => file.status === 'mapped');

  const handleBulkOpen = (targets: UploadedFile[], actionLabel: string) => {
    if (!targets.length) {
      messageApi.info(`Select files that can be ${actionLabel}.`);
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

  const handleBulkDelete = async () => {
    if (!selectedFiles.length) {
      messageApi.info('Select at least one file to delete.');
      return;
    }
    setLoading(true);
    const token = localStorage.getItem('refine-auth');
    let deleted = 0;
    let failed = 0;

    for (const file of selectedFiles) {
      try {
        const response = await axios.delete(`${API_URL}/uploaded-files/${file.id}`, {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        });
        if (response.data.success) {
          deleted += 1;
        } else {
          failed += 1;
        }
      } catch (error) {
        failed += 1;
        console.error(`Error deleting file ${file.file_name}:`, error);
      }
    }

    if (deleted) {
      messageApi.success(`Deleted ${deleted} file(s).`);
    }
    if (failed) {
      messageApi.error(`Failed to delete ${failed} file(s).`);
    }

    setSelectedRowKeys([]);
    await fetchFiles(activeTab, currentPage, pageSize);
    setLoading(false);
  };

  const handleBulkProcess = async () => {
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
    const extra = file.active_job_status
      ? ` (${file.active_job_stage ?? file.active_job_status})`
      : '';
    return <Badge status={config.status} text={`${config.text}${extra}`} />;
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
          <Popconfirm
            title="Delete file"
            description={`Are you sure you want to delete ${record.file_name}?`}
            onConfirm={() => handleDelete(record.id, record.file_name)}
            okText="Yes"
            cancelText="No"
          >
            <Button
              size="small"
              danger
              icon={<DeleteOutlined />}
            />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const tabItems = [
    {
      key: 'all',
      label: `All (${tabCounts.all || totalCount})`,
    },
    {
      key: 'needs_mapping',
      label: `Needs Mapping (${tabCounts.needs_mapping || 0})`,
    },
    {
      key: 'uploaded',
      label: `Uploaded (${tabCounts.uploaded || 0})`,
    },
    {
      key: 'mapped',
      label: `Mapped (${tabCounts.mapped || 0})`,
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Card
        title="Upload Files"
        style={{ marginBottom: '24px' }}
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
        title="Uploaded Files"
        extra={
          <Button
            icon={<ReloadOutlined />}
            onClick={() => fetchFiles(activeTab, currentPage, pageSize)}
            loading={loading}
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
          <Popconfirm
            title="Delete selected files"
            description="Are you sure you want to delete the selected files?"
            onConfirm={handleBulkDelete}
            okText="Yes"
            cancelText="No"
            disabled={!selectedFiles.length}
          >
            <Button danger disabled={!selectedFiles.length}>
              Delete selected
            </Button>
          </Popconfirm>
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
