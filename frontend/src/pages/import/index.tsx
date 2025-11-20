import React, { useState, useEffect } from 'react';
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
export const ImportPage: React.FC = () => {
  const navigate = useNavigate();
  const [files, setFiles] = useState<UploadedFile[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<string>('all');
  const [selectedRowKeys, setSelectedRowKeys] = useState<Key[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const { message: messageApi } = AntdApp.useApp();

  const fetchFiles = async (status: string = activeTab, page: number = currentPage, size: number = pageSize) => {
    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const params: { status?: string; limit: number; offset: number } = {
        limit: size,
        offset: (page - 1) * size,
      };
      
      if (status && status !== 'all') {
        params.status = status;
      }

      const response = await axios.get(`${API_URL}/uploaded-files`, {
        params,
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        setFiles(response.data.files);
        setTotalCount(response.data.total_count);
        setSelectedRowKeys((prev) =>
          prev.filter((key) => response.data.files.some((file: UploadedFile) => file.id === key))
        );
      }
    } catch (error) {
      messageApi.error('Failed to fetch files');
      console.error('Error fetching files:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchFiles(activeTab, currentPage, pageSize);
    
    // Auto-refresh every 10 seconds
    const interval = setInterval(() => {
      fetchFiles(activeTab, currentPage, pageSize);
    }, 10000);

    return () => clearInterval(interval);
  }, [activeTab, currentPage, pageSize]);

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

  const columns: ColumnsType<UploadedFile> = [
    {
      title: 'File Name',
      dataIndex: 'file_name',
      key: 'file_name',
      width: 250,
      ellipsis: true,
    },
    {
      title: 'Size',
      dataIndex: 'file_size',
      key: 'file_size',
      width: 100,
      render: (size: number) => formatBytes(size),
    },
    {
      title: 'Upload Date',
      dataIndex: 'upload_date',
      key: 'upload_date',
      width: 180,
      render: (date: string) => formatDate(date),
    },
    {
      title: 'Status',
      key: 'status',
      width: 160,
      render: (_: string, record: UploadedFile) => getStatusBadge(record),
    },
    {
      title: 'Table',
      dataIndex: 'mapped_table_name',
      key: 'mapped_table_name',
      width: 150,
      render: (tableName?: string) => tableName || '-',
    },
    {
      title: 'Rows',
      dataIndex: 'mapped_rows',
      key: 'mapped_rows',
      width: 100,
      render: (rows?: number) => rows?.toLocaleString() || '-',
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 200,
      render: (_: unknown, record: UploadedFile) => (
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
      label: `All (${totalCount})`,
    },
    {
      key: 'uploaded',
      label: 'Uploaded',
    },
    {
      key: 'mapped',
      label: 'Mapped',
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

        <Table
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
