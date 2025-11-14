import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router';
import { App as AntdApp, Card, Table, Tabs, Badge, Button, Space, Popconfirm } from 'antd';
import { ReloadOutlined, DeleteOutlined, EyeOutlined, ThunderboltOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { FileUpload } from '../../components/file-upload';
import axios from 'axios';
import { API_URL } from '../../config';

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
  const [totalCount, setTotalCount] = useState(0);
  const { message: messageApi } = AntdApp.useApp();

  const fetchFiles = async (status?: string) => {
    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const params: { status?: string; limit: number; offset: number } = {
        limit: 100,
        offset: 0,
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
      }
    } catch (error) {
      messageApi.error('Failed to fetch files');
      console.error('Error fetching files:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchFiles(activeTab);
    
    // Auto-refresh every 10 seconds
    const interval = setInterval(() => {
      fetchFiles(activeTab);
    }, 10000);

    return () => clearInterval(interval);
  }, [activeTab]);

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
        fetchFiles(activeTab);
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
            setTimeout(() => fetchFiles(activeTab), 1000);
          }}
          multiple={true}
        />
      </Card>

      <Card
        title="Uploaded Files"
        extra={
          <Button
            icon={<ReloadOutlined />}
            onClick={() => fetchFiles(activeTab)}
            loading={loading}
          >
            Refresh
          </Button>
        }
      >
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={tabItems}
          style={{ marginBottom: '16px' }}
        />

        <Table
          columns={columns}
          dataSource={files}
          rowKey="id"
          loading={loading}
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showTotal: (total) => `Total ${total} files`,
          }}
        />
      </Card>
    </div>
  );
};

export default ImportPage;
