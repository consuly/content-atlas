import React, { useState, useEffect } from 'react';
import { App as AntdApp, Card, Table, Tabs, Badge, Button, Space, Popconfirm, Tooltip } from 'antd';
import {
  ReloadOutlined,
  DeleteOutlined,
  EditOutlined,
  PlusOutlined,
  StopOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import axios from 'axios';
import { CreateKeyModal } from './CreateKeyModal';
import { UpdateKeyModal } from './UpdateKeyModal';
import { RevealKeyModal } from './RevealKeyModal';
import type { ApiKey, CreateKeyResponse } from './types';
import { API_URL } from '../../config';

export const ApiKeysPage: React.FC = () => {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState<string>('all');
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [updateModalVisible, setUpdateModalVisible] = useState(false);
  const [selectedKey, setSelectedKey] = useState<ApiKey | null>(null);
  const [recentlyCreatedKey, setRecentlyCreatedKey] = useState<CreateKeyResponse | null>(null);
  const [showRevealModal, setShowRevealModal] = useState(false);
  const { message: messageApi } = AntdApp.useApp();

  const fetchKeys = async (status?: string) => {
    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const params: Record<string, string> = {};

      if (status === 'active') {
        params.is_active = 'true';
      } else if (status === 'revoked') {
        params.is_active = 'false';
      } else if (status === 'expired') {
        params.expired = 'true';
      }

      const response = await axios.get(`${API_URL}/admin/api-keys`, {
        params,
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      setKeys(response.data.api_keys || []);
    } catch (error) {
      messageApi.error('Failed to fetch API keys');
      console.error('Error fetching API keys:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchKeys(activeTab);
  }, [activeTab]);

  const handleDelete = async (keyId: string, appName: string) => {
    try {
      const token = localStorage.getItem('refine-auth');
      await axios.delete(`${API_URL}/admin/api-keys/${keyId}`, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      messageApi.success(`API key for ${appName} deleted successfully`);
      fetchKeys(activeTab);
    } catch (error) {
      messageApi.error(`Failed to delete API key for ${appName}`);
      console.error('Error deleting API key:', error);
    }
  };

  const handleRevoke = async (keyId: string, appName: string) => {
    try {
      const token = localStorage.getItem('refine-auth');
      await axios.patch(
        `${API_URL}/admin/api-keys/${keyId}`,
        { is_active: false },
        {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      messageApi.success(`API key for ${appName} revoked successfully`);
      fetchKeys(activeTab);
    } catch (error) {
      messageApi.error(`Failed to revoke API key for ${appName}`);
      console.error('Error revoking API key:', error);
    }
  };

  const handleEdit = (key: ApiKey) => {
    setSelectedKey(key);
    setUpdateModalVisible(true);
  };

  const formatDate = (dateString?: string): string => {
    if (!dateString) return 'Never';
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins} min${diffMins > 1 ? 's' : ''} ago`;
    if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`;
    if (diffDays < 7) return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;
    return date.toLocaleDateString();
  };

  const formatExpirationDate = (dateString?: string): string => {
    if (!dateString) return 'Never';
    return new Date(dateString).toLocaleString();
  };

  const isExpired = (dateString?: string): boolean => {
    if (!dateString) return false;
    return new Date(dateString) < new Date();
  };

  const isExpiringSoon = (dateString?: string): boolean => {
    if (!dateString) return false;
    const expiryDate = new Date(dateString);
    const now = new Date();
    const daysUntilExpiry = (expiryDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24);
    return daysUntilExpiry > 0 && daysUntilExpiry <= 7;
  };

  const getStatusBadge = (key: ApiKey) => {
    if (!key.is_active) {
      return <Badge status="error" text="Revoked" />;
    }
    if (isExpired(key.expires_at)) {
      return <Badge status="warning" text="Expired" />;
    }
    if (isExpiringSoon(key.expires_at)) {
      return (
        <Tooltip title="Expires within 7 days">
          <Badge status="processing" text="Active (Expiring Soon)" />
        </Tooltip>
      );
    }
    return <Badge status="success" text="Active" />;
  };

  const columns: ColumnsType<ApiKey> = [
    {
      title: 'Application Name',
      dataIndex: 'app_name',
      key: 'app_name',
      width: 200,
      ellipsis: true,
    },
    {
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
      width: 250,
      ellipsis: true,
      render: (desc?: string) => desc || '-',
    },
    {
      title: 'Status',
      key: 'status',
      width: 180,
      render: (_: unknown, record: ApiKey) => getStatusBadge(record),
    },
    {
      title: 'Rate Limit',
      dataIndex: 'rate_limit_per_minute',
      key: 'rate_limit_per_minute',
      width: 120,
      render: (limit: number) => `${limit} req/min`,
    },
    {
      title: 'Created',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 150,
      render: (date: string) => formatDate(date),
    },
    {
      title: 'Last Used',
      dataIndex: 'last_used_at',
      key: 'last_used_at',
      width: 150,
      render: (date?: string) => formatDate(date),
    },
    {
      title: 'Expires',
      dataIndex: 'expires_at',
      key: 'expires_at',
      width: 180,
      render: (date?: string) => {
        if (!date) return 'Never';
        const expired = isExpired(date);
        const expiringSoon = isExpiringSoon(date);
        return (
          <Tooltip title={formatExpirationDate(date)}>
            <span style={{ color: expired ? '#ff4d4f' : expiringSoon ? '#faad14' : undefined }}>
              {expired && <ClockCircleOutlined style={{ marginRight: 4 }} />}
              {formatDate(date)}
            </span>
          </Tooltip>
        );
      },
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 200,
      fixed: 'right',
      render: (_: unknown, record: ApiKey) => (
        <Space size="small">
          <Tooltip title="Edit">
            <Button
              size="small"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
          {record.is_active && (
            <Popconfirm
              title="Revoke API key"
              description={`Are you sure you want to revoke the key for ${record.app_name}?`}
              onConfirm={() => handleRevoke(record.id, record.app_name)}
              okText="Yes"
              cancelText="No"
            >
              <Tooltip title="Revoke">
                <Button
                  size="small"
                  icon={<StopOutlined />}
                  danger
                />
              </Tooltip>
            </Popconfirm>
          )}
          <Popconfirm
            title="Delete API key"
            description={`Are you sure you want to permanently delete the key for ${record.app_name}?`}
            onConfirm={() => handleDelete(record.id, record.app_name)}
            okText="Yes"
            cancelText="No"
          >
            <Tooltip title="Delete">
              <Button
                size="small"
                icon={<DeleteOutlined />}
                danger
              />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  const tabItems = [
    {
      key: 'all',
      label: `All (${keys.length})`,
    },
    {
      key: 'active',
      label: 'Active',
    },
    {
      key: 'revoked',
      label: 'Revoked',
    },
    {
      key: 'expired',
      label: 'Expired',
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      {recentlyCreatedKey && (
        <Card
          type="inner"
          title="Don't forget to configure your new API key"
          style={{ marginBottom: 16 }}
          extra={
            <Space>
              <Button type="primary" onClick={() => setShowRevealModal(true)}>
                Show API Key Again
              </Button>
              <Button onClick={() => setRecentlyCreatedKey(null)}>Dismiss</Button>
            </Space>
          }
        >
          The base URL for this project is{' '}
          <strong>{API_URL}</strong>. Use it together with the API key when setting
          up another application.
        </Card>
      )}
      <Card
        title="API Keys Management"
        extra={
          <Space>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalVisible(true)}
            >
              Create New Key
            </Button>
            <Button
              icon={<ReloadOutlined />}
              onClick={() => fetchKeys(activeTab)}
              loading={loading}
            >
              Refresh
            </Button>
          </Space>
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
          dataSource={keys}
          rowKey="id"
          loading={loading}
          scroll={{ x: 1400 }}
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showTotal: (total) => `Total ${total} keys`,
          }}
        />
      </Card>

      <CreateKeyModal
        visible={createModalVisible}
        onClose={() => setCreateModalVisible(false)}
        onSuccess={() => fetchKeys(activeTab)}
        onKeyCreated={(key) => {
          setRecentlyCreatedKey(key);
        }}
      />

      <UpdateKeyModal
        visible={updateModalVisible}
        apiKey={selectedKey}
        onClose={() => {
          setUpdateModalVisible(false);
          setSelectedKey(null);
        }}
        onSuccess={() => fetchKeys(activeTab)}
      />
      <RevealKeyModal
        visible={showRevealModal && !!recentlyCreatedKey}
        apiKey={recentlyCreatedKey}
        onClose={() => setShowRevealModal(false)}
        onDismiss={() => setRecentlyCreatedKey(null)}
      />
    </div>
  );
};

export default ApiKeysPage;
