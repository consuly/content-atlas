import React, { useState } from 'react';
import { App as AntdApp, Modal, Form, Input, InputNumber, Select, Button, Alert, Space, Typography } from 'antd';
import { CopyOutlined, CheckOutlined } from '@ant-design/icons';
import axios from 'axios';
import type { CreateKeyRequest, CreateKeyResponse } from './types';
import { saveApiKeySecret } from './apiKeyStorage';

const { TextArea } = Input;
const { Text, Paragraph } = Typography;

interface CreateKeyModalProps {
  visible: boolean;
  onClose: () => void;
  onSuccess: () => void;
  onKeyCreated?: (key: CreateKeyResponse) => void;
}

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const CreateKeyModal: React.FC<CreateKeyModalProps> = ({
  visible,
  onClose,
  onSuccess,
  onKeyCreated,
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [createdKey, setCreatedKey] = useState<CreateKeyResponse | null>(null);
  const [copied, setCopied] = useState(false);
  const { message: messageApi } = AntdApp.useApp();

  const handleSubmit = async (values: CreateKeyRequest) => {
    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post<CreateKeyResponse>(
        `${API_URL}/admin/api-keys`,
        values,
        {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      setCreatedKey(response.data);
      saveApiKeySecret(response.data.key_id, response.data.api_key, response.data.app_name);
      onKeyCreated?.(response.data);
      messageApi.success('API key created successfully!');
      // Refresh the list immediately after creation
      onSuccess();
    } catch (error) {
      messageApi.error('Failed to create API key');
      console.error('Error creating API key:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = async () => {
    if (createdKey?.api_key) {
      try {
        await navigator.clipboard.writeText(createdKey.api_key);
        setCopied(true);
        messageApi.success('API key copied to clipboard!');
        setTimeout(() => setCopied(false), 2000);
      } catch {
        messageApi.error('Failed to copy to clipboard');
      }
    }
  };

  const handleClose = () => {
    form.resetFields();
    setCreatedKey(null);
    setCopied(false);
    onClose();
  };

  return (
    <Modal
      title={createdKey ? 'API Key Created' : 'Create New API Key'}
      open={visible}
      onCancel={handleClose}
      footer={
        createdKey ? (
          <Button type="primary" onClick={handleClose}>
            Done
          </Button>
        ) : (
          [
            <Button key="cancel" onClick={handleClose}>
              Cancel
            </Button>,
            <Button
              key="submit"
              type="primary"
              loading={loading}
              onClick={() => form.submit()}
            >
              Create Key
            </Button>,
          ]
        )
      }
      width={600}
    >
      {createdKey ? (
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <Alert
            message="Important: Save this API key now!"
            description="This is the only time you will see this key. Make sure to copy it and store it securely."
            type="warning"
            showIcon
          />

          <div>
            <Text strong>API Key:</Text>
            <div
              style={{
                marginTop: 8,
                padding: 12,
                background: '#f5f5f5',
                borderRadius: 4,
                fontFamily: 'monospace',
                wordBreak: 'break-all',
              }}
            >
              {createdKey.api_key}
            </div>
            <Button
              icon={copied ? <CheckOutlined /> : <CopyOutlined />}
              onClick={handleCopy}
              style={{ marginTop: 8 }}
              type={copied ? 'default' : 'primary'}
            >
              {copied ? 'Copied!' : 'Copy to Clipboard'}
            </Button>
          </div>

          <div>
            <Text strong>Base URL:</Text>
            <div
              style={{
                marginTop: 8,
                padding: 12,
                background: '#f5f5f5',
                borderRadius: 4,
                fontFamily: 'monospace',
                wordBreak: 'break-all',
              }}
            >
              {API_URL}
            </div>
          </div>

          <div>
            <Paragraph>
              <Text strong>App Name:</Text> {createdKey.app_name}
            </Paragraph>
            {createdKey.expires_at && (
              <Paragraph>
                <Text strong>Expires:</Text> {new Date(createdKey.expires_at).toLocaleString()}
              </Paragraph>
            )}
            <Paragraph type="secondary" style={{ marginTop: 16 }}>
              The new API key has been added to your keys list.
            </Paragraph>
          </div>
        </Space>
      ) : (
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSubmit}
          initialValues={{
            rate_limit_per_minute: 100,
            expires_in_days: 365,
          }}
        >
          <Form.Item
            name="app_name"
            label="Application Name"
            rules={[
              { required: true, message: 'Please enter an application name' },
              { min: 3, message: 'Name must be at least 3 characters' },
            ]}
          >
            <Input placeholder="e.g., Mobile App, Dashboard, Analytics Service" />
          </Form.Item>

          <Form.Item
            name="description"
            label="Description"
          >
            <TextArea
              rows={3}
              placeholder="Optional description of what this key will be used for"
            />
          </Form.Item>

          <Form.Item
            name="rate_limit_per_minute"
            label="Rate Limit (requests per minute)"
            rules={[
              { required: true, message: 'Please enter a rate limit' },
            ]}
          >
            <InputNumber
              min={1}
              max={1000}
              style={{ width: '100%' }}
            />
          </Form.Item>

          <Form.Item
            name="expires_in_days"
            label="Expires In (days)"
            rules={[
              { required: true, message: 'Please enter expiration days' },
            ]}
          >
            <InputNumber
              min={1}
              max={3650}
              style={{ width: '100%' }}
            />
          </Form.Item>

          <Form.Item
            name="allowed_endpoints"
            label="Allowed Endpoints (optional)"
            tooltip="Leave empty to allow all endpoints"
          >
            <Select
              mode="tags"
              placeholder="e.g., /api/v1/query, /api/v1/tables"
              style={{ width: '100%' }}
            />
          </Form.Item>

          <Alert
            message="Security Note"
            description="The API key will only be shown once after creation. Make sure to copy and store it securely."
            type="info"
            showIcon
            style={{ marginTop: 16 }}
          />
        </Form>
      )}
    </Modal>
  );
};
