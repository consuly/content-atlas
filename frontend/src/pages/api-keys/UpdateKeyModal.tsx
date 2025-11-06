import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  InputNumber,
  Switch,
  Button,
  message,
  DatePicker,
  Alert,
  Space,
  Typography,
} from 'antd';
import { CopyOutlined, CheckOutlined } from '@ant-design/icons';
import axios from 'axios';
import dayjs from 'dayjs';
import type { ApiKey, UpdateKeyRequest } from './types';
import { getStoredApiKey } from './apiKeyStorage';

const { TextArea } = Input;
const { Text, Paragraph } = Typography;

interface UpdateKeyModalProps {
  visible: boolean;
  apiKey: ApiKey | null;
  onClose: () => void;
  onSuccess: () => void;
}

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const UpdateKeyModal: React.FC<UpdateKeyModalProps> = ({
  visible,
  apiKey,
  onClose,
  onSuccess,
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [storedApiKey, setStoredApiKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (apiKey && visible) {
      form.setFieldsValue({
        description: apiKey.description,
        rate_limit_per_minute: apiKey.rate_limit_per_minute,
        is_active: apiKey.is_active,
        expires_at: apiKey.expires_at ? dayjs(apiKey.expires_at) : null,
      });
      setStoredApiKey(getStoredApiKey(apiKey.id));
      setCopied(false);
    } else if (!visible) {
      setStoredApiKey(null);
      setCopied(false);
    }
  }, [apiKey, visible, form]);

  const handleSubmit = async (values: UpdateKeyRequest & { expires_at?: dayjs.Dayjs }) => {
    if (!apiKey) return;

    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const payload: UpdateKeyRequest = {
        description: values.description,
        rate_limit_per_minute: values.rate_limit_per_minute,
        is_active: values.is_active,
        expires_at: values.expires_at ? values.expires_at.toISOString() : undefined,
      };

      await axios.patch(
        `${API_URL}/admin/api-keys/${apiKey.id}`,
        payload,
        {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      message.success('API key updated successfully!');
      onSuccess();
      handleClose();
    } catch (error) {
      message.error('Failed to update API key');
      console.error('Error updating API key:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    form.resetFields();
    onClose();
  };

  const handleCopy = async () => {
    if (!storedApiKey) {
      return;
    }

    try {
      await navigator.clipboard.writeText(storedApiKey);
      setCopied(true);
      message.success('API key copied to clipboard!');
      setTimeout(() => setCopied(false), 2000);
    } catch {
      message.error('Failed to copy to clipboard');
    }
  };

  return (
    <Modal
      title="Update API Key"
      open={visible}
      onCancel={handleClose}
      footer={[
        <Button key="cancel" onClick={handleClose}>
          Cancel
        </Button>,
        <Button
          key="submit"
          type="primary"
          loading={loading}
          onClick={() => form.submit()}
        >
          Update
        </Button>,
      ]}
      width={600}
    >
      {apiKey && (
        <>
          <Space direction="vertical" size="large" style={{ width: '100%', marginBottom: 16 }}>
            {storedApiKey ? (
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
                  {storedApiKey}
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
            ) : (
              <Alert
                message="API key not available"
                description="This API key was not generated in this browser, so the secret can't be shown. Create a new key if you need the plain value."
                type="info"
                showIcon
              />
            )}

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
              <Paragraph type="secondary" style={{ marginTop: 8 }}>
                Use this base URL together with the API key when configuring external integrations.
              </Paragraph>
            </div>
          </Space>

          <Form
            form={form}
            layout="vertical"
            onFinish={handleSubmit}
          >
            <Form.Item label="Application Name">
              <Input value={apiKey.app_name} disabled />
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
              name="expires_at"
              label="Expiration Date"
            >
              <DatePicker
                showTime
                style={{ width: '100%' }}
                format="YYYY-MM-DD HH:mm:ss"
              />
            </Form.Item>

            <Form.Item
              name="is_active"
              label="Active Status"
              valuePropName="checked"
            >
              <Switch checkedChildren="Active" unCheckedChildren="Inactive" />
            </Form.Item>
          </Form>
        </>
      )}
    </Modal>
  );
};
