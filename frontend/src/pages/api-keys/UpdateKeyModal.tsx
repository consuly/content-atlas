import React, { useState, useEffect } from 'react';
import { Modal, Form, Input, InputNumber, Switch, Button, message, DatePicker } from 'antd';
import axios from 'axios';
import dayjs from 'dayjs';
import type { ApiKey, UpdateKeyRequest } from './types';

const { TextArea } = Input;

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

  useEffect(() => {
    if (apiKey && visible) {
      form.setFieldsValue({
        description: apiKey.description,
        rate_limit_per_minute: apiKey.rate_limit_per_minute,
        is_active: apiKey.is_active,
        expires_at: apiKey.expires_at ? dayjs(apiKey.expires_at) : null,
      });
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
      )}
    </Modal>
  );
};
