import React, { useEffect, useState } from 'react';
import { App as AntdApp, Modal, Typography, Alert, Space, Button } from 'antd';
import { CopyOutlined, CheckOutlined } from '@ant-design/icons';
import type { CreateKeyResponse } from './types';
import { API_URL } from '../../config';

const { Text, Paragraph } = Typography;

interface RevealKeyModalProps {
  visible: boolean;
  apiKey: CreateKeyResponse | null;
  onClose: () => void;
  onDismiss: () => void;
}

export const RevealKeyModal: React.FC<RevealKeyModalProps> = ({
  visible,
  apiKey,
  onClose,
  onDismiss,
}) => {
  const [copied, setCopied] = useState(false);
  const { message: messageApi } = AntdApp.useApp();

  useEffect(() => {
    if (!visible) {
      setCopied(false);
    }
  }, [visible]);

  if (!apiKey) {
    return null;
  }

  const handleCopy = async () => {
    if (!apiKey.api_key) {
      return;
    }

    try {
      await navigator.clipboard.writeText(apiKey.api_key);
      setCopied(true);
      messageApi.success('API key copied to clipboard!');
      setTimeout(() => setCopied(false), 2000);
    } catch {
      messageApi.error('Failed to copy to clipboard');
    }
  };

  return (
    <Modal
      title="Your API Key"
      open={visible}
      onCancel={onClose}
      width={600}
      footer={[
        <Button
          key="dismiss"
          onClick={() => {
            onDismiss();
            onClose();
          }}
        >
          Dismiss Reminder
        </Button>,
        <Button key="close" type="primary" onClick={onClose}>
          Close
        </Button>,
      ]}
    >
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Alert
          message="Here's your API key again"
          description="Make sure to copy and store it securely. This key will remain visible here until you dismiss the reminder."
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
            {apiKey.api_key}
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
            <Text strong>App Name:</Text> {apiKey.app_name}
          </Paragraph>
          {apiKey.expires_at && (
            <Paragraph>
              <Text strong>Expires:</Text>{' '}
              {new Date(apiKey.expires_at).toLocaleString()}
            </Paragraph>
          )}
        </div>
      </Space>
    </Modal>
  );
};
