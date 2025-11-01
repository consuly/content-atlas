import React, { useState } from 'react';
import { useNavigate } from 'react-router';
import { Modal, Tabs, Button, Space, Alert, Spin, Typography, message } from 'antd';
import { ThunderboltOutlined, MessageOutlined, CheckCircleOutlined } from '@ant-design/icons';
import axios, { AxiosError } from 'axios';
import { ErrorLogViewer } from '../error-log-viewer';

const { Text, Paragraph } = Typography;
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface MappingModalProps {
  visible: boolean;
  fileId: string;
  fileName: string;
  onClose: () => void;
  onSuccess: () => void;
}

export const MappingModal: React.FC<MappingModalProps> = ({
  visible,
  fileId,
  fileName,
  onClose,
  onSuccess,
}) => {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<string>('auto');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [errorDetails, setErrorDetails] = useState<Record<string, unknown> | null>(null);
  
  // Interactive mode state
  const [threadId, setThreadId] = useState<string | null>(null);
  const [conversation, setConversation] = useState<Array<{ role: 'user' | 'assistant'; content: string }>>([]);
  const [userInput, setUserInput] = useState('');
  const [canExecute, setCanExecute] = useState(false);

  const handleAutoProcess = async () => {
    setLoading(true);
    setError(null);
    setErrorDetails(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const formData = new FormData();
      formData.append('file_id', fileId);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');

      const response = await axios.post(`${API_URL}/analyze-file`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        message.success('File mapped successfully!');
        // Trigger parent refresh and close modal
        onSuccess();
        onClose();
      } else {
        setError(response.data.error || 'Processing failed');
        setErrorDetails(response.data.error_details || null);
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string; error_details?: Record<string, unknown> }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Processing failed';
      setError(errorMsg);
      setErrorDetails(error.response?.data?.error_details || null);
      message.error(errorMsg);
    } finally {
      setLoading(false);
    }
  };

  const handleInteractiveStart = async () => {
    setLoading(true);
    setError(null);
    setConversation([]);

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: fileId,
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
        setThreadId(response.data.thread_id);
        setConversation([
          { role: 'assistant', content: response.data.llm_message },
        ]);
        setCanExecute(response.data.can_execute);
      } else {
        setError(response.data.error || 'Analysis failed');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Analysis failed';
      setError(errorMsg);
      message.error(errorMsg);
    } finally {
      setLoading(false);
    }
  };

  const handleInteractiveSend = async () => {
    if (!userInput.trim() || !threadId) return;

    setLoading(true);
    setError(null);

    // Add user message to conversation
    const newConversation = [...conversation, { role: 'user' as const, content: userInput }];
    setConversation(newConversation);
    setUserInput('');

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: fileId,
          user_message: userInput,
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
        setConversation([
          ...newConversation,
          { role: 'assistant', content: response.data.llm_message },
        ]);
        setCanExecute(response.data.can_execute);
      } else {
        setError(response.data.error || 'Analysis failed');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Analysis failed';
      setError(errorMsg);
      message.error(errorMsg);
    } finally {
      setLoading(false);
    }
  };

  const handleInteractiveExecute = async () => {
    if (!threadId) return;

    setLoading(true);
    setError(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/execute-interactive-import`,
        {
          file_id: fileId,
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
        message.success('Import executed successfully!');
        onSuccess();
        onClose();
        // Small delay to ensure backend has updated file status
        setTimeout(() => {
          navigate(`/import/${fileId}`, { replace: true });
          window.location.reload();
        }, 500);
      } else {
        setError('Import execution failed');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Import execution failed';
      setError(errorMsg);
      message.error(errorMsg);
    } finally {
      setLoading(false);
    }
  };

  const handleModalClose = () => {
    // Reset state
    setActiveTab('auto');
    setLoading(false);
    setError(null);
    setThreadId(null);
    setConversation([]);
    setUserInput('');
    setCanExecute(false);
    onClose();
  };

  const autoTabContent = (
    <div style={{ padding: '24px 0' }}>
      <Alert
        message="Automatic Processing"
        description="The AI will analyze your file, compare it with existing tables, and automatically import the data without asking questions. This is the fastest option."
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

      {error && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer
            error={error}
            errorDetails={errorDetails || undefined}
            onRetry={() => {
              setError(null);
              setErrorDetails(null);
              handleAutoProcess();
            }}
            showRetry={true}
          />
        </div>
      )}

      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <div>
          <Text strong>File: </Text>
          <Text>{fileName}</Text>
        </div>

        <Button
          type="primary"
          size="large"
          icon={<ThunderboltOutlined />}
          onClick={handleAutoProcess}
          loading={loading}
          block
        >
          {loading ? 'Processing...' : 'Process Now'}
        </Button>
      </Space>
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

      {error && (
        <Alert
          message="Error"
          description={error}
          type="error"
          showIcon
          closable
          onClose={() => setError(null)}
          style={{ marginBottom: 24 }}
        />
      )}

      {conversation.length === 0 ? (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div>
            <Text strong>File: </Text>
            <Text>{fileName}</Text>
          </div>

          <Button
            type="primary"
            size="large"
            icon={<MessageOutlined />}
            onClick={handleInteractiveStart}
            loading={loading}
            block
          >
            {loading ? 'Starting...' : 'Start Interactive Analysis'}
          </Button>
        </Space>
      ) : (
        <div>
          {/* Conversation history */}
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
            {loading && (
              <div style={{ textAlign: 'center', padding: 16 }}>
                <Spin />
              </div>
            )}
          </div>

          {/* Input area or execute button */}
          {canExecute ? (
            <Button
              type="primary"
              size="large"
              icon={<CheckCircleOutlined />}
              onClick={handleInteractiveExecute}
              loading={loading}
              block
            >
              {loading ? 'Executing...' : 'Execute Import'}
            </Button>
          ) : (
            <Space.Compact style={{ width: '100%' }}>
              <input
                type="text"
                value={userInput}
                onChange={(e) => setUserInput(e.target.value)}
                onKeyPress={(e) => {
                  if (e.key === 'Enter' && !loading) {
                    handleInteractiveSend();
                  }
                }}
                placeholder="Type your response..."
                disabled={loading}
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
                loading={loading}
                disabled={!userInput.trim()}
                style={{ borderRadius: '0 4px 4px 0' }}
              >
                Send
              </Button>
            </Space.Compact>
          )}
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
    <Modal
      title={`Map File: ${fileName}`}
      open={visible}
      onCancel={handleModalClose}
      footer={null}
      width={700}
      destroyOnClose
    >
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={tabItems}
      />
    </Modal>
  );
};

export default MappingModal;
