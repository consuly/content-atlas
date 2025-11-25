import React, { useState } from 'react';
import { useNavigate } from 'react-router';
import { App as AntdApp, Modal, Tabs, Button, Space, Alert, Spin, Typography, Input, Select, Checkbox } from 'antd';
import { ThunderboltOutlined, MessageOutlined, CheckCircleOutlined } from '@ant-design/icons';
import axios, { AxiosError } from 'axios';
import { ErrorLogViewer } from '../error-log-viewer';
import { formatUserFacingError } from '../../utils/errorMessages';
import { API_URL } from '../../config';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

interface MappingModalProps {
  visible: boolean;
  fileId: string;
  fileName: string;
  onClose: () => void;
  onSuccess: () => void;
}

interface LlmInstructionProfile {
  id: string;
  title: string;
  content: string;
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
  const { message: messageApi } = AntdApp.useApp();
  const friendlyError = error ? formatUserFacingError(error) : null;
  const [llmInstruction, setLlmInstruction] = useState('');
  const [saveInstruction, setSaveInstruction] = useState(false);
  const [instructionTitle, setInstructionTitle] = useState('');
  const [instructionOptions, setInstructionOptions] = useState<LlmInstructionProfile[]>([]);
  const [selectedInstructionId, setSelectedInstructionId] = useState<string | null>(null);
  const [loadingInstructions, setLoadingInstructions] = useState(false);
  const [instructionActionLoading, setInstructionActionLoading] = useState(false);
  
  // Interactive mode state
  const [threadId, setThreadId] = useState<string | null>(null);
  const [conversation, setConversation] = useState<Array<{ role: 'user' | 'assistant'; content: string }>>([]);
  const [userInput, setUserInput] = useState('');
  const [canExecute, setCanExecute] = useState(false);
  const [needsUserInput, setNeedsUserInput] = useState(true);
  const quickActions = [
    { label: 'Approve Plan', prompt: 'CONFIRM IMPORT' },
    {
      label: 'Request New Table',
      prompt: 'Could we import this file into a brand new table instead? Outline the new schema you recommend.',
    },
    {
      label: 'Adjust Column Mapping',
      prompt: 'Let us revisit the column mapping. Suggest column renames and ask me to confirm.',
    },
    {
      label: 'Review Duplicates',
      prompt: 'Explain the duplicate detection strategy and offer alternatives if they seem safer.',
    },
  ];

  const fetchInstructions = async () => {
    setLoadingInstructions(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.get<{ success: boolean; instructions: LlmInstructionProfile[] }>(
        `${API_URL}/llm-instructions`,
        {
          headers: {
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );
      if (response.data?.success && Array.isArray(response.data.instructions)) {
        setInstructionOptions(response.data.instructions);
      }
    } catch (err) {
      console.error('Failed to load instructions', err);
    } finally {
      setLoadingInstructions(false);
    }
  };

  React.useEffect(() => {
    if (visible) {
      void fetchInstructions();
    }
  }, [visible]);

  const instructionField = (
    <div style={{ width: '100%' }}>
      <Text strong>LLM instruction (optional)</Text>
      <Paragraph type="secondary" style={{ marginBottom: 8 }}>
        This note is sent to the AI for every file in this import so it can honor your special rules.
      </Paragraph>
      <Space direction="vertical" size="small" style={{ width: '100%' }}>
        <Select
          allowClear
          showSearch
          placeholder="Select a saved instruction"
          value={selectedInstructionId || undefined}
          onChange={(value) => {
            setSelectedInstructionId(value || null);
            const selected = instructionOptions.find((option) => option.id === value);
            if (selected) {
              setLlmInstruction(selected.content);
              setInstructionTitle(selected.title);
            }
          }}
          options={instructionOptions.map((option) => ({
            value: option.id,
            label: option.title,
          }))}
          loading={loadingInstructions}
          style={{ width: '100%' }}
        />
        <TextArea
          value={llmInstruction}
          onChange={(e) => {
            setLlmInstruction(e.target.value);
            if (selectedInstructionId) {
              setSelectedInstructionId(null);
            }
          }}
          placeholder="Example: Always treat phone numbers as text and never drop rows with empty addresses."
          autoSize={{ minRows: 2, maxRows: 4 }}
        />
        <Space align="start">
          <Checkbox
            checked={saveInstruction}
            onChange={(e) => setSaveInstruction(e.target.checked)}
          >
            Save this instruction for future imports
          </Checkbox>
          {saveInstruction && (
            <Input
              value={instructionTitle}
              onChange={(e) => setInstructionTitle(e.target.value)}
              placeholder="Instruction name (e.g., Marketing Cleanup Rules)"
              style={{ minWidth: 240 }}
            />
          )}
        </Space>
        {selectedInstructionId && (
          <Space>
            <Button
              size="small"
              onClick={async () => {
                if (!selectedInstructionId) return;
                const title = instructionTitle.trim() || 'Saved import instruction';
                setInstructionActionLoading(true);
                try {
                  const token = localStorage.getItem('refine-auth');
                  await axios.patch(
                    `${API_URL}/llm-instructions/${selectedInstructionId}`,
                    { title, content: llmInstruction },
                    {
                      headers: {
                        'Content-Type': 'application/json',
                        ...(token && { Authorization: `Bearer ${token}` }),
                      },
                    }
                  );
                  messageApi.success('Instruction updated');
                  await fetchInstructions();
                } catch (err) {
                  messageApi.error('Unable to update instruction');
                } finally {
                  setInstructionActionLoading(false);
                }
              }}
              loading={instructionActionLoading}
            >
              Update selected
            </Button>
            <Button
              size="small"
              danger
              onClick={async () => {
                if (!selectedInstructionId) return;
                setInstructionActionLoading(true);
                try {
                  const token = localStorage.getItem('refine-auth');
                  await axios.delete(`${API_URL}/llm-instructions/${selectedInstructionId}`, {
                    headers: {
                      ...(token && { Authorization: `Bearer ${token}` }),
                    },
                  });
                  messageApi.success('Instruction deleted');
                  setSelectedInstructionId(null);
                  setLlmInstruction('');
                  setInstructionTitle('');
                  await fetchInstructions();
                } catch (err) {
                  messageApi.error('Unable to delete instruction');
                } finally {
                  setInstructionActionLoading(false);
                }
              }}
              loading={instructionActionLoading}
            >
              Delete selected
            </Button>
          </Space>
        )}
      </Space>
    </div>
  );

  const handleAutoProcess = async () => {
    setLoading(true);
    setError(null);
    setErrorDetails(null);
    const instruction = llmInstruction.trim();
    const instructionName = instructionTitle.trim();

    try {
      const token = localStorage.getItem('refine-auth');
      const formData = new FormData();
      formData.append('file_id', fileId);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');
      if (instruction) {
        formData.append('llm_instruction', instruction);
      }
      if (selectedInstructionId) {
        formData.append('llm_instruction_id', selectedInstructionId);
      }
      if (saveInstruction && instruction) {
        formData.append('save_llm_instruction', 'true');
        if (instructionName) {
          formData.append('llm_instruction_title', instructionName);
        }
      }

      const response = await axios.post(`${API_URL}/analyze-file`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        messageApi.success('File mapped successfully!');
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
      const parsedError = formatUserFacingError(errorMsg);
      messageApi.error(parsedError.summary);
    } finally {
      setLoading(false);
    }
  };

  const handleInteractiveStart = async () => {
    setLoading(true);
    setError(null);
    setConversation([]);
    setNeedsUserInput(true);
    const instruction = llmInstruction.trim();
    const instructionName = instructionTitle.trim();

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: fileId,
          max_iterations: 5,
          ...(instruction ? { llm_instruction: instruction } : {}),
          ...(selectedInstructionId ? { llm_instruction_id: selectedInstructionId } : {}),
          ...(saveInstruction && instruction ? { save_llm_instruction: true } : {}),
          ...(saveInstruction && instructionName ? { llm_instruction_title: instructionName } : {}),
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
        setNeedsUserInput(response.data.needs_user_input ?? true);
      } else {
        setError(response.data.error || 'Analysis failed');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Analysis failed';
      setError(errorMsg);
      messageApi.error(formatUserFacingError(errorMsg).summary);
    } finally {
      setLoading(false);
    }
  };

  const sendInteractiveMessage = async (messageToSend: string) => {
    if (!threadId) return;
    const trimmed = messageToSend.trim();
    if (!trimmed) return;
    const instruction = llmInstruction.trim();
    const instructionName = instructionTitle.trim();

    setLoading(true);
    setError(null);

    setConversation((prev) => [...prev, { role: 'user', content: trimmed }]);
    setUserInput('');

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: fileId,
          user_message: trimmed,
          thread_id: threadId,
          max_iterations: 5,
          ...(instruction ? { llm_instruction: instruction } : {}),
          ...(selectedInstructionId ? { llm_instruction_id: selectedInstructionId } : {}),
          ...(saveInstruction && instruction ? { save_llm_instruction: true } : {}),
          ...(saveInstruction && instructionName ? { llm_instruction_title: instructionName } : {}),
        },
        {
          headers: {
            'Content-Type': 'application/json',
            ...(token && { Authorization: `Bearer ${token}` }),
          },
        }
      );

      if (response.data.success) {
        setConversation((prev) => [
          ...prev,
          { role: 'assistant', content: response.data.llm_message },
        ]);
        setCanExecute(response.data.can_execute);
        setNeedsUserInput(response.data.needs_user_input ?? true);
      } else {
        const fallback = response.data.error || 'Analysis failed';
        setError(fallback);
        messageApi.error(formatUserFacingError(fallback).summary);
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Analysis failed';
      setError(errorMsg);
      messageApi.error(formatUserFacingError(errorMsg).summary);
    } finally {
      setLoading(false);
    }
  };

  const handleInteractiveSend = async () => {
    if (!userInput.trim()) return;
    await sendInteractiveMessage(userInput);
  };

  const handleQuickAction = async (prompt: string) => {
    if (loading) return;
    await sendInteractiveMessage(prompt);
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
        messageApi.success('Import executed successfully!');
        onSuccess();
        setThreadId(null);
        setCanExecute(false);
        setNeedsUserInput(false);
        onClose();
        // Small delay to ensure backend has updated file status
        setTimeout(() => {
          navigate(`/import/${fileId}`, { replace: true });
        }, 500);
      } else {
        const failureMessage = response.data.message || 'Import execution failed';
        setConversation((prev) => {
          const next: Array<{ role: 'user' | 'assistant'; content: string }> = [
            ...prev,
            { role: 'assistant', content: `⚠️ ${failureMessage}` },
          ];
          if (response.data.llm_followup) {
            next.push({ role: 'assistant', content: response.data.llm_followup });
          }
          return next;
        });
        setCanExecute(response.data.can_execute ?? false);
        setNeedsUserInput(response.data.needs_user_input ?? true);
        if (response.data.thread_id) {
          setThreadId(response.data.thread_id);
        }
        setError(failureMessage);
        messageApi.error(formatUserFacingError(failureMessage).summary);
    }
  } catch (err) {
    const error = err as AxiosError<{ detail?: string }>;
    const errorMsg = error.response?.data?.detail || error.message || 'Import execution failed';
    setError(errorMsg);
    messageApi.error(formatUserFacingError(errorMsg).summary);
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
    setNeedsUserInput(true);
    setLlmInstruction('');
    setInstructionTitle('');
    setSaveInstruction(false);
    setSelectedInstructionId(null);
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
        {instructionField}

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

      {friendlyError && (
        <Alert
          message="Error"
          description={
            <Space direction="vertical" size={4} style={{ width: '100%' }}>
              <Text>{friendlyError.summary}</Text>
              {friendlyError.note && (
                <Text type="secondary">{friendlyError.note}</Text>
              )}
              <Text strong>{friendlyError.action}</Text>
            </Space>
          }
          type="error"
          showIcon
          closable
          onClose={() => setError(null)}
          style={{ marginBottom: 24 }}
        />
      )}

      <div style={{ marginBottom: 16 }}>{instructionField}</div>

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

          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Alert
              type={canExecute ? 'success' : 'info'}
              message={
                canExecute
                  ? 'Mapping confirmed. Execute the import or ask for more refinements below.'
                  : needsUserInput
                    ? 'The assistant is waiting for your guidance. Ask for adjustments or confirm when ready.'
                    : 'Processing your last request...'
              }
              showIcon
            />

            {canExecute && (
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
            )}

            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
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
                  placeholder="Ask for changes, confirmations, or new options..."
                  disabled={loading || !threadId}
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
                  disabled={!userInput.trim() || loading || !threadId}
                  style={{ borderRadius: '0 4px 4px 0' }}
                >
                  Send
                </Button>
              </Space.Compact>

              <Space wrap>
                {quickActions.map(({ label, prompt }) => (
                  <Button
                    key={label}
                    size="small"
                    type={label === 'Approve Plan' ? 'primary' : 'default'}
                    disabled={loading || !threadId}
                    onClick={() => handleQuickAction(prompt)}
                  >
                    {label}
                  </Button>
                ))}
              </Space>
            </Space>
          </Space>
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
