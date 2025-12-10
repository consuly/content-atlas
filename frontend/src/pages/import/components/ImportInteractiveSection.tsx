import React from 'react';
import {
  Alert,
  Space,
  Typography,
  Select,
  Switch,
  Input,
  Button,
  Spin,
} from 'antd';
import {
  MessageOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import { ErrorLogViewer } from '../../../components/error-log-viewer';
import { UploadedFile, ProcessingResult } from './types';

const { Text, Paragraph } = Typography;

interface ImportInteractiveSectionProps {
  file: UploadedFile;
  isExcelFile: boolean;
  sheetNames: string[];
  interactiveSheet: string | undefined;
  setInteractiveSheet: (value: string | undefined) => void;
  processing: boolean;
  error: string | null;
  result: ProcessingResult | null;
  conversation: Array<{ role: 'user' | 'assistant'; content: string }>;
  userInput: string;
  setUserInput: (val: string) => void;
  canExecute: boolean;
  needsUserInput: boolean;
  threadId: string | null;
  showInteractiveRetry: boolean;
  onInteractiveStart: (options?: { previousError?: string }) => void;
  onInteractiveSend: () => void;
  onInteractiveExecute: () => void;
  onQuickAction: (prompt: string) => void;
  instructionField: React.ReactNode;
  formatBytes: (bytes: number) => string;
  quickActions: Array<{ label: string; prompt: string }>;
  disableMappingActions: boolean;

  // New props for configuration
  skipFileDuplicateCheck: boolean;
  setSkipFileDuplicateCheck: (value: boolean) => void;
  useSharedTable: boolean;
  setUseSharedTable: (value: boolean) => void;
  sharedTableName: string;
  setSharedTableName: (value: string) => void;
  sharedTableMode: 'existing' | 'new';
  setSharedTableMode: (value: 'existing' | 'new') => void;
  existingTables: Array<{ table_name: string; row_count: number }>;
  loadingTables: boolean;
}

export const ImportInteractiveSection: React.FC<ImportInteractiveSectionProps> = ({
  file,
  isExcelFile,
  sheetNames,
  interactiveSheet,
  setInteractiveSheet,
  processing,
  error,
  result,
  conversation,
  userInput,
  setUserInput,
  canExecute,
  needsUserInput,
  threadId,
  showInteractiveRetry,
  onInteractiveStart,
  onInteractiveSend,
  onInteractiveExecute,
  onQuickAction,
  instructionField,
  formatBytes,
  quickActions,
  disableMappingActions,
  skipFileDuplicateCheck,
  setSkipFileDuplicateCheck,
  useSharedTable,
  setUseSharedTable,
  sharedTableName,
  setSharedTableName,
  sharedTableMode,
  setSharedTableMode,
  existingTables,
  loadingTables,
}) => {
  return (
    <div style={{ padding: '24px 0' }}>
      <Alert
        message="Interactive Processing"
        description="The AI will ask you questions to better understand how to import your data. This gives you more control over the process."
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

      <div style={{ marginBottom: 16 }}>{instructionField}</div>

      {!result && conversation.length === 0 && (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div>
            <Text strong>File: </Text>
            <Text>{file.file_name}</Text>
          </div>
          <div>
            <Text strong>Size: </Text>
            <Text>{formatBytes(file.file_size)}</Text>
          </div>
          {isExcelFile && sheetNames.length > 0 && (
            <div style={{ maxWidth: 360 }}>
              <Text strong>Choose a tab to review</Text>
              <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                Interactive mode works one sheet at a time. Pick a tab or leave the default to start with the first sheet.
              </Paragraph>
              <Select
                style={{ width: '100%' }}
                placeholder="Select a sheet"
                value={interactiveSheet}
                onChange={(value) => setInteractiveSheet(value)}
                options={sheetNames.map((sheet) => ({ label: sheet, value: sheet }))}
              />
            </div>
          )}

          <div>
            <Space align="start">
              <Switch checked={skipFileDuplicateCheck} onChange={(checked) => setSkipFileDuplicateCheck(checked)} />
              <div>
                <Text strong>Skip duplicate row detection</Text>
                <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                  By default, duplicate rows are detected and skipped based on unique columns. Enable this to import all rows without checking for duplicates.
                </Paragraph>
              </div>
            </Space>
          </div>
          
          <div>
            <Space align="start">
              <Switch checked={useSharedTable} onChange={(checked) => setUseSharedTable(checked)} />
              <div>
                <Text strong>Use a single table for this import</Text>
                <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                  Map this file into a specific table.
                </Paragraph>
                {useSharedTable && (
                  <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                    <Select
                      value={sharedTableMode}
                      style={{ width: 240 }}
                      onChange={(value) => {
                        setSharedTableMode(value as 'existing' | 'new');
                        setSharedTableName('');
                      }}
                      options={[
                        { value: 'new', label: 'Create new table' },
                        { value: 'existing', label: 'Use existing table' },
                      ]}
                    />
                    {sharedTableMode === 'new' ? (
                      <Input
                        value={sharedTableName}
                        placeholder="Enter new table name"
                        onChange={(e) => setSharedTableName(e.target.value)}
                        style={{ width: 360 }}
                      />
                    ) : (
                      <Select
                        showSearch
                        value={sharedTableName || undefined}
                        placeholder="Select an existing table"
                        onChange={(value) => setSharedTableName(value)}
                        loading={loadingTables}
                        style={{ width: 360 }}
                        options={existingTables.map((table) => ({
                          value: table.table_name,
                          label: `${table.table_name} (${table.row_count.toLocaleString()} rows)`,
                        }))}
                        filterOption={(input, option) =>
                          (option?.label?.toString() ?? '').toLowerCase().includes(input.toLowerCase())
                        }
                      />
                    )}
                  </Space>
                )}
              </div>
            </Space>
          </div>

          {processing ? (
            <div style={{ textAlign: 'center', padding: '24px 0' }}>
              <Spin size="large" />
            </div>
          ) : (
            <Button
              type="primary"
              size="large"
              icon={<MessageOutlined />}
              onClick={() => {
                const previousError =
                  file.status === 'failed' && showInteractiveRetry
                    ? (file.error_message || '').trim()
                    : '';
                onInteractiveStart(previousError ? { previousError } : undefined);
              }}
              disabled={disableMappingActions}
              block
            >
              Start Interactive Analysis
            </Button>
          )}
        </Space>
      )}

      {!result && conversation.length > 0 && (
        <div>
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
            {processing && (
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
                  ? 'Mapping confirmed. Execute when ready or ask for additional adjustments below.'
                  : needsUserInput
                    ? 'The assistant is waiting for your direction. Ask for changes or confirm when the plan looks right.'
                    : 'Processing... the assistant will respond shortly.'
              }
              showIcon
            />

            <Button
              type="primary"
              size="large"
              icon={<CheckCircleOutlined />}
              onClick={onInteractiveExecute}
              loading={processing}
              disabled={!canExecute || processing}
              block
            >
              {processing ? 'Executing...' : 'Execute Import'}
            </Button>

            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <Space.Compact style={{ width: '100%' }}>
                <input
                  type="text"
                  value={userInput}
                  onChange={(e) => setUserInput(e.target.value)}
                  onKeyPress={(e) => {
                    if (e.key === 'Enter' && !processing) {
                      onInteractiveSend();
                    }
                  }}
                  placeholder="Ask for changes, confirmations, or next steps..."
                  disabled={processing || !threadId}
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
                  onClick={onInteractiveSend}
                  loading={processing}
                  disabled={
                    !userInput.trim() || processing || !threadId
                  }
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
                    disabled={!threadId || processing}
                    onClick={() => onQuickAction(prompt)}
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
};
