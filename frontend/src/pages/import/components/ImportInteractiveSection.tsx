import React, { useContext } from 'react';
import {
  Alert,
  Space,
  Typography,
  Select,
  Switch,
  Input,
  Button,
  Spin,
  Row,
  Col,
} from 'antd';
import {
  MessageOutlined,
  CheckCircleOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { ColorModeContext } from '../../../contexts/color-mode/context';
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
  instructionField: React.ReactNode;
  disableMappingActions: boolean;

  // New props for configuration
  skipFileDuplicateCheck: boolean;
  setSkipFileDuplicateCheck: (value: boolean) => void;
  updateOnDuplicate: boolean;
  setUpdateOnDuplicate: (value: boolean) => void;
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
  instructionField,
  disableMappingActions,
  skipFileDuplicateCheck,
  setSkipFileDuplicateCheck,
  updateOnDuplicate,
  setUpdateOnDuplicate,
  useSharedTable,
  setUseSharedTable,
  sharedTableName,
  setSharedTableName,
  sharedTableMode,
  setSharedTableMode,
  existingTables,
  loadingTables,
}) => {
  const { mode } = useContext(ColorModeContext);
  
  return (
    <div style={{ padding: '24px 0' }}>
      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

      {!result && conversation.length === 0 && (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <Space>
            <SettingOutlined />
            <Text strong>Advanced Configuration</Text>
            <Text type="secondary" style={{ fontSize: '12px' }}>(Schema, Duplicates, Instructions)</Text>
          </Space>

          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            {isExcelFile && sheetNames.length > 0 && (
              <div>
                <Text strong>Choose a tab to review</Text>
                <Paragraph type="secondary" style={{ marginBottom: 8, fontSize: '12px' }}>
                  Interactive mode works one sheet at a time.
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

            <Row gutter={[24, 24]}>
              <Col span={12}>
                <Space align="start">
                  <Switch checked={skipFileDuplicateCheck} onChange={(checked) => setSkipFileDuplicateCheck(checked)} />
                  <div>
                    <Text strong>Skip duplicate check</Text>
                    <div style={{ fontSize: '12px', color: '#8c8c8c' }}>
                      Import all rows without checking for duplicates.
                    </div>
                  </div>
                </Space>
              </Col>
              <Col span={12}>
                <Space align="start">
                  <Switch checked={useSharedTable} onChange={(checked) => setUseSharedTable(checked)} />
                  <div>
                    <Text strong>Use single table</Text>
                    <div style={{ fontSize: '12px', color: '#8c8c8c' }}>
                      Map this file into a specific table.
                    </div>
                  </div>
                </Space>
              </Col>
              <Col span={12}>
                <Space align="start">
                  <Switch 
                    checked={updateOnDuplicate} 
                    onChange={(checked) => setUpdateOnDuplicate(checked)}
                    disabled={skipFileDuplicateCheck}
                  />
                  <div>
                    <Text strong>Update duplicates</Text>
                    <div style={{ fontSize: '12px', color: '#8c8c8c' }}>
                      Update existing rows when duplicates are found.
                    </div>
                  </div>
                </Space>
              </Col>
            </Row>
            
            {useSharedTable && (
              <div style={{ background: mode === 'dark' ? '#1a2332' : '#fafafa', padding: 16, borderRadius: 8 }}>
                <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                  <Text strong>Target Table Settings</Text>
                  <Space>
                    <Select
                      value={sharedTableMode}
                      style={{ width: 180 }}
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
                        style={{ width: 300 }}
                      />
                    ) : (
                      <Select
                        showSearch
                        value={sharedTableName || undefined}
                        placeholder="Select an existing table"
                        onChange={(value) => setSharedTableName(value)}
                        loading={loadingTables}
                        style={{ width: 300 }}
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
                </Space>
              </div>
            )}

            {instructionField}
          </Space>
          
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
              style={{ height: '50px', fontSize: '18px' }}
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
              height: '600px',
              overflowY: 'auto',
              marginBottom: 16,
              padding: 16,
              border: mode === 'dark' ? '1px solid #263040' : '1px solid #d9d9d9',
              borderRadius: 4,
              backgroundColor: mode === 'dark' ? '#0b1220' : '#fafafa',
            }}
          >
            {conversation.map((msg, idx) => (
              <div
                key={idx}
                style={{
                  marginBottom: 16,
                  padding: 12,
                  backgroundColor:
                    msg.role === 'user'
                      ? mode === 'dark'
                        ? '#1e3a5f'
                        : '#e6f7ff'
                      : mode === 'dark'
                        ? '#1a2332'
                        : '#fff',
                  borderRadius: 4,
                  border: '1px solid',
                  borderColor:
                    msg.role === 'user'
                      ? mode === 'dark'
                        ? '#2563eb'
                        : '#91d5ff'
                      : mode === 'dark'
                        ? '#263040'
                        : '#d9d9d9',
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

            <div style={{ display: 'flex', gap: '10px', alignItems: 'flex-start' }}>
              <Input.TextArea
                value={userInput}
                onChange={(e) => setUserInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey && !processing) {
                    e.preventDefault();
                    onInteractiveSend();
                  }
                }}
                placeholder="Ask for changes, confirmations, or next steps..."
                disabled={processing || !threadId}
                autoSize={{ minRows: 4, maxRows: 8 }}
                style={{ flex: 1, fontSize: '16px' }}
              />
              <Button
                type="primary"
                onClick={onInteractiveSend}
                loading={processing}
                disabled={!userInput.trim() || processing || !threadId}
                style={{ height: '40px', padding: '0 24px' }}
              >
                Send
              </Button>
            </div>
          </Space>
        </div>
      )}
    </div>
  );
};
