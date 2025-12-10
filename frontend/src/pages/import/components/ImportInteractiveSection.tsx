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
  Collapse,
  Row,
  Col,
} from 'antd';
import {
  MessageOutlined,
  CheckCircleOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { ErrorLogViewer } from '../../../components/error-log-viewer';
import { UploadedFile, ProcessingResult } from './types';

const { Text, Paragraph } = Typography;
const { Panel } = Collapse;

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
      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

      {!result && conversation.length === 0 && (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          
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

          <Collapse ghost expandIconPosition="end">
            <Panel 
              header={
                <Space>
                  <SettingOutlined />
                  <Text strong>Advanced Configuration</Text>
                  <Text type="secondary" style={{ fontSize: '12px' }}>(Schema, Duplicates, Instructions)</Text>
                </Space>
              } 
              key="1"
            >
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
                </Row>
                
                {useSharedTable && (
                  <div style={{ background: '#fafafa', padding: 16, borderRadius: 8 }}>
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
            </Panel>
          </Collapse>
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
