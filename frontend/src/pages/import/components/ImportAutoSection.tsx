import React from 'react';
import {
  Alert,
  Space,
  Typography,
  Select,
  Switch,
  Input,
  Button,
} from 'antd';
import {
  ThunderboltOutlined,
} from '@ant-design/icons';
import { ErrorLogViewer } from '../../../components/error-log-viewer';
import { UploadedFile, ProcessingResult } from './types';

const { Text, Paragraph } = Typography;

interface ImportAutoSectionProps {
  file: UploadedFile;
  isArchive: boolean;
  isExcelFile: boolean;
  sheetNames: string[];
  selectedSheets: string[];
  setSelectedSheets: (value: string[]) => void;
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
  processing: boolean;
  archiveProcessing: boolean;
  disableMappingActions: boolean;
  error: string | null;
  result: ProcessingResult | null;
  onAutoProcess: () => void;
  onArchiveAutoProcess: () => void;
  instructionField: React.ReactNode;
  archiveResultsPanel: React.ReactNode;
  formatBytes: (bytes: number) => string;
}

export const ImportAutoSection: React.FC<ImportAutoSectionProps> = ({
  file,
  isArchive,
  isExcelFile,
  sheetNames,
  selectedSheets,
  setSelectedSheets,
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
  processing,
  archiveProcessing,
  disableMappingActions,
  error,
  result,
  onAutoProcess,
  onArchiveAutoProcess,
  instructionField,
  archiveResultsPanel,
  formatBytes,
}) => {
  return (
    <div style={{ padding: '24px 0' }}>
      <Alert
        message="Automatic Processing"
        description="The AI will analyze your file, compare it with existing tables, and automatically import the data without asking questions. This is the fastest option."
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />
      {isArchive && (
        <Alert
          message="Archive detected"
          description="Auto Process Archive will unpack every CSV/XLSX in this ZIP file and run the auto mapper on each one sequentially."
          type="warning"
          showIcon
          style={{ marginBottom: 24 }}
        />
      )}

      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

      {!result && (
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div>
            <Text strong>File: </Text>
            <Text>{file.file_name}</Text>
          </div>
          <div>
            <Text strong>Size: </Text>
            <Text>{formatBytes(file.file_size)}</Text>
          </div>
          
          {instructionField}

          {isExcelFile && (
            <div style={{ maxWidth: 480 }}>
              <Text strong>Workbook tabs</Text>
              <Paragraph type="secondary" style={{ marginBottom: 8 }}>
                Auto processing will create one import per selected tab using the workbook name plus the sheet name.
              </Paragraph>
              <Select
                mode="multiple"
                style={{ width: '100%' }}
                placeholder={sheetNames.length ? 'Select sheets to process' : 'No sheets found'}
                value={selectedSheets}
                onChange={(values) => setSelectedSheets(values)}
                options={sheetNames.map((sheet) => ({ label: sheet, value: sheet }))}
                disabled={!sheetNames.length}
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
                  Map {isArchive ? 'every file in this archive' : 'this file'} into one table.
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

          {!isArchive && (
            <Button
              type="primary"
              size="large"
              icon={<ThunderboltOutlined />}
              onClick={onAutoProcess}
              loading={processing}
              disabled={disableMappingActions}
              block
            >
              {processing ? 'Processing...' : 'Process Now'}
            </Button>
          )}

          {isArchive && (
            <Button
              type="primary"
              size="large"
              icon={<ThunderboltOutlined />}
              onClick={onArchiveAutoProcess}
              loading={archiveProcessing}
              disabled={disableMappingActions}
              block
            >
              {archiveProcessing ? 'Processing Archive...' : 'Auto Process Archive'}
            </Button>
          )}
        </Space>
      )}

      {archiveResultsPanel}
    </div>
  );
};
