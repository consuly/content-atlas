import React from 'react';
import {
  Alert,
  Space,
  Typography,
  Select,
  Switch,
  Input,
  Button,
  Row,
  Col,
} from 'antd';
import {
  ThunderboltOutlined,
  SettingOutlined,
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
}

export const ImportAutoSection: React.FC<ImportAutoSectionProps> = ({
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
}) => {
  return (
    <div style={{ padding: '24px 0' }}>
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
          <Space>
            <SettingOutlined />
            <Text strong>Advanced Configuration</Text>
            <Text type="secondary" style={{ fontSize: '12px' }}>(Schema, Duplicates, Instructions)</Text>
          </Space>

          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            {isExcelFile && (
              <div>
                <Text strong>Workbook tabs</Text>
                <Paragraph type="secondary" style={{ marginBottom: 8, fontSize: '12px' }}>
                  Creates one import per selected tab.
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
                      Map {isArchive ? 'all files' : 'this file'} into one specific table.
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

          {!isArchive && (
            <Button
              type="primary"
              size="large"
              icon={<ThunderboltOutlined />}
              onClick={onAutoProcess}
              loading={processing}
              disabled={disableMappingActions}
              block
              style={{ height: '50px', fontSize: '18px' }}
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
              style={{ height: '50px', fontSize: '18px' }}
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
