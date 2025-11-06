import React, { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router';
import { App as AntdApp, Card, Tabs, Button, Space, Alert, Spin, Typography, Result, Statistic, Row, Col, Breadcrumb, Descriptions, Table, Tag, Divider } from 'antd';
import type { BreadcrumbProps, DescriptionsProps } from 'antd';
import { ThunderboltOutlined, MessageOutlined, CheckCircleOutlined, ArrowLeftOutlined, HomeOutlined, FileOutlined, DatabaseOutlined, InfoCircleOutlined, EyeOutlined } from '@ant-design/icons';
import axios, { AxiosError } from 'axios';
import { ErrorLogViewer } from '../../components/error-log-viewer';
import { formatUserFacingError } from '../../utils/errorMessages';

const { Text, Paragraph } = Typography;
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface UploadedFile {
  id: string;
  file_name: string;
  b2_file_id: string;
  b2_file_path: string;
  file_size: number;
  content_type?: string;
  upload_date?: string;
  status: string;
  mapped_table_name?: string;
  mapped_date?: string;
  mapped_rows?: number;
  error_message?: string;
}

interface ProcessingResult {
  success: boolean;
  table_name?: string;
  rows_imported?: number;
  execution_time?: number;
  error?: string;
}

interface TableData {
  data: Array<Record<string, unknown>>;
  total_rows: number;
}

interface ImportHistory {
  import_id: string;
  import_timestamp: string;
  table_name: string;
  import_strategy?: string;
  status: string;
  total_rows_in_file?: number;
  rows_inserted?: number;
  duplicates_found?: number;
  data_validation_errors?: number;
  duration_seconds?: number;
  mapping_config?: Record<string, unknown>;
}

export const ImportMappingPage: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { message: messageApi } = AntdApp.useApp();
  
  const [file, setFile] = useState<UploadedFile | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<string>('auto');
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ProcessingResult | null>(null);
  
  // Interactive mode state
  const [threadId, setThreadId] = useState<string | null>(null);
  const [conversation, setConversation] = useState<Array<{ role: 'user' | 'assistant'; content: string }>>([]);
  const [userInput, setUserInput] = useState('');
  const [canExecute, setCanExecute] = useState(false);
  const [needsUserInput, setNeedsUserInput] = useState(true);
  const [showInteractiveRetry, setShowInteractiveRetry] = useState(false);
  const quickActions = [
    { label: 'Approve Plan', prompt: 'CONFIRM IMPORT' },
    {
      label: 'Request New Table',
      prompt: 'Could we create a brand new table for this import instead? Outline the recommended schema.',
    },
    {
      label: 'Adjust Column Mapping',
      prompt: 'Please walk me through adjusting the column mapping. Suggest columns that should be renamed or remapped.',
    },
    {
      label: 'Review Duplicates',
      prompt: 'Explain how duplicate detection is configured. Are there better uniqueness rules we should consider?',
    },
  ];

  // Mapped file details state
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [importHistory, setImportHistory] = useState<ImportHistory | null>(null);
  const [loadingDetails, setLoadingDetails] = useState(false);

  const fetchFileDetails = useCallback(async () => {
    if (!id) return;
    
    setLoading(true);
    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.get(`${API_URL}/uploaded-files/${id}`, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        setFile(response.data.file);
      } else {
        setError('Failed to load file details');
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Failed to load file';
      setError(errorMsg);
    } finally {
      setLoading(false);
    }
  }, [id]);

  const fetchMappedFileDetails = useCallback(async (tableName: string) => {
    setLoadingDetails(true);
    try {
      const token = localStorage.getItem('refine-auth');
      
      // Fetch table data preview
      const tableResponse = await axios.get(`${API_URL}/tables/${tableName}`, {
        params: { limit: 10, offset: 0 },
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (tableResponse.data.success) {
        const rawData = tableResponse.data.data as Record<string, unknown>[];
        const dataWithKeys = rawData.map((row, index) => {
          const existingKey =
            (row.id ?? row.ID ?? row.Id ?? row.uuid ?? row.UUID) as
              | string
              | number
              | undefined;
          const key =
            existingKey !== undefined
              ? String(existingKey)
              : `${tableName}-${index}`;

          return {
            __rowKey: key,
            ...row,
          };
        });
        setTableData({
          data: dataWithKeys,
          total_rows: tableResponse.data.total_rows,
        });
      }

      // Fetch import history
      const historyResponse = await axios.get(`${API_URL}/import-history`, {
        params: { table_name: tableName, limit: 1 },
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (historyResponse.data.success && historyResponse.data.imports.length > 0) {
        setImportHistory(historyResponse.data.imports[0]);
      }
    } catch (err) {
      console.error('Error fetching mapped file details:', err);
    } finally {
      setLoadingDetails(false);
    }
  }, []);

  useEffect(() => {
    fetchFileDetails();
  }, [fetchFileDetails]);

  // Reset result state when file details are fetched and file is mapped
  useEffect(() => {
    if (file?.status === 'mapped' && result) {
      setResult(null);
    }
  }, [file, result]);

  useEffect(() => {
    if (file?.status === 'mapped' && file.mapped_table_name) {
      fetchMappedFileDetails(file.mapped_table_name);
    }
  }, [file, fetchMappedFileDetails]);

  useEffect(() => {
    if (file && file.status !== 'failed' && showInteractiveRetry) {
      setShowInteractiveRetry(false);
    }
  }, [file, showInteractiveRetry]);

  const handleAutoProcess = async () => {
    if (!id) return;
    
    setProcessing(true);
    setError(null);
    setResult(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const formData = new FormData();
      formData.append('file_id', id);
      formData.append('analysis_mode', 'auto_always');
      formData.append('conflict_resolution', 'llm_decide');
      formData.append('max_iterations', '5');

      const response = await axios.post(`${API_URL}/analyze-file`, formData, {
        headers: {
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        setResult({
          success: true,
          table_name: response.data.table_name,
          rows_imported: response.data.rows_imported,
          execution_time: response.data.execution_time,
        });
        // Refetch file details to get updated status and trigger detailed view
        await fetchFileDetails();
      } else {
        setResult({
          success: false,
          error: response.data.error || 'Processing failed',
        });
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Processing failed';
      setResult({
        success: false,
        error: errorMsg,
      });
    } finally {
      setProcessing(false);
    }
  };

  const handleInteractiveStart = async (options?: { previousError?: string }) => {
    if (!id) return;
    
    setProcessing(true);
    setError(null);
    setConversation([]);
    setNeedsUserInput(true);
    setResult(null);
    setThreadId(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const payload: Record<string, unknown> = {
        file_id: id,
        max_iterations: 5,
      };

      if (options?.previousError) {
        payload.previous_error_message = options.previousError;
      }

      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        payload,
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
      setProcessing(false);
    }
  };

  const handleRetryInteractive = async () => {
    if (!id || processing) return;

    setShowInteractiveRetry(true);
    setActiveTab('interactive');

    const cleanedError = file?.error_message?.trim();
    await handleInteractiveStart({
      previousError: cleanedError && cleanedError.length > 0 ? cleanedError : undefined,
    });
  };

  const sendInteractiveMessage = async (messageToSend: string) => {
    if (!threadId || !id) return;
    const trimmed = messageToSend.trim();
    if (!trimmed) return;

    setProcessing(true);
    setError(null);

    setConversation((prev) => [...prev, { role: 'user', content: trimmed }]);
    setUserInput('');

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: id,
          user_message: trimmed,
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
      setProcessing(false);
    }
  };

  const handleInteractiveSend = async () => {
    if (!userInput.trim()) return;
    await sendInteractiveMessage(userInput);
  };

  const handleQuickAction = async (prompt: string) => {
    if (!prompt || processing) return;
    await sendInteractiveMessage(prompt);
  };

  const handleInteractiveExecute = async () => {
    if (!threadId || !id) return;

    setProcessing(true);
    setError(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/execute-interactive-import`,
        {
          file_id: id,
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
        setConversation((prev) => [
          ...prev,
          {
            role: 'assistant',
            content: `✅ Import executed successfully into ${response.data.table_name}.`,
          },
        ]);
        setResult({
          success: true,
          table_name: response.data.table_name,
          rows_imported: response.data.rows_imported,
          execution_time: response.data.execution_time,
        });
        // Refetch file details to get updated status and trigger detailed view
        await fetchFileDetails();
        setCanExecute(false);
        setNeedsUserInput(false);
        setThreadId(null);
      } else {
        const failureMessage = response.data.message || 'Import execution failed';
        setError(failureMessage);
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
        messageApi.error(formatUserFacingError(failureMessage).summary);
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Import execution failed';
      setError(errorMsg);
      messageApi.error(formatUserFacingError(errorMsg).summary);
    } finally {
      setProcessing(false);
    }
  };

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const formatDate = (dateString?: string): string => {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString();
  };

  const renderMappedFileView = () => {
    if (!file || file.status !== 'mapped') return null;

    const summaryItems: DescriptionsProps['items'] = [
      {
        key: 'table-name',
        label: 'Table Name',
        children: <Tag color="blue">{file.mapped_table_name}</Tag>,
      },
      {
        key: 'mapped-date',
        label: 'Mapped Date',
        children: formatDate(file.mapped_date),
      },
      {
        key: 'rows-imported',
        label: 'Rows Imported',
        children: <Text strong>{file.mapped_rows?.toLocaleString() || 0}</Text>,
      },
      {
        key: 'file-size',
        label: 'File Size',
        children: formatBytes(file.file_size),
      },
      {
        key: 'upload-date',
        label: 'Upload Date',
        children: formatDate(file.upload_date),
      },
      {
        key: 'status',
        label: 'Status',
        children: <Tag color="success">Mapped</Tag>,
      },
    ];

    const historyItems: DescriptionsProps['items'] = importHistory
      ? [
          ...(importHistory.import_strategy
            ? [
                {
                  key: 'import-strategy',
                  label: 'Import Strategy',
                  children: <Tag>{importHistory.import_strategy}</Tag>,
                  span: 2,
                } as const,
              ]
            : []),
          {
            key: 'total-rows',
            label: 'Total Rows in File',
            children:
              importHistory.total_rows_in_file?.toLocaleString() || '-',
          },
          {
            key: 'rows-inserted',
            label: 'Rows Inserted',
            children: importHistory.rows_inserted?.toLocaleString() || '-',
          },
          ...(importHistory.duplicates_found !== undefined &&
          importHistory.duplicates_found > 0
            ? [
                {
                  key: 'duplicates-found',
                  label: 'Duplicates Found',
                  children: (
                    <Text type="warning">
                      {importHistory.duplicates_found.toLocaleString()}
                    </Text>
                  ),
                  span: 2,
                } as const,
              ]
            : []),
          ...(importHistory.data_validation_errors !== undefined &&
          importHistory.data_validation_errors > 0
            ? [
                {
                  key: 'validation-errors',
                  label: 'Validation Errors',
                  children: (
                    <Text type="danger">
                      {importHistory.data_validation_errors.toLocaleString()}
                    </Text>
                  ),
                  span: 2,
                } as const,
              ]
            : []),
          ...(importHistory.duration_seconds
            ? [
                {
                  key: 'processing-time',
                  label: 'Processing Time',
                  children: `${importHistory.duration_seconds.toFixed(2)}s`,
                  span: 2,
                } as const,
              ]
            : []),
          {
            key: 'import-id',
            label: 'Import ID',
            children: (
              <Text code style={{ fontSize: '11px' }}>
                {importHistory.import_id}
              </Text>
            ),
            span: 2,
          },
        ]
      : [];

    return (
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Alert
          message="File Already Mapped"
          description="This file has been successfully imported into the database. View the details below."
          type="success"
          showIcon
          icon={<CheckCircleOutlined />}
        />

        {/* Import Summary */}
        <Card title={<><InfoCircleOutlined /> Import Summary</>} size="small">
          <Descriptions column={2} bordered size="small" items={summaryItems} />
        </Card>

        {/* Import Details */}
        {importHistory && (
          <Card title={<><DatabaseOutlined /> Import Details</>} size="small" loading={loadingDetails}>
            <Descriptions
              column={2}
              bordered
              size="small"
              items={historyItems}
            />
          </Card>
        )}

        {/* Data Preview */}
        {tableData && tableData.data.length > 0 && (
          <Card 
            title={<><EyeOutlined /> Data Preview (First 10 Rows)</>} 
            size="small"
            loading={loadingDetails}
            extra={
              <Button 
                type="link" 
                onClick={() => window.open(`/tables/${file.mapped_table_name}`, '_blank')}
              >
                View Full Table
              </Button>
            }
          >
            <Table
              dataSource={tableData.data}
              columns={Object.keys(tableData.data[0] || {})
                .filter((key) => key !== "__rowKey")
                .map((key) => ({
                  title: key,
                  dataIndex: key,
                  key,
                  ellipsis: true,
                  width: 150,
                }))}
              pagination={false}
              scroll={{ x: 'max-content' }}
              size="small"
              rowKey="__rowKey"
            />
            <Divider />
            <Text type="secondary">
              Showing 10 of {tableData.total_rows.toLocaleString()} total rows
            </Text>
          </Card>
        )}

        {/* Action Buttons */}
        <Space>
          <Button 
            type="primary" 
            icon={<DatabaseOutlined />}
            onClick={() => navigate(`/query`)}
          >
            Query This Data
          </Button>
          <Button 
            icon={<EyeOutlined />}
            onClick={() => window.open(`/tables/${file.mapped_table_name}`, '_blank')}
          >
            View Full Table
          </Button>
          <Button 
            icon={<ArrowLeftOutlined />}
            onClick={() => navigate('/import')}
          >
            Back to Import List
          </Button>
        </Space>
      </Space>
    );
  };

  if (loading) {
    return (
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16 }}>Loading file details...</div>
      </div>
    );
  }

  if (error && !file) {
    return (
      <div style={{ padding: '24px' }}>
        <Result
          status="error"
          title="Failed to Load File"
          subTitle={error}
          extra={
            <Button type="primary" onClick={() => navigate('/import')}>
              Back to Import List
            </Button>
          }
        />
      </div>
    );
  }

  if (!file) {
    return (
      <div style={{ padding: '24px' }}>
        <Result
          status="404"
          title="File Not Found"
          subTitle="The file you're looking for doesn't exist."
          extra={
            <Button type="primary" onClick={() => navigate('/import')}>
              Back to Import List
            </Button>
          }
        />
      </div>
    );
  }

  const breadcrumbItems: BreadcrumbProps['items'] = [
    {
      key: 'import',
      title: (
        <span
          style={{ cursor: 'pointer' }}
          onClick={() => navigate('/import')}
        >
          <HomeOutlined />
          <span style={{ marginLeft: 8 }}>Import</span>
        </span>
      ),
    },
    {
      key: 'file',
      title: (
        <span>
          <FileOutlined />
          <span style={{ marginLeft: 8 }}>{file.file_name}</span>
        </span>
      ),
    },
  ];

  const autoTabContent = (
    <div style={{ padding: '24px 0' }}>
      <Alert
        message="Automatic Processing"
        description="The AI will analyze your file, compare it with existing tables, and automatically import the data without asking questions. This is the fastest option."
        type="info"
        showIcon
        style={{ marginBottom: 24 }}
      />

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

          <Button
            type="primary"
            size="large"
            icon={<ThunderboltOutlined />}
            onClick={handleAutoProcess}
            loading={processing}
            block
          >
            {processing ? 'Processing...' : 'Process Now'}
          </Button>
        </Space>
      )}
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

      {error && !result && (
        <div style={{ marginBottom: 24 }}>
          <ErrorLogViewer error={error} showRetry={false} />
        </div>
      )}

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
                handleInteractiveStart(previousError ? { previousError } : undefined);
              }}
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

            {canExecute && (
              <Button
                type="primary"
                size="large"
                icon={<CheckCircleOutlined />}
                onClick={handleInteractiveExecute}
                loading={processing}
                block
              >
                {processing ? 'Executing...' : 'Execute Import'}
              </Button>
            )}

            <Space direction="vertical" size="middle" style={{ width: '100%' }}>
              <Space.Compact style={{ width: '100%' }}>
                <input
                  type="text"
                  value={userInput}
                  onChange={(e) => setUserInput(e.target.value)}
                  onKeyPress={(e) => {
                    if (e.key === 'Enter' && !processing) {
                      handleInteractiveSend();
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
                  onClick={handleInteractiveSend}
                  loading={processing}
                  disabled={!userInput.trim() || processing || !threadId}
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
    <div style={{ padding: '24px' }}>
      <Breadcrumb style={{ marginBottom: 16 }} items={breadcrumbItems} />

      <Button
        icon={<ArrowLeftOutlined />}
        onClick={() => navigate('/import')}
        style={{ marginBottom: 16 }}
      >
        Back to Import List
      </Button>

      {file.status === 'failed' ? (
        <Card title={`Failed Mapping: ${file.file_name}`}>
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Alert
              message="Mapping Failed"
              description="The file mapping process encountered an error. Please review the details below and try again."
              type="error"
              showIcon
            />

            {file.error_message && (
              <Card title="Error Details" size="small" type="inner">
                <ErrorLogViewer error={file.error_message} showRetry={false} />
              </Card>
            )}
            <Space>
              {!showInteractiveRetry && (
                <Button 
                  type="primary"
                  onClick={handleRetryInteractive}
                  disabled={processing}
                >
                  {processing ? 'Starting...' : 'Try Again'}
                </Button>
              )}
              <Button 
                icon={<ArrowLeftOutlined />}
                onClick={() => navigate('/import')}
              >
                Back to Import List
              </Button>
            </Space>

            {showInteractiveRetry && (
              <Card 
                title="Retry with AI Assistant" 
                size="small" 
                type="inner"
              >
                {interactiveTabContent}
              </Card>
            )}
          </Space>
        </Card>
      ) : file.status === 'mapped' ? (
        <Card title={`Mapped File: ${file.file_name}`}>
          {renderMappedFileView()}
        </Card>
      ) : result ? (
        <Card>
          {result.success ? (
            <Result
              status="success"
              title="File Mapped Successfully!"
              subTitle={`Your data has been imported into the database.`}
              extra={[
                <Button type="primary" key="list" onClick={() => navigate('/import')}>
                  Back to Import List
                </Button>,
                <Button key="query" onClick={() => navigate('/query')}>
                  Query Data
                </Button>,
              ]}
            >
              <Row gutter={16} style={{ marginTop: 24 }}>
                <Col span={8}>
                  <Statistic
                    title="Table Name"
                    value={result.table_name || 'N/A'}
                  />
                </Col>
                <Col span={8}>
                  <Statistic
                    title="Rows Imported"
                    value={result.rows_imported || 0}
                  />
                </Col>
                <Col span={8}>
                  <Statistic
                    title="Execution Time"
                    value={result.execution_time || 0}
                    suffix="s"
                    precision={2}
                  />
                </Col>
              </Row>
            </Result>
          ) : (
            <Result
              status="error"
              title="Import Failed"
              subTitle={result.error || 'An error occurred during import'}
              extra={[
                <Button type="primary" key="retry" onClick={() => setResult(null)}>
                  Try Again
                </Button>,
                <Button key="list" onClick={() => navigate('/import')}>
                  Back to Import List
                </Button>,
              ]}
            />
          )}
        </Card>
      ) : (
        <Card title={`Map File: ${file.file_name}`}>
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            items={tabItems}
          />
        </Card>
      )}
    </div>
  );
};

export default ImportMappingPage;
