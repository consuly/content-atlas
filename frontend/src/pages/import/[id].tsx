import React, { useState, useEffect, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router';
import { Card, Tabs, Button, Space, Alert, Spin, Typography, Result, Statistic, Row, Col, Breadcrumb, Descriptions, Table, Tag, Divider } from 'antd';
import { ThunderboltOutlined, MessageOutlined, CheckCircleOutlined, ArrowLeftOutlined, HomeOutlined, FileOutlined, DatabaseOutlined, InfoCircleOutlined, EyeOutlined } from '@ant-design/icons';
import axios, { AxiosError } from 'axios';

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
        setTableData({
          data: tableResponse.data.data,
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

  useEffect(() => {
    if (file?.status === 'mapped' && file.mapped_table_name) {
      fetchMappedFileDetails(file.mapped_table_name);
    }
  }, [file, fetchMappedFileDetails]);

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

  const handleInteractiveStart = async () => {
    if (!id) return;
    
    setProcessing(true);
    setError(null);
    setConversation([]);
    setResult(null);

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: id,
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
    } finally {
      setProcessing(false);
    }
  };

  const handleInteractiveSend = async () => {
    if (!userInput.trim() || !threadId || !id) return;

    setProcessing(true);
    setError(null);

    const newConversation = [...conversation, { role: 'user' as const, content: userInput }];
    setConversation(newConversation);
    setUserInput('');

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(
        `${API_URL}/analyze-file-interactive`,
        {
          file_id: id,
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
    } finally {
      setProcessing(false);
    }
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
        setResult({
          success: true,
          table_name: response.data.table_name,
          rows_imported: response.data.rows_imported,
          execution_time: response.data.execution_time,
        });
      } else {
        setResult({
          success: false,
          error: 'Import execution failed',
        });
      }
    } catch (err) {
      const error = err as AxiosError<{ detail?: string }>;
      const errorMsg = error.response?.data?.detail || error.message || 'Import execution failed';
      setResult({
        success: false,
        error: errorMsg,
      });
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
          <Descriptions column={2} bordered size="small">
            <Descriptions.Item label="Table Name">
              <Tag color="blue">{file.mapped_table_name}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Mapped Date">
              {formatDate(file.mapped_date)}
            </Descriptions.Item>
            <Descriptions.Item label="Rows Imported">
              <Text strong>{file.mapped_rows?.toLocaleString() || 0}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="File Size">
              {formatBytes(file.file_size)}
            </Descriptions.Item>
            <Descriptions.Item label="Upload Date">
              {formatDate(file.upload_date)}
            </Descriptions.Item>
            <Descriptions.Item label="Status">
              <Tag color="success">Mapped</Tag>
            </Descriptions.Item>
          </Descriptions>
        </Card>

        {/* Import Details */}
        {importHistory && (
          <Card title={<><DatabaseOutlined /> Import Details</>} size="small" loading={loadingDetails}>
            <Descriptions column={2} bordered size="small">
              {importHistory.import_strategy && (
                <Descriptions.Item label="Import Strategy" span={2}>
                  <Tag>{importHistory.import_strategy}</Tag>
                </Descriptions.Item>
              )}
              <Descriptions.Item label="Total Rows in File">
                {importHistory.total_rows_in_file?.toLocaleString() || '-'}
              </Descriptions.Item>
              <Descriptions.Item label="Rows Inserted">
                {importHistory.rows_inserted?.toLocaleString() || '-'}
              </Descriptions.Item>
              {importHistory.duplicates_found !== undefined && importHistory.duplicates_found > 0 && (
                <Descriptions.Item label="Duplicates Found">
                  <Text type="warning">{importHistory.duplicates_found.toLocaleString()}</Text>
                </Descriptions.Item>
              )}
              {importHistory.data_validation_errors !== undefined && importHistory.data_validation_errors > 0 && (
                <Descriptions.Item label="Validation Errors">
                  <Text type="danger">{importHistory.data_validation_errors.toLocaleString()}</Text>
                </Descriptions.Item>
              )}
              {importHistory.duration_seconds && (
                <Descriptions.Item label="Processing Time">
                  {importHistory.duration_seconds.toFixed(2)}s
                </Descriptions.Item>
              )}
              <Descriptions.Item label="Import ID" span={2}>
                <Text code style={{ fontSize: '11px' }}>{importHistory.import_id}</Text>
              </Descriptions.Item>
            </Descriptions>
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
              columns={Object.keys(tableData.data[0] || {}).map(key => ({
                title: key,
                dataIndex: key,
                key: key,
                ellipsis: true,
                width: 150,
              }))}
              pagination={false}
              scroll={{ x: 'max-content' }}
              size="small"
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

          <Button
            type="primary"
            size="large"
            icon={<MessageOutlined />}
            onClick={handleInteractiveStart}
            loading={processing}
            block
          >
            {processing ? 'Starting...' : 'Start Interactive Analysis'}
          </Button>
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

          {canExecute ? (
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
          ) : (
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
                placeholder="Type your response..."
                disabled={processing}
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
    <div style={{ padding: '24px' }}>
      <Breadcrumb style={{ marginBottom: 16 }}>
        <Breadcrumb.Item>
          <a onClick={() => navigate('/import')} style={{ cursor: 'pointer' }}>
            <HomeOutlined />
            <span style={{ marginLeft: 8 }}>Import</span>
          </a>
        </Breadcrumb.Item>
        <Breadcrumb.Item>
          <FileOutlined />
          <span style={{ marginLeft: 8 }}>{file.file_name}</span>
        </Breadcrumb.Item>
      </Breadcrumb>

      <Button
        icon={<ArrowLeftOutlined />}
        onClick={() => navigate('/import')}
        style={{ marginBottom: 16 }}
      >
        Back to Import List
      </Button>

      {file.status === 'mapped' ? (
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
