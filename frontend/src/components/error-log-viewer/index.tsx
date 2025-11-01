import React from 'react';
import { Alert, Collapse, Typography, Space, Tag, Button } from 'antd';
import { ExclamationCircleOutlined, ReloadOutlined, InfoCircleOutlined } from '@ant-design/icons';

const { Panel } = Collapse;
const { Text, Paragraph } = Typography;

interface ErrorDetails {
  error_type?: string;
  timestamp?: string;
  strategy_attempted?: string;
  target_table?: string;
  llm_decision_context?: Record<string, unknown>;
  suggestions?: string[];
  error_history?: string[];
}

interface ErrorLogViewerProps {
  error: string;
  errorDetails?: ErrorDetails;
  onRetry?: () => void;
  showRetry?: boolean;
}

export const ErrorLogViewer: React.FC<ErrorLogViewerProps> = ({
  error,
  errorDetails,
  onRetry,
  showRetry = true,
}) => {
  const getErrorTypeColor = (type?: string) => {
    switch (type) {
      case 'EXECUTION_FAILED':
        return 'error';
      case 'SCHEMA_MISMATCH':
        return 'warning';
      case 'VALIDATION_ERROR':
        return 'warning';
      default:
        return 'default';
    }
  };

  const getErrorTypeLabel = (type?: string) => {
    switch (type) {
      case 'EXECUTION_FAILED':
        return 'Execution Failed';
      case 'SCHEMA_MISMATCH':
        return 'Schema Mismatch';
      case 'VALIDATION_ERROR':
        return 'Validation Error';
      default:
        return 'Error';
    }
  };

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Alert
        message="Import Failed"
        description={error}
        type="error"
        showIcon
        icon={<ExclamationCircleOutlined />}
        action={
          showRetry && onRetry ? (
            <Button
              size="small"
              type="primary"
              icon={<ReloadOutlined />}
              onClick={onRetry}
            >
              Retry
            </Button>
          ) : undefined
        }
      />

      {errorDetails && (
        <Collapse
          bordered={false}
          style={{ background: '#fafafa' }}
          expandIconPosition="end"
        >
          <Panel
            header={
              <Space>
                <InfoCircleOutlined />
                <Text strong>Error Details</Text>
                {errorDetails.error_type && (
                  <Tag color={getErrorTypeColor(errorDetails.error_type)}>
                    {getErrorTypeLabel(errorDetails.error_type)}
                  </Tag>
                )}
              </Space>
            }
            key="1"
          >
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              {errorDetails.timestamp && (
                <div>
                  <Text strong>Timestamp: </Text>
                  <Text type="secondary">
                    {new Date(errorDetails.timestamp).toLocaleString()}
                  </Text>
                </div>
              )}

              {errorDetails.strategy_attempted && (
                <div>
                  <Text strong>Strategy Attempted: </Text>
                  <Tag>{errorDetails.strategy_attempted}</Tag>
                </div>
              )}

              {errorDetails.target_table && (
                <div>
                  <Text strong>Target Table: </Text>
                  <Tag color="blue">{errorDetails.target_table}</Tag>
                </div>
              )}

              {errorDetails.llm_decision_context && (
                <div>
                  <Text strong>LLM Decision Context:</Text>
                  <Paragraph
                    code
                    style={{
                      marginTop: 8,
                      padding: 12,
                      background: '#fff',
                      borderRadius: 4,
                      fontSize: 12,
                      maxHeight: 200,
                      overflow: 'auto',
                    }}
                  >
                    {JSON.stringify(errorDetails.llm_decision_context, null, 2)}
                  </Paragraph>
                </div>
              )}

              {errorDetails.suggestions && errorDetails.suggestions.length > 0 && (
                <div>
                  <Text strong>Suggestions:</Text>
                  <ul style={{ marginTop: 8, marginBottom: 0 }}>
                    {errorDetails.suggestions.map((suggestion, idx) => (
                      <li key={idx}>
                        <Text>{suggestion}</Text>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {errorDetails.error_history && errorDetails.error_history.length > 0 && (
                <div>
                  <Text strong>Error History:</Text>
                  <div style={{ marginTop: 8 }}>
                    {errorDetails.error_history.map((historyError, idx) => (
                      <Alert
                        key={idx}
                        message={`Attempt ${idx + 1}`}
                        description={historyError}
                        type="warning"
                        showIcon
                        style={{ marginBottom: 8 }}
                      />
                    ))}
                  </div>
                </div>
              )}
            </Space>
          </Panel>
        </Collapse>
      )}
    </Space>
  );
};

export default ErrorLogViewer;
