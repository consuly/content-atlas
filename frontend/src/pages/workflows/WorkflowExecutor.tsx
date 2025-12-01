/**
 * Execute workflow with variable inputs and display results
 */

import React, { useState } from 'react';
import { Form, Input, DatePicker, InputNumber, Select, Button, Space, Alert, Spin, Card, Typography, Tag, Collapse } from 'antd';
import { PlayCircleOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { executeWorkflow, type Workflow, type WorkflowStepResult } from '../../api/workflows';
import { ResultsTable } from '../query/ResultsTable';

const { Text, Paragraph } = Typography;
const { Panel } = Collapse;

interface WorkflowExecutorProps {
  workflow: Workflow;
  onComplete: () => void;
  onCancel: () => void;
}

export const WorkflowExecutor: React.FC<WorkflowExecutorProps> = ({ workflow, onComplete, onCancel }) => {
  const [form] = Form.useForm();
  const [isExecuting, setIsExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [results, setResults] = useState<WorkflowStepResult[] | null>(null);
  const [executionTime, setExecutionTime] = useState<number | null>(null);

  const handleExecute = async () => {
    try {
      const values = await form.validateFields();
      
      // Convert date values to strings
      const variables: Record<string, unknown> = {};
      workflow.variables?.forEach((variable) => {
        const value = values[variable.name];
        if (variable.variable_type === 'date' && value) {
          variables[variable.name] = dayjs(value).format('YYYY-MM-DD');
        } else {
          variables[variable.name] = value;
        }
      });

      setIsExecuting(true);
      setError(null);
      setResults(null);

      const response = await executeWorkflow(workflow.id, {
        variables,
        include_context: true,
      });

      if (response.success && response.step_results) {
        setResults(response.step_results);
        setExecutionTime(response.total_execution_time_seconds || null);
      } else {
        setError(response.error || 'Execution failed');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to execute workflow');
    } finally {
      setIsExecuting(false);
    }
  };

  const renderVariableInput = (variable: NonNullable<Workflow['variables']>[0]) => {
    const commonProps = {
      placeholder: variable.default_value || undefined,
    };

    switch (variable.variable_type) {
      case 'date':
        return (
          <DatePicker
            {...commonProps}
            style={{ width: '100%' }}
            format="YYYY-MM-DD"
            defaultValue={variable.default_value ? dayjs(variable.default_value) : undefined}
          />
        );
      case 'number':
        return (
          <InputNumber
            {...commonProps}
            style={{ width: '100%' }}
            defaultValue={variable.default_value ? Number(variable.default_value) : undefined}
          />
        );
      case 'select':
        return (
          <Select
            {...commonProps}
            options={variable.options?.map((opt) => ({ label: opt, value: opt }))}
            defaultValue={variable.default_value}
          />
        );
      case 'text':
      default:
        return <Input {...commonProps} defaultValue={variable.default_value} />;
    }
  };

  if (results) {
    return (
      <div>
        <Alert
          message="Execution Complete"
          description={
            <div>
              <Text>
                Workflow executed successfully in {executionTime?.toFixed(2)} seconds
              </Text>
            </div>
          }
          type="success"
          showIcon
          style={{ marginBottom: '16px' }}
        />

        <Collapse defaultActiveKey={results.map((_, i) => i.toString())}>
          {results.map((result, index) => (
            <Panel
              key={index}
              header={
                <Space>
                  {result.status === 'success' ? (
                    <CheckCircleOutlined style={{ color: '#52c41a' }} />
                  ) : (
                    <CloseCircleOutlined style={{ color: '#ff4d4f' }} />
                  )}
                  <Text strong>
                    Step {result.step_order}: {result.step_name || `Step ${result.step_order}`}
                  </Text>
                  <Tag color={result.status === 'success' ? 'success' : 'error'}>
                    {result.status}
                  </Tag>
                  {result.rows_returned !== undefined && (
                    <Tag color="blue">{result.rows_returned} rows</Tag>
                  )}
                  {result.execution_time_seconds !== undefined && (
                    <Tag>{result.execution_time_seconds.toFixed(2)}s</Tag>
                  )}
                </Space>
              }
            >
              <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                {result.response && (
                  <div>
                    <Text strong>Response:</Text>
                    <Paragraph style={{ marginTop: '8px' }}>{result.response}</Paragraph>
                  </div>
                )}

                {result.executed_sql && (
                  <div>
                    <Text strong>SQL Query:</Text>
                    <Card size="small" style={{ marginTop: '8px' }}>
                      <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{result.executed_sql}</pre>
                    </Card>
                  </div>
                )}

                {result.result_csv && (
                  <div>
                    <Text strong>Results:</Text>
                    <div style={{ marginTop: '8px' }}>
                      <ResultsTable csvData={result.result_csv} />
                    </div>
                  </div>
                )}

                {result.error_message && (
                  <Alert
                    message="Error"
                    description={result.error_message}
                    type="error"
                    showIcon
                  />
                )}
              </Space>
            </Panel>
          ))}
        </Collapse>

        <div style={{ marginTop: '16px', display: 'flex', justifyContent: 'flex-end' }}>
          <Button type="primary" onClick={onComplete}>
            Done
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div>
      {workflow.description && (
        <Alert
          message="Workflow Description"
          description={workflow.description}
          type="info"
          showIcon
          style={{ marginBottom: '16px' }}
        />
      )}

      {error && (
        <Alert
          message="Execution Error"
          description={error}
          type="error"
          closable
          onClose={() => setError(null)}
          style={{ marginBottom: '16px' }}
        />
      )}

      <Form form={form} layout="vertical">
        {workflow.variables?.map((variable) => (
          <Form.Item
            key={variable.name}
            name={variable.name}
            label={variable.display_name || variable.name}
            rules={[
              {
                required: variable.required,
                message: `${variable.display_name || variable.name} is required`,
              },
            ]}
          >
            {renderVariableInput(variable)}
          </Form.Item>
        ))}
      </Form>

      <div style={{ marginTop: '16px', display: 'flex', justifyContent: 'flex-end', gap: '8px' }}>
        <Button onClick={onCancel} disabled={isExecuting}>
          Cancel
        </Button>
        <Button
          type="primary"
          icon={<PlayCircleOutlined />}
          onClick={handleExecute}
          loading={isExecuting}
        >
          {isExecuting ? 'Executing...' : 'Execute Workflow'}
        </Button>
      </div>

      {isExecuting && (
        <div style={{ marginTop: '16px', textAlign: 'center' }}>
          <Space direction="vertical" align="center">
            <Spin size="large" />
            <Text type="secondary">Executing workflow steps...</Text>
          </Space>
        </div>
      )}
    </div>
  );
};

export default WorkflowExecutor;
