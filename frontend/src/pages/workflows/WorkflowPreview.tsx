/**
 * Preview and edit workflow definition
 */

import React from 'react';
import { Card, Input, Select, Space, Typography, Tag, Divider, Checkbox } from 'antd';
import { OrderedListOutlined, SettingOutlined } from '@ant-design/icons';
import type { CreateWorkflowRequest, WorkflowStep, WorkflowVariable } from '../../api/workflows';

const { Title, Text } = Typography;
const { TextArea } = Input;

interface WorkflowPreviewProps {
  workflow: CreateWorkflowRequest;
  onChange: (workflow: CreateWorkflowRequest) => void;
}

export const WorkflowPreview: React.FC<WorkflowPreviewProps> = ({ workflow, onChange }) => {
  const handleNameChange = (name: string) => {
    onChange({ ...workflow, name });
  };

  const handleDescriptionChange = (description: string) => {
    onChange({ ...workflow, description });
  };

  const handleStepChange = (index: number, field: keyof WorkflowStep, value: string | number) => {
    const newSteps = [...workflow.steps];
    newSteps[index] = { ...newSteps[index], [field]: value };
    onChange({ ...workflow, steps: newSteps });
  };

  const handleVariableChange = (index: number, field: keyof WorkflowVariable, value: string | boolean | string[]) => {
    const newVariables = [...workflow.variables];
    newVariables[index] = { ...newVariables[index], [field]: value };
    onChange({ ...workflow, variables: newVariables });
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      {/* Workflow Metadata */}
      <div>
        <Title level={5}>Workflow Details</Title>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <div>
            <Text strong>Name</Text>
            <Input
              value={workflow.name}
              onChange={(e) => handleNameChange(e.target.value)}
              placeholder="Workflow name"
              style={{ marginTop: '4px' }}
            />
          </div>
          <div>
            <Text strong>Description</Text>
            <TextArea
              value={workflow.description || ''}
              onChange={(e) => handleDescriptionChange(e.target.value)}
              placeholder="Workflow description"
              rows={2}
              style={{ marginTop: '4px' }}
            />
          </div>
        </Space>
      </div>

      <Divider />

      {/* Steps */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <OrderedListOutlined style={{ fontSize: '18px', color: '#1890ff' }} />
          <Title level={5} style={{ margin: 0 }}>
            Steps ({workflow.steps.length})
          </Title>
        </div>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          {workflow.steps.map((step, index) => (
            <Card
              key={index}
              size="small"
              title={
                <Space>
                  <Tag color="blue">Step {step.step_order}</Tag>
                  <Text strong>{step.name || `Step ${step.step_order}`}</Text>
                </Space>
              }
              styles={{ body: { padding: '12px' } }}
            >
              <Space direction="vertical" size="small" style={{ width: '100%' }}>
                <div>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    Step Name (optional)
                  </Text>
                  <Input
                    value={step.name || ''}
                    onChange={(e) => handleStepChange(index, 'name', e.target.value)}
                    placeholder="e.g., Total Revenue"
                    size="small"
                    style={{ marginTop: '4px' }}
                  />
                </div>
                <div>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    Prompt Template
                  </Text>
                  <TextArea
                    value={step.prompt_template}
                    onChange={(e) => handleStepChange(index, 'prompt_template', e.target.value)}
                    placeholder="LLM prompt with {{variable}} placeholders"
                    rows={3}
                    size="small"
                    style={{ marginTop: '4px' }}
                  />
                </div>
              </Space>
            </Card>
          ))}
        </Space>
      </div>

      <Divider />

      {/* Variables */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <SettingOutlined style={{ fontSize: '18px', color: '#52c41a' }} />
          <Title level={5} style={{ margin: 0 }}>
            Variables ({workflow.variables.length})
          </Title>
        </div>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          {workflow.variables.map((variable, index) => (
            <Card
              key={index}
              size="small"
              title={
                <Space>
                  <Tag color="green">{variable.variable_type}</Tag>
                  <Text strong>{variable.display_name || variable.name}</Text>
                  {variable.required && <Tag color="red">Required</Tag>}
                </Space>
              }
              styles={{ body: { padding: '12px' } }}
            >
              <Space direction="vertical" size="small" style={{ width: '100%' }}>
                <div style={{ display: 'flex', gap: '8px' }}>
                  <div style={{ flex: 1 }}>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Variable Name
                    </Text>
                    <Input
                      value={variable.name}
                      onChange={(e) => handleVariableChange(index, 'name', e.target.value)}
                      placeholder="e.g., start_date"
                      size="small"
                      style={{ marginTop: '4px' }}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Display Name
                    </Text>
                    <Input
                      value={variable.display_name || ''}
                      onChange={(e) => handleVariableChange(index, 'display_name', e.target.value)}
                      placeholder="e.g., Start Date"
                      size="small"
                      style={{ marginTop: '4px' }}
                    />
                  </div>
                </div>
                <div style={{ display: 'flex', gap: '8px' }}>
                  <div style={{ flex: 1 }}>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Type
                    </Text>
                    <Select
                      value={variable.variable_type}
                      onChange={(value) => handleVariableChange(index, 'variable_type', value)}
                      size="small"
                      style={{ width: '100%', marginTop: '4px' }}
                      options={[
                        { label: 'Text', value: 'text' },
                        { label: 'Date', value: 'date' },
                        { label: 'Number', value: 'number' },
                        { label: 'Select', value: 'select' },
                      ]}
                    />
                  </div>
                  <div style={{ flex: 1 }}>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Default Value
                    </Text>
                    <Input
                      value={variable.default_value || ''}
                      onChange={(e) => handleVariableChange(index, 'default_value', e.target.value)}
                      placeholder="Optional default"
                      size="small"
                      style={{ marginTop: '4px' }}
                    />
                  </div>
                </div>
                {variable.variable_type === 'select' && (
                  <div>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Options (comma-separated)
                    </Text>
                    <Input
                      value={variable.options?.join(', ') || ''}
                      onChange={(e) =>
                        handleVariableChange(
                          index,
                          'options',
                          e.target.value.split(',').map((s) => s.trim()).filter(Boolean)
                        )
                      }
                      placeholder="e.g., Option 1, Option 2, Option 3"
                      size="small"
                      style={{ marginTop: '4px' }}
                    />
                  </div>
                )}
                <div>
                  <Checkbox
                    checked={variable.required}
                    onChange={(e) => handleVariableChange(index, 'required', e.target.checked)}
                  >
                    Required
                  </Checkbox>
                </div>
              </Space>
            </Card>
          ))}
        </Space>
      </div>
    </Space>
  );
};

export default WorkflowPreview;
