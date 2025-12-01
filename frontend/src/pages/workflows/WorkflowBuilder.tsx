/**
 * Chat-based workflow builder using LLM
 */

import React, { useState } from 'react';
import { Card, Input, Button, Space, Typography, Spin, Alert } from 'antd';
import { SendOutlined, RobotOutlined } from '@ant-design/icons';
import { generateWorkflow, createWorkflow, type CreateWorkflowRequest } from '../../api/workflows';
import { WorkflowPreview } from './WorkflowPreview';

const { TextArea } = Input;
const { Title, Text, Paragraph } = Typography;

interface WorkflowBuilderProps {
  onWorkflowCreated: () => void;
}

export const WorkflowBuilder: React.FC<WorkflowBuilderProps> = ({ onWorkflowCreated }) => {
  const [description, setDescription] = useState('');
  const [isGenerating, setIsGenerating] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [generatedWorkflow, setGeneratedWorkflow] = useState<CreateWorkflowRequest | null>(null);
  const [llmResponse, setLlmResponse] = useState<string | null>(null);

  const handleGenerate = async () => {
    if (!description.trim()) return;

    setIsGenerating(true);
    setError(null);
    setGeneratedWorkflow(null);
    setLlmResponse(null);

    try {
      const response = await generateWorkflow(description);

      if (response.success && response.workflow) {
        setGeneratedWorkflow(response.workflow as CreateWorkflowRequest);
        setLlmResponse(response.llm_response || null);
      } else {
        setError(response.error || 'Failed to generate workflow');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate workflow');
    } finally {
      setIsGenerating(false);
    }
  };

  const handleSave = async () => {
    if (!generatedWorkflow) return;

    setIsSaving(true);
    setError(null);

    try {
      await createWorkflow(generatedWorkflow);
      onWorkflowCreated();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save workflow');
    } finally {
      setIsSaving(false);
    }
  };

  const handleWorkflowChange = (updated: CreateWorkflowRequest) => {
    setGeneratedWorkflow(updated);
  };

  const examplePrompts = [
    'Create a workflow to analyze monthly revenue by client with date filters',
    'Build a workflow for tracking top products by sales with configurable time period',
    'Generate a workflow to compare regional performance with date range selection',
  ];

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Instructions */}
      {!generatedWorkflow && (
        <Card
          className="surface-card"
          style={{ margin: '16px', flexShrink: 0 }}
          styles={{ body: { padding: '16px' } }}
        >
          <Space direction="vertical" size="small" style={{ width: '100%' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <RobotOutlined style={{ fontSize: '20px', color: '#1890ff' }} />
              <Title level={5} style={{ margin: 0 }}>
                Describe Your Workflow
              </Title>
            </div>
            <Paragraph type="secondary" style={{ margin: 0 }}>
              Tell me what kind of data analysis workflow you want to create. I'll generate a workflow
              with steps and configurable variables based on your database schema.
            </Paragraph>
          </Space>
        </Card>
      )}

      {/* Input Area */}
      {!generatedWorkflow && (
        <Card
          className="surface-card"
          style={{ margin: '0 16px 16px', flexShrink: 0 }}
          styles={{ body: { padding: '16px' } }}
        >
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <TextArea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Example: Create a workflow to analyze monthly revenue by client with date filters..."
              rows={4}
              disabled={isGenerating}
            />
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text type="secondary" style={{ fontSize: '12px' }}>
                Try these examples:
              </Text>
              <Button
                type="primary"
                icon={<SendOutlined />}
                onClick={handleGenerate}
                loading={isGenerating}
                disabled={!description.trim()}
              >
                Generate Workflow
              </Button>
            </div>
            <Space size="small" wrap>
              {examplePrompts.map((prompt, index) => (
                <Button
                  key={index}
                  size="small"
                  type="link"
                  onClick={() => setDescription(prompt)}
                  disabled={isGenerating}
                >
                  • {prompt}
                </Button>
              ))}
            </Space>
          </Space>
        </Card>
      )}

      {/* Loading State */}
      {isGenerating && (
        <div style={{ flex: 1, display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
          <Space direction="vertical" align="center">
            <Spin size="large" />
            <Text type="secondary">Analyzing your database and generating workflow...</Text>
          </Space>
        </div>
      )}

      {/* Error */}
      {error && (
        <Alert
          message="Error"
          description={error}
          type="error"
          closable
          onClose={() => setError(null)}
          style={{ margin: '0 16px 16px' }}
        />
      )}

      {/* Generated Workflow Preview */}
      {generatedWorkflow && (
        <div style={{ flex: 1, overflow: 'auto', padding: '0 16px 16px' }}>
          <Card className="surface-card" styles={{ body: { padding: '16px' } }}>
            {llmResponse && (
              <Alert
                message="LLM Response"
                description={llmResponse}
                type="info"
                style={{ marginBottom: '16px' }}
              />
            )}
            <WorkflowPreview workflow={generatedWorkflow} onChange={handleWorkflowChange} />
          </Card>
        </div>
      )}

      {/* Actions */}
      {generatedWorkflow && (
        <Card
          className="surface-card"
          style={{ margin: '0 16px 16px', flexShrink: 0 }}
          styles={{ body: { padding: '16px' } }}
        >
          <Space style={{ width: '100%', justifyContent: 'flex-end' }}>
            <Button
              onClick={() => {
                setGeneratedWorkflow(null);
                setLlmResponse(null);
                setDescription('');
              }}
              disabled={isSaving}
            >
              Start Over
            </Button>
            <Button type="primary" onClick={handleSave} loading={isSaving}>
              Save Workflow
            </Button>
          </Space>
        </Card>
      )}
    </div>
  );
};

export default WorkflowBuilder;
