/**
 * Workflow detail page - View, edit, and see execution history
 */

import React, { useState, useEffect } from 'react';
import { Card, Button, Space, Typography, Spin, Alert, Tabs, List, Tag, Modal, App as AntdApp } from 'antd';
import { ArrowLeftOutlined, PlayCircleOutlined, EditOutlined, SaveOutlined, CloseOutlined } from '@ant-design/icons';
import { useNavigate, useParams } from 'react-router';
import {
  getWorkflow,
  updateWorkflow,
  listExecutions,
  type Workflow,
  type WorkflowExecution,
  type CreateWorkflowRequest,
} from '../../api/workflows';
import { WorkflowPreview } from './WorkflowPreview';
import { WorkflowExecutor } from './WorkflowExecutor';

const { Title, Text } = Typography;

export const WorkflowDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { message } = AntdApp.useApp();

  const [workflow, setWorkflow] = useState<Workflow | null>(null);
  const [executions, setExecutions] = useState<WorkflowExecution[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isLoadingExecutions, setIsLoadingExecutions] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [editedWorkflow, setEditedWorkflow] = useState<CreateWorkflowRequest | null>(null);
  const [isExecutorOpen, setIsExecutorOpen] = useState(false);

  const loadWorkflow = async () => {
    if (!id) return;

    setIsLoading(true);
    setError(null);

    try {
      const response = await getWorkflow(id);
      if (response.success && response.workflow) {
        setWorkflow(response.workflow);
      } else {
        setError('Workflow not found');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load workflow');
    } finally {
      setIsLoading(false);
    }
  };

  const loadExecutions = async () => {
    if (!id) return;

    setIsLoadingExecutions(true);

    try {
      const response = await listExecutions(id, { limit: 20 });
      if (response.success) {
        setExecutions(response.executions);
      }
    } catch (err) {
      console.error('Failed to load executions:', err);
    } finally {
      setIsLoadingExecutions(false);
    }
  };

  useEffect(() => {
    loadWorkflow();
    loadExecutions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  const handleEdit = () => {
    if (!workflow) return;

    setEditedWorkflow({
      name: workflow.name,
      description: workflow.description,
      steps: workflow.steps || [],
      variables: workflow.variables || [],
    });
    setIsEditing(true);
  };

  const handleSave = async () => {
    if (!workflow || !editedWorkflow) return;

    setIsSaving(true);

    try {
      await updateWorkflow(workflow.id, {
        name: editedWorkflow.name,
        description: editedWorkflow.description,
      });

      message.success('Workflow updated successfully');
      setIsEditing(false);
      loadWorkflow();
    } catch (err) {
      message.error(err instanceof Error ? err.message : 'Failed to update workflow');
    } finally {
      setIsSaving(false);
    }
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    setEditedWorkflow(null);
  };

  const handleExecutionComplete = () => {
    setIsExecutorOpen(false);
    loadExecutions();
    message.success('Workflow executed successfully!');
  };

  if (isLoading) {
    return (
      <div style={{ padding: '24px', display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error || !workflow) {
    return (
      <div style={{ padding: '24px' }}>
        <Alert
          message="Error"
          description={error || 'Workflow not found'}
          type="error"
          showIcon
          action={
            <Button onClick={() => navigate('/workflows')}>
              Back to Workflows
            </Button>
          }
        />
      </div>
    );
  }

  return (
    <div style={{ padding: '24px' }}>
      {/* Header */}
      <Card
        className="surface-card"
        style={{ marginBottom: '24px' }}
        styles={{ body: { padding: '16px 24px' } }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/workflows')}>
              Back
            </Button>
            <div>
              <Title level={4} style={{ margin: 0 }}>
                {workflow.name}
              </Title>
              <Text type="secondary">{workflow.description || 'No description'}</Text>
            </div>
          </Space>
          <Space>
            {isEditing ? (
              <>
                <Button icon={<CloseOutlined />} onClick={handleCancelEdit} disabled={isSaving}>
                  Cancel
                </Button>
                <Button
                  type="primary"
                  icon={<SaveOutlined />}
                  onClick={handleSave}
                  loading={isSaving}
                >
                  Save Changes
                </Button>
              </>
            ) : (
              <>
                <Button icon={<EditOutlined />} onClick={handleEdit}>
                  Edit
                </Button>
                <Button
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  onClick={() => setIsExecutorOpen(true)}
                >
                  Execute
                </Button>
              </>
            )}
          </Space>
        </div>
      </Card>

      {/* Content */}
      <Card className="surface-card">
        <Tabs
          defaultActiveKey="details"
          items={[
            {
              key: 'details',
              label: 'Workflow Details',
              children: isEditing && editedWorkflow ? (
                <WorkflowPreview workflow={editedWorkflow} onChange={setEditedWorkflow} />
              ) : (
                <WorkflowPreview
                  workflow={{
                    name: workflow.name,
                    description: workflow.description,
                    steps: workflow.steps || [],
                    variables: workflow.variables || [],
                  }}
                  onChange={() => {}}
                />
              ),
            },
            {
              key: 'history',
              label: `Execution History (${executions.length})`,
              children: (
                <div>
                  {isLoadingExecutions ? (
                    <div style={{ textAlign: 'center', padding: '48px' }}>
                      <Spin />
                    </div>
                  ) : executions.length === 0 ? (
                    <Alert
                      message="No Executions Yet"
                      description="This workflow hasn't been executed yet. Click the Execute button to run it."
                      type="info"
                      showIcon
                    />
                  ) : (
                    <List
                      dataSource={executions}
                      renderItem={(execution) => (
                        <List.Item>
                          <List.Item.Meta
                            title={
                              <Space>
                                <Text strong>
                                  {execution.executed_at
                                    ? new Date(execution.executed_at).toLocaleString()
                                    : 'Unknown time'}
                                </Text>
                                <Tag color={execution.status === 'completed' ? 'success' : 'error'}>
                                  {execution.status}
                                </Tag>
                              </Space>
                            }
                            description={
                              <Space direction="vertical" size="small">
                                <Text type="secondary">
                                  Variables: {JSON.stringify(execution.variables_used)}
                                </Text>
                                {execution.executed_by && (
                                  <Text type="secondary">Executed by: {execution.executed_by}</Text>
                                )}
                              </Space>
                            }
                          />
                        </List.Item>
                      )}
                    />
                  )}
                </div>
              ),
            },
          ]}
        />
      </Card>

      {/* Executor Modal */}
      <Modal
        title={`Execute: ${workflow.name}`}
        open={isExecutorOpen}
        onCancel={() => setIsExecutorOpen(false)}
        footer={null}
        width={800}
        destroyOnClose
      >
        <WorkflowExecutor
          workflow={workflow}
          onComplete={handleExecutionComplete}
          onCancel={() => setIsExecutorOpen(false)}
        />
      </Modal>
    </div>
  );
};

export default WorkflowDetail;
