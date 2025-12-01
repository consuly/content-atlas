/**
 * Main Workflows page - List and manage workflows
 */

import React, { useState, useEffect } from 'react';
import { Card, Button, Space, Typography, Empty, Spin, App as AntdApp, Row, Col, Tag, Modal, Drawer } from 'antd';
import {
  PlusOutlined,
  PlayCircleOutlined,
  EditOutlined,
  DeleteOutlined,
  ExclamationCircleOutlined,
  RocketOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router';
import { listWorkflows, deleteWorkflow, type Workflow } from '../../api/workflows';
import { WorkflowBuilder } from './WorkflowBuilder';
import { WorkflowExecutor } from './WorkflowExecutor';

const { Title, Text, Paragraph } = Typography;

export const WorkflowsPage: React.FC = () => {
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isBuilderOpen, setIsBuilderOpen] = useState(false);
  const [executingWorkflow, setExecutingWorkflow] = useState<Workflow | null>(null);
  const { message, modal } = AntdApp.useApp();
  const navigate = useNavigate();

  const loadWorkflows = async () => {
    setIsLoading(true);
    try {
      const response = await listWorkflows({ limit: 100, active_only: true });
      if (response.success) {
        setWorkflows(response.workflows);
      }
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to load workflows');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadWorkflows();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleDelete = (workflow: Workflow) => {
    modal.confirm({
      title: 'Delete Workflow',
      icon: <ExclamationCircleOutlined />,
      content: `Are you sure you want to delete "${workflow.name}"? This will also delete all execution history.`,
      okText: 'Delete',
      okType: 'danger',
      onOk: async () => {
        try {
          await deleteWorkflow(workflow.id);
          message.success('Workflow deleted successfully');
          loadWorkflows();
        } catch (error) {
          message.error(error instanceof Error ? error.message : 'Failed to delete workflow');
        }
      },
    });
  };

  const handleWorkflowCreated = () => {
    setIsBuilderOpen(false);
    loadWorkflows();
    message.success('Workflow created successfully!');
  };

  const handleExecutionComplete = () => {
    setExecutingWorkflow(null);
    message.success('Workflow executed successfully!');
  };

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
            <RocketOutlined style={{ fontSize: '24px', color: '#1890ff' }} />
            <div>
              <Title level={4} style={{ margin: 0 }}>
                Workflows
              </Title>
              <Text type="secondary">Create and manage automated data analysis workflows</Text>
            </div>
          </Space>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setIsBuilderOpen(true)}
            size="large"
          >
            Create Workflow
          </Button>
        </div>
      </Card>

      {/* Workflows List */}
      {isLoading ? (
        <div style={{ display: 'flex', justifyContent: 'center', padding: '48px' }}>
          <Spin size="large" />
        </div>
      ) : workflows.length === 0 ? (
        <Card className="surface-card">
          <Empty
            image={<RocketOutlined style={{ fontSize: 64, color: '#1890ff' }} />}
            description={
              <div>
                <Title level={4}>No Workflows Yet</Title>
                <Paragraph type="secondary">
                  Create your first workflow to automate data analysis tasks. Workflows are reusable
                  templates with configurable variables that execute a series of SQL queries.
                </Paragraph>
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={() => setIsBuilderOpen(true)}
                  size="large"
                >
                  Create Your First Workflow
                </Button>
              </div>
            }
          />
        </Card>
      ) : (
        <Row gutter={[16, 16]}>
          {workflows.map((workflow) => (
            <Col xs={24} sm={24} md={12} lg={8} xl={6} key={workflow.id}>
              <Card
                className="surface-card"
                hoverable
                styles={{ body: { padding: '20px' } }}
                actions={[
                  <Button
                    type="text"
                    icon={<PlayCircleOutlined />}
                    onClick={() => setExecutingWorkflow(workflow)}
                    key="execute"
                  >
                    Execute
                  </Button>,
                  <Button
                    type="text"
                    icon={<EditOutlined />}
                    onClick={() => navigate(`/workflows/${workflow.id}`)}
                    key="edit"
                  >
                    Edit
                  </Button>,
                  <Button
                    type="text"
                    danger
                    icon={<DeleteOutlined />}
                    onClick={() => handleDelete(workflow)}
                    key="delete"
                  >
                    Delete
                  </Button>,
                ]}
              >
                <div style={{ marginBottom: '12px' }}>
                  <Title level={5} style={{ margin: 0, marginBottom: '8px' }}>
                    {workflow.name}
                  </Title>
                  <Paragraph
                    type="secondary"
                    ellipsis={{ rows: 2 }}
                    style={{ marginBottom: '12px', minHeight: '40px' }}
                  >
                    {workflow.description || 'No description'}
                  </Paragraph>
                </div>
                <Space size="small" wrap>
                  <Tag color="blue">{workflow.step_count || 0} steps</Tag>
                  <Tag color="green">{workflow.variable_count || 0} variables</Tag>
                  {!workflow.is_active && <Tag color="red">Inactive</Tag>}
                </Space>
                {workflow.created_at && (
                  <div style={{ marginTop: '12px' }}>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Created {new Date(workflow.created_at).toLocaleDateString()}
                    </Text>
                  </div>
                )}
              </Card>
            </Col>
          ))}
        </Row>
      )}

      {/* Workflow Builder Drawer */}
      <Drawer
        title="Create Workflow"
        placement="right"
        width="60%"
        open={isBuilderOpen}
        onClose={() => setIsBuilderOpen(false)}
        styles={{ body: { padding: 0 } }}
      >
        <WorkflowBuilder onWorkflowCreated={handleWorkflowCreated} />
      </Drawer>

      {/* Workflow Executor Modal */}
      <Modal
        title={`Execute: ${executingWorkflow?.name}`}
        open={!!executingWorkflow}
        onCancel={() => setExecutingWorkflow(null)}
        footer={null}
        width={800}
        destroyOnClose
      >
        {executingWorkflow && (
          <WorkflowExecutor
            workflow={executingWorkflow}
            onComplete={handleExecutionComplete}
            onCancel={() => setExecutingWorkflow(null)}
          />
        )}
      </Modal>
    </div>
  );
};

export default WorkflowsPage;
