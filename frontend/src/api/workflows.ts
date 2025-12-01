/**
 * API integration for workflow management
 */

import { API_URL } from '../config';

// Types
export interface WorkflowVariable {
  id?: string;
  name: string;
  display_name?: string;
  variable_type: 'text' | 'date' | 'number' | 'select';
  default_value?: string;
  options?: string[];
  required: boolean;
}

export interface WorkflowStep {
  id?: string;
  step_order: number;
  name?: string;
  prompt_template: string;
}

export interface Workflow {
  id: string;
  name: string;
  description?: string;
  created_by?: string;
  created_at?: string;
  updated_at?: string;
  is_active: boolean;
  steps?: WorkflowStep[];
  variables?: WorkflowVariable[];
  step_count?: number;
  variable_count?: number;
}

export interface WorkflowStepResult {
  step_order: number;
  step_name?: string;
  executed_sql?: string;
  result_csv?: string;
  rows_returned?: number;
  execution_time_seconds?: number;
  status: string;
  error_message?: string;
  response?: string;
}

export interface WorkflowExecution {
  id: string;
  workflow_id: string;
  workflow_name?: string;
  executed_by?: string;
  executed_at?: string;
  status: string;
  variables_used: Record<string, unknown>;
  completed_at?: string;
  step_count?: number;
  step_results?: WorkflowStepResult[];
  total_execution_time_seconds?: number;
}

export interface GenerateWorkflowResponse {
  success: boolean;
  workflow?: {
    name: string;
    description?: string;
    steps: WorkflowStep[];
    variables: WorkflowVariable[];
  };
  llm_response?: string;
  error?: string;
}

export interface CreateWorkflowRequest {
  name: string;
  description?: string;
  steps: WorkflowStep[];
  variables: WorkflowVariable[];
  created_by?: string;
}

export interface UpdateWorkflowRequest {
  name?: string;
  description?: string;
  is_active?: boolean;
}

export interface ExecuteWorkflowRequest {
  variables: Record<string, unknown>;
  executed_by?: string;
  include_context?: boolean;
}

const getAuthHeaders = (): HeadersInit => {
  const token = localStorage.getItem('refine-auth');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };

  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  return headers;
};

/**
 * Generate a workflow from natural language description
 */
export const generateWorkflow = async (description: string): Promise<GenerateWorkflowResponse> => {
  const response = await fetch(`${API_URL}/workflows/generate`, {
    method: 'POST',
    headers: getAuthHeaders(),
    body: JSON.stringify({ description }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to generate workflow' }));
    throw new Error(error.detail || 'Failed to generate workflow');
  }

  return response.json();
};

/**
 * Create a new workflow
 */
export const createWorkflow = async (request: CreateWorkflowRequest): Promise<{ success: boolean; workflow_id: string; message: string }> => {
  const response = await fetch(`${API_URL}/workflows`, {
    method: 'POST',
    headers: getAuthHeaders(),
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to create workflow' }));
    throw new Error(error.detail || 'Failed to create workflow');
  }

  return response.json();
};

/**
 * List all workflows
 */
export const listWorkflows = async (params?: {
  limit?: number;
  offset?: number;
  active_only?: boolean;
}): Promise<{ success: boolean; workflows: Workflow[]; total_count: number }> => {
  const queryParams = new URLSearchParams();
  if (params?.limit) queryParams.append('limit', params.limit.toString());
  if (params?.offset) queryParams.append('offset', params.offset.toString());
  if (params?.active_only !== undefined) queryParams.append('active_only', params.active_only.toString());

  const response = await fetch(`${API_URL}/workflows?${queryParams.toString()}`, {
    headers: getAuthHeaders(),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to list workflows' }));
    throw new Error(error.detail || 'Failed to list workflows');
  }

  return response.json();
};

/**
 * Get workflow details
 */
export const getWorkflow = async (workflowId: string): Promise<{ success: boolean; workflow: Workflow }> => {
  const response = await fetch(`${API_URL}/workflows/${workflowId}`, {
    headers: getAuthHeaders(),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to get workflow' }));
    throw new Error(error.detail || 'Failed to get workflow');
  }

  return response.json();
};

/**
 * Update workflow metadata
 */
export const updateWorkflow = async (
  workflowId: string,
  request: UpdateWorkflowRequest
): Promise<{ success: boolean; workflow: Workflow }> => {
  const response = await fetch(`${API_URL}/workflows/${workflowId}`, {
    method: 'PUT',
    headers: getAuthHeaders(),
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to update workflow' }));
    throw new Error(error.detail || 'Failed to update workflow');
  }

  return response.json();
};

/**
 * Delete a workflow
 */
export const deleteWorkflow = async (workflowId: string): Promise<{ success: boolean; message: string }> => {
  const response = await fetch(`${API_URL}/workflows/${workflowId}`, {
    method: 'DELETE',
    headers: getAuthHeaders(),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to delete workflow' }));
    throw new Error(error.detail || 'Failed to delete workflow');
  }

  return response.json();
};

/**
 * Execute a workflow
 */
export const executeWorkflow = async (
  workflowId: string,
  request: ExecuteWorkflowRequest
): Promise<{
  success: boolean;
  execution_id?: string;
  workflow_id?: string;
  workflow_name?: string;
  status?: string;
  variables_used?: Record<string, unknown>;
  step_results?: WorkflowStepResult[];
  total_execution_time_seconds?: number;
  error?: string;
}> => {
  const response = await fetch(`${API_URL}/workflows/${workflowId}/execute`, {
    method: 'POST',
    headers: getAuthHeaders(),
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to execute workflow' }));
    throw new Error(error.detail || 'Failed to execute workflow');
  }

  return response.json();
};

/**
 * List workflow executions
 */
export const listExecutions = async (
  workflowId: string,
  params?: { limit?: number; offset?: number }
): Promise<{ success: boolean; executions: WorkflowExecution[]; total_count: number }> => {
  const queryParams = new URLSearchParams();
  if (params?.limit) queryParams.append('limit', params.limit.toString());
  if (params?.offset) queryParams.append('offset', params.offset.toString());

  const response = await fetch(`${API_URL}/workflows/${workflowId}/executions?${queryParams.toString()}`, {
    headers: getAuthHeaders(),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to list executions' }));
    throw new Error(error.detail || 'Failed to list executions');
  }

  return response.json();
};

/**
 * Get execution details
 */
export const getExecution = async (executionId: string): Promise<{ success: boolean; execution: WorkflowExecution }> => {
  const response = await fetch(`${API_URL}/workflows/executions/${executionId}`, {
    headers: getAuthHeaders(),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to get execution' }));
    throw new Error(error.detail || 'Failed to get execution');
  }

  return response.json();
};
