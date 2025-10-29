/**
 * API integration for database queries
 */

import { QueryRequest, QueryResponse } from '../pages/query/types';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const queryDatabase = async (
  prompt: string,
  threadId?: string
): Promise<QueryResponse> => {
  const token = localStorage.getItem('refine-auth');

  const request: QueryRequest = {
    prompt,
    thread_id: threadId,
    max_rows: 1000,
  };

  const response = await fetch(`${API_URL}/query-database`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token && { Authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.detail || 'Failed to query database');
  }

  return response.json();
};
