/**
 * API integration for database queries
 */

import {
  QueryConversationListResponse,
  QueryConversationResponse,
  QueryRequest,
  QueryResponse,
} from '../pages/query/types';
import { API_URL } from '../config';

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

  try {
    const response = await fetch(`${API_URL}/query-database`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token && { Authorization: `Bearer ${token}` }),
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      // Try to parse error response
      let errorMessage = 'Failed to query database';
      let errorDetail = '';

      try {
        const errorData = await response.json();
        errorMessage = errorData.detail || errorData.message || errorMessage;
        errorDetail = errorData.error || '';
      } catch {
        // If response is not JSON, try to get text
        try {
          const errorText = await response.text();
          if (errorText) {
            errorDetail = errorText.substring(0, 200); // Limit error text length
          }
        } catch {
          // Ignore text parsing errors
        }
      }

      // Create detailed error based on status code
      if (response.status === 401) {
        throw new Error('Authentication failed. Please log in again.');
      } else if (response.status === 403) {
        throw new Error('You do not have permission to query the database.');
      } else if (response.status === 404) {
        throw new Error('Query endpoint not found. Please check your API configuration.');
      } else if (response.status === 500) {
        throw new Error(`Server error: ${errorMessage}${errorDetail ? `\n\nDetails: ${errorDetail}` : ''}`);
      } else if (response.status === 503) {
        throw new Error('Service temporarily unavailable. Please try again later.');
      } else {
        throw new Error(`${errorMessage}${errorDetail ? `\n\nDetails: ${errorDetail}` : ''}`);
      }
    }

    return response.json();
  } catch (error) {
    // Handle network errors and other exceptions
    if (error instanceof TypeError) {
      // Network error (fetch failed)
      throw new Error(
        'Network error: Unable to connect to the server. Please check your internet connection and ensure the API server is running.'
      );
    } else if (error instanceof Error) {
      // Re-throw errors we've already formatted
      throw error;
    } else {
      // Unknown error type
      throw new Error('An unexpected error occurred while querying the database.');
    }
  }
};

const getAuthHeaders = (): HeadersInit => {
  const token = localStorage.getItem('refine-auth');
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };

  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  return headers;
};

const parseConversationResponse = async (
  response: Response
): Promise<QueryConversationResponse> => {
  const data = await response.json();
  return data as QueryConversationResponse;
};

export const fetchLatestConversation = async (): Promise<QueryConversationResponse> => {
  const base = API_URL.replace(/\/?$/, '');
  const urls = [
    `${base}/query-conversations/latest`,
    `${base}/api/v1/query-conversations/latest`,
  ];

  for (const url of urls) {
    const response = await fetch(url, { headers: getAuthHeaders() });

    if (response.status === 404) {
      continue;
    }

    if (response.ok) {
      return parseConversationResponse(response);
    }
  }

  throw new Error('Query conversation endpoints not found');
};

export const fetchConversationByThreadId = async (
  threadId: string
): Promise<QueryConversationResponse> => {
  const base = API_URL.replace(/\/?$/, '');
  const urls = [
    `${base}/query-conversations/${threadId}`,
    `${base}/api/v1/query-conversations/${threadId}`,
  ];

  for (const url of urls) {
    const response = await fetch(url, { headers: getAuthHeaders() });

    if (response.status === 404) {
      continue;
    }

    if (response.ok) {
      return parseConversationResponse(response);
    }
  }

  throw new Error('Query conversation endpoints not found');
};

export const fetchConversations = async (
  limit = 50,
  offset = 0
): Promise<QueryConversationListResponse> => {
  const params = new URLSearchParams({ limit: `${limit}`, offset: `${offset}` });
  const base = API_URL.replace(/\/?$/, '');
  const urls = [
    `${base}/query-conversations?${params.toString()}`,
    `${base}/api/v1/query-conversations?${params.toString()}`,
  ];

  for (const url of urls) {
    const response = await fetch(url, { headers: getAuthHeaders() });

    if (response.status === 404) {
      continue;
    }

    if (response.ok) {
      const data = await response.json();
      return data as QueryConversationListResponse;
    }
  }

  throw new Error('Query conversation list endpoint not found');
};
