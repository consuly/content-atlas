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
