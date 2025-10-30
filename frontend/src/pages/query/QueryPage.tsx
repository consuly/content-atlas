/**
 * Main Query Database page with LLM-powered natural language queries
 */

import React, { useState, useEffect, useRef } from 'react';
import { Card, Button, Space, Typography, Alert, Spin, Empty } from 'antd';
import { 
  PlusOutlined, 
  RobotOutlined, 
  QuestionCircleOutlined 
} from '@ant-design/icons';
import { QueryInput } from './QueryInput';
import { MessageDisplay } from './MessageDisplay';
import { QueryMessage } from './types';
import { queryDatabase } from '../../api/query';

// Simple UUID generator (alternative to uuid package)
const generateUuid = () => {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
};

const { Title, Text, Paragraph } = Typography;

// LocalStorage keys
const THREAD_ID_KEY = 'query-thread-id';
const MESSAGES_KEY_PREFIX = 'query-messages-';

export const QueryPage: React.FC = () => {
  const [threadId, setThreadId] = useState<string>('');
  const [messages, setMessages] = useState<QueryMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Initialize or load conversation on mount
  useEffect(() => {
    const savedThreadId = localStorage.getItem(THREAD_ID_KEY);
    
    if (savedThreadId) {
      // Load existing conversation
      setThreadId(savedThreadId);
      const savedMessages = localStorage.getItem(`${MESSAGES_KEY_PREFIX}${savedThreadId}`);
      if (savedMessages) {
        try {
          const parsed = JSON.parse(savedMessages);
          // Convert timestamp strings back to Date objects
          const messagesWithDates = parsed.map((msg: QueryMessage) => ({
            ...msg,
            timestamp: new Date(msg.timestamp),
          }));
          setMessages(messagesWithDates);
        } catch (e) {
          console.error('Failed to parse saved messages:', e);
        }
      }
    } else {
      // Create new conversation
      const newThreadId = generateUuid();
      setThreadId(newThreadId);
      localStorage.setItem(THREAD_ID_KEY, newThreadId);
    }
  }, []);

  // Save messages to localStorage whenever they change
  useEffect(() => {
    if (threadId && messages.length > 0) {
      localStorage.setItem(`${MESSAGES_KEY_PREFIX}${threadId}`, JSON.stringify(messages));
    }
  }, [threadId, messages]);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleNewConversation = () => {
    const newThreadId = generateUuid();
    setThreadId(newThreadId);
    setMessages([]);
    setError(null);
    localStorage.setItem(THREAD_ID_KEY, newThreadId);
    // Optionally clean up old conversation from localStorage
    // localStorage.removeItem(`${MESSAGES_KEY_PREFIX}${threadId}`);
  };

  const handleSendQuery = async (query: string) => {
    if (!query.trim() || isLoading) return;

    // Add user message
    const userMessage: QueryMessage = {
      id: generateUuid(),
      type: 'user',
      content: query,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setIsLoading(true);
    setError(null);

    try {
      // Call API
      const response = await queryDatabase(query, threadId);

      // Add assistant message
      const assistantMessage: QueryMessage = {
        id: generateUuid(),
        type: 'assistant',
        content: response.response,
        timestamp: new Date(),
        executedSql: response.executed_sql,
        dataCsv: response.data_csv,
        executionTime: response.execution_time_seconds,
        rowsReturned: response.rows_returned,
        error: response.error,
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      // Extract error message with fallback
      let errorMessage = 'An unexpected error occurred while processing your query.';
      let errorDetails = '';

      if (err instanceof Error) {
        errorMessage = err.message;
        // Check if error message contains details separator
        if (errorMessage.includes('\n\nDetails: ')) {
          const parts = errorMessage.split('\n\nDetails: ');
          errorMessage = parts[0];
          errorDetails = parts[1] || '';
        }
      } else if (typeof err === 'string') {
        errorMessage = err;
      }

      // Set error in state for the alert banner
      setError(errorMessage);

      // Add error message to conversation with more helpful content
      const errorAssistantMessage: QueryMessage = {
        id: generateUuid(),
        type: 'assistant',
        content: `I encountered an error while processing your query. ${errorMessage.includes('Network error') ? 'This appears to be a connectivity issue.' : 'Please try rephrasing your question or check the error details below.'}`,
        timestamp: new Date(),
        error: errorDetails ? `${errorMessage}\n\nDetails: ${errorDetails}` : errorMessage,
      };

      setMessages((prev) => [...prev, errorAssistantMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const exampleQueries = [
    'Show me all tables in the database',
    'What are the top 10 customers by revenue?',
    'How many records are in each table?',
    'Show me recent data imports',
  ];

  return (
    <div style={{ padding: '24px', height: 'calc(100vh - 64px)', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <Card
        style={{ marginBottom: '16px', flexShrink: 0 }}
        bodyStyle={{ padding: '16px 24px' }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            <RobotOutlined style={{ fontSize: '24px', color: '#1890ff' }} />
            <div>
              <Title level={4} style={{ margin: 0 }}>
                Query Database
              </Title>
              <Text type="secondary">Ask questions in natural language</Text>
            </div>
          </Space>
          <Button
            icon={<PlusOutlined />}
            onClick={handleNewConversation}
            disabled={isLoading}
          >
            New Conversation
          </Button>
        </div>
      </Card>

      {/* Messages Area */}
      <Card
        style={{ 
          flex: 1, 
          marginBottom: '16px', 
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column'
        }}
        bodyStyle={{ 
          padding: '24px', 
          overflow: 'auto',
          flex: 1
        }}
      >
        {messages.length === 0 ? (
          <Empty
            image={<QuestionCircleOutlined style={{ fontSize: 64, color: '#1890ff' }} />}
            description={
              <div>
                <Title level={4}>Welcome to Query Database</Title>
                <Paragraph type="secondary">
                  Ask questions about your data in natural language. I'll help you explore your database,
                  generate SQL queries, and visualize results.
                </Paragraph>
                <div style={{ marginTop: 24 }}>
                  <Text strong>Try these examples:</Text>
                  <div style={{ marginTop: 12 }}>
                    {exampleQueries.map((example, index) => (
                      <Button
                        key={index}
                        type="link"
                        onClick={() => handleSendQuery(example)}
                        disabled={isLoading}
                        style={{ display: 'block', textAlign: 'left', marginBottom: 8 }}
                      >
                        • {example}
                      </Button>
                    ))}
                  </div>
                </div>
              </div>
            }
          />
        ) : (
          <>
            {messages.map((message) => (
              <MessageDisplay key={message.id} message={message} />
            ))}
            {isLoading && (
              <div style={{ display: 'flex', justifyContent: 'flex-start', marginBottom: 16 }}>
                <Card
                  size="small"
                  style={{ maxWidth: '85%', borderRadius: 8 }}
                  bodyStyle={{ padding: '16px' }}
                >
                  <Space>
                    <Spin size="small" />
                    <Text type="secondary">Thinking...</Text>
                  </Space>
                </Card>
              </div>
            )}
            <div ref={messagesEndRef} />
          </>
        )}
      </Card>

      {/* Error Alert */}
      {error && (
        <Alert
          message="Error"
          description={error}
          type="error"
          closable
          onClose={() => setError(null)}
          style={{ marginBottom: '16px' }}
        />
      )}

      {/* Input Area */}
      <Card bodyStyle={{ padding: '16px' }}>
        <QueryInput
          onSend={handleSendQuery}
          disabled={isLoading}
          placeholder="Ask a question about your data... (Press Enter to send, Shift+Enter for new line)"
        />
        <div style={{ marginTop: 8 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>
            💡 Tip: You can ask follow-up questions and I'll remember the context of our conversation.
          </Text>
        </div>
      </Card>
    </div>
  );
};

export default QueryPage;
