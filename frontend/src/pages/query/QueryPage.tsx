/**
 * Main Query Database page with LLM-powered natural language queries
 */

import React, { useState, useEffect, useRef } from 'react';
import { Card, Button, Space, Typography, Alert, Spin, Empty, App as AntdApp, List, Skeleton } from 'antd';
import { 
  PlusOutlined, 
  RobotOutlined, 
  QuestionCircleOutlined, 
  HistoryOutlined
} from '@ant-design/icons';
import { QueryInput } from './QueryInput';
import { MessageDisplay } from './MessageDisplay';
import { QueryConversationMessage, QueryConversationSummary, QueryMessage } from './types';
import { fetchConversations, fetchLatestConversation, fetchConversationByThreadId, queryDatabase } from '../../api/query';

// Simple UUID generator (alternative to uuid package)
const generateUuid = () => {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
};

const { Title, Text, Paragraph } = Typography;

export const QueryPage: React.FC = () => {
  const [threadId, setThreadId] = useState<string>('');
  const [messages, setMessages] = useState<QueryMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isHydrating, setIsHydrating] = useState(true);
  const [isLoadingConversationList, setIsLoadingConversationList] = useState(false);
  const [conversations, setConversations] = useState<QueryConversationSummary[]>([]);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const { message: messageApi } = AntdApp.useApp();
  const isBusy = isLoading || isHydrating;

  const convertConversationMessages = (items: QueryConversationMessage[]) => {
    return items.map((msg) => ({
      id: generateUuid(),
      type: msg.role === 'assistant' ? 'assistant' : 'user',
      content: msg.content,
      timestamp: msg.timestamp ? new Date(msg.timestamp) : new Date(),
      executedSql: msg.executed_sql,
      dataCsv: msg.data_csv,
      executionTime: msg.execution_time_seconds,
      rowsReturned: msg.rows_returned,
      chartSuggestion: msg.chart_suggestion,
      error: msg.error,
    } as QueryMessage));
  };

  const loadConversationList = async () => {
    setIsLoadingConversationList(true);
    try {
      const response = await fetchConversations();
      if (response.success && response.conversations.length > 0) {
        setConversations(response.conversations);
      }
    } catch (err) {
      console.error('Failed to list conversations', err);
      messageApi.error('Unable to load conversations list.');
    } finally {
      setIsLoadingConversationList(false);
    }
  };

  const hydrateLatestConversation = async () => {
    setIsHydrating(true);
    setError(null);

    try {
      const response = await fetchLatestConversation();
      if (response.success && response.conversation) {
        setThreadId(response.conversation.thread_id);
        setMessages(convertConversationMessages(response.conversation.messages));
        await loadConversationList();
        return;
      }
    } catch (err) {
      console.error('Failed to load latest conversation', err);
      messageApi.warning('Could not load the latest conversation from the database.');
    } finally {
      setIsHydrating(false);
    }

    const newThreadId = generateUuid();
    setThreadId(newThreadId);
    setMessages([]);
  };

  // Initialize conversation on mount
  useEffect(() => {
    hydrateLatestConversation();
    loadConversationList();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleNewConversation = () => {
    const newThreadId = generateUuid();
    setThreadId(newThreadId);
    setMessages([]);
    setError(null);
    loadConversationList();
  };

  const handleLoadConversation = async (id: string) => {
    if (!id) return;
    setIsHydrating(true);
    setError(null);

    try {
      const response = await fetchConversationByThreadId(id);
      if (response.success && response.conversation) {
        setThreadId(response.conversation.thread_id);
        setMessages(convertConversationMessages(response.conversation.messages));
      } else {
        setMessages([]);
        setThreadId(id);
        messageApi.info('No messages found for this conversation.');
      }
    } catch (err) {
      console.error('Failed to load conversation', err);
      messageApi.error('Could not load that conversation.');
    } finally {
      setIsHydrating(false);
    }
  };

  const handleSendQuery = async (query: string) => {
    if (!query.trim() || isLoading) return;

    const activeThreadId = threadId || generateUuid();
    if (!threadId) {
      setThreadId(activeThreadId);
    }

    // Ensure sidebar shows this conversation even if list API is unavailable
    setConversations((prev) => {
      const exists = prev.some((c) => c.thread_id === activeThreadId);
      if (exists) return prev;
      return [
        {
          thread_id: activeThreadId,
          updated_at: new Date().toISOString(),
          created_at: new Date().toISOString(),
          first_user_prompt: query,
        },
        ...prev,
      ];
    });

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
      const response = await queryDatabase(query, activeThreadId);

      if (response.thread_id && response.thread_id !== threadId) {
        setThreadId(response.thread_id);
      }

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
        chartSuggestion: response.chart_suggestion,
        error: response.error,
      };

      setMessages((prev) => [...prev, assistantMessage]);
      loadConversationList();
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
        styles={{ body: { padding: '16px 24px' } }}
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
          <Space>
            <Button
              icon={<HistoryOutlined />}
              onClick={hydrateLatestConversation}
              loading={isHydrating}
              disabled={isLoading}
            >
              Load Latest
            </Button>
            <Button
              icon={<PlusOutlined />}
              onClick={handleNewConversation}
              disabled={isBusy}
            >
              New Conversation
            </Button>
          </Space>
        </div>
      </Card>

      <div style={{ display: 'flex', flex: 1, gap: 16, overflow: 'hidden' }}>
        {/* Sidebar Conversations */}
          <Card
            title="Conversations"
            style={{ width: 320, flexShrink: 0, overflow: 'hidden' }}
            styles={{ body: { padding: 0, height: '100%', overflow: 'auto' } }}
            extra={<Button size="small" onClick={loadConversationList} loading={isLoadingConversationList}>Refresh</Button>}
        >
          <List
            loading={isLoadingConversationList}
            dataSource={conversations}
            renderItem={(conv) => (
              <List.Item
                key={conv.thread_id}
                style={{ cursor: 'pointer', padding: '12px 16px' }}
                onClick={() => handleLoadConversation(conv.thread_id)}
              >
                <List.Item.Meta
                  title={
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Text strong ellipsis style={{ maxWidth: 200 }}>
                        {conv.first_user_prompt || 'Conversation'}
                      </Text>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {conv.updated_at ? new Date(conv.updated_at).toLocaleString() : ''}
                      </Text>
                    </div>
                  }
                  description={
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      ID: {conv.thread_id}
                    </Text>
                  }
                />
              </List.Item>
            )}
            locale={{ emptyText: isLoadingConversationList ? <Skeleton active paragraph={false} /> : 'No conversations yet' }}
          />
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
          styles={{ body: { padding: '24px', overflow: 'auto', flex: 1 } }}
        >
          {isHydrating ? (
            <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
              <Space>
                <Spin size="large" />
                <Text type="secondary">Loading conversation...</Text>
              </Space>
            </div>
          ) : messages.length === 0 ? (
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
                          disabled={isBusy}
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
      </div>

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
      <Card styles={{ body: { padding: '16px' } }}>
        <QueryInput
          onSend={handleSendQuery}
          disabled={isBusy}
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
