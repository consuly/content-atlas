/**
 * Component to display individual query messages
 */

import React, { useState } from 'react';
import { Card, Badge, Collapse, Space, Typography } from 'antd';
import { 
  ClockCircleOutlined, 
  DatabaseOutlined, 
  CodeOutlined
} from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { QueryMessage } from './types';
import { ResultsTable } from './ResultsTable';

const { Text } = Typography;
const { Panel } = Collapse;

interface MessageDisplayProps {
  message: QueryMessage;
}

export const MessageDisplay: React.FC<MessageDisplayProps> = ({ message }) => {
  const [sqlExpanded, setSqlExpanded] = useState(false);

  if (message.type === 'user') {
    return (
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 16 }}>
        <Card
          size="small"
          style={{
            maxWidth: '70%',
            backgroundColor: '#1890ff',
            color: 'white',
            borderRadius: 8,
          }}
          bodyStyle={{ padding: '12px 16px' }}
        >
          <Text style={{ color: 'white' }}>{message.content}</Text>
        </Card>
      </div>
    );
  }

  // Assistant message
  return (
    <div style={{ display: 'flex', justifyContent: 'flex-start', marginBottom: 16 }}>
      <Card
        size="small"
        style={{
          maxWidth: '85%',
          borderRadius: 8,
        }}
        bodyStyle={{ padding: '16px' }}
      >
        {/* Error message */}
        {message.error && (
          <div style={{ marginBottom: 16 }}>
            <Badge status="error" text="Error" />
            <div style={{ 
              marginTop: 8, 
              padding: 12, 
              backgroundColor: '#fff2f0', 
              borderRadius: 4,
              border: '1px solid #ffccc7'
            }}>
              <Text type="danger">{message.error}</Text>
            </div>
          </div>
        )}

        {/* Success indicator */}
        {!message.error && message.executedSql && (
          <div style={{ marginBottom: 12 }}>
            <Badge 
              status="success" 
              text={<Text type="success">Query executed successfully</Text>} 
            />
          </div>
        )}

        {/* Markdown explanation */}
        <div style={{ marginBottom: message.executedSql ? 16 : 0 }}>
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            components={{
              code({ className, children, ...props }: React.HTMLAttributes<HTMLElement> & { inline?: boolean }) {
                const match = /language-(\w+)/.exec(className || '');
                const isInline = props.inline ?? true;
                return !isInline && match ? (
                  <SyntaxHighlighter
                    style={vscDarkPlus as { [key: string]: React.CSSProperties }}
                    language={match[1]}
                    PreTag="div"
                  >
                    {String(children).replace(/\n$/, '')}
                  </SyntaxHighlighter>
                ) : (
                  <code className={className} {...props}>
                    {children}
                  </code>
                );
              },
            }}
          >
            {message.content}
          </ReactMarkdown>
        </div>

        {/* SQL Query (collapsible) */}
        {message.executedSql && (
          <Collapse
            ghost
            activeKey={sqlExpanded ? ['sql'] : []}
            onChange={(keys) => setSqlExpanded(keys.includes('sql'))}
            style={{ marginBottom: 16 }}
          >
            <Panel
              header={
                <Space>
                  <CodeOutlined />
                  <Text strong>Executed SQL</Text>
                </Space>
              }
              key="sql"
            >
              <SyntaxHighlighter
                language="sql"
                style={vscDarkPlus}
                customStyle={{
                  margin: 0,
                  borderRadius: 4,
                }}
              >
                {message.executedSql}
              </SyntaxHighlighter>
            </Panel>
          </Collapse>
        )}

        {/* Results Table */}
        {message.dataCsv && (
          <div style={{ marginBottom: 16 }}>
            <ResultsTable csvData={message.dataCsv} />
          </div>
        )}

        {/* Metadata */}
        {(message.rowsReturned !== undefined || message.executionTime !== undefined) && (
          <Space size="large" style={{ marginTop: 8 }}>
            {message.rowsReturned !== undefined && (
              <Space size="small">
                <DatabaseOutlined style={{ color: '#1890ff' }} />
                <Text type="secondary">
                  {message.rowsReturned} {message.rowsReturned === 1 ? 'row' : 'rows'}
                </Text>
              </Space>
            )}
            {message.executionTime !== undefined && (
              <Space size="small">
                <ClockCircleOutlined style={{ color: '#52c41a' }} />
                <Text type="secondary">{message.executionTime.toFixed(2)}s</Text>
              </Space>
            )}
          </Space>
        )}
      </Card>
    </div>
  );
};
