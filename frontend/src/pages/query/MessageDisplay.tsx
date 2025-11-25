/**
 * Component to display individual query messages
 */

import React, { useState } from 'react';
import { Badge, Collapse, Space, Typography } from 'antd';
import { 
  ClockCircleOutlined, 
  DatabaseOutlined, 
  CodeOutlined,
  WarningOutlined
} from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { QueryMessage } from './types';
import { ResultsTable } from './ResultsTable';
import { ChartPreview } from './ChartPreview';

const { Text } = Typography;
const { Panel } = Collapse;

interface MessageDisplayProps {
  message: QueryMessage;
}

export const MessageDisplay: React.FC<MessageDisplayProps> = ({ message }) => {
  const [sqlExpanded, setSqlExpanded] = useState(false);
  const hasChart = message.chartSuggestion?.should_display && message.chartSuggestion.spec;

  if (message.type === 'user') {
    return (
      <div className="flex justify-end mb-4">
        <div className="bg-brand-500 text-white p-4 rounded-2xl rounded-br-none shadow-lg max-w-[80%]">
          <Text style={{ color: 'white' }}>{message.content}</Text>
        </div>
      </div>
    );
  }

  // Assistant message
  return (
    <div className="flex justify-start mb-4">
      <div className="message-bubble-assistant p-5 rounded-2xl rounded-bl-none shadow-lg max-w-[90%] w-full">
        {/* Error message */}
        {message.error && (
          <div className="mb-4">
            <Badge status="error" text={<Text strong>Query Failed</Text>} />
            <div className="mt-2 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg">
              <div className="mb-2 flex items-center gap-2 text-red-600 dark:text-red-400">
                <WarningOutlined />
                <Text strong type="danger">Error Details:</Text>
              </div>
              <Text type="danger" style={{ whiteSpace: 'pre-wrap', display: 'block' }}>
                {message.error}
              </Text>
              {message.error.includes('Network error') && (
                <div className="mt-3 p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-900/50 rounded">
                  <Text strong className="block mb-1 text-yellow-700 dark:text-yellow-400">
                    💡 Troubleshooting Tips:
                  </Text>
                  <ul className="list-disc pl-5 m-0 text-yellow-700 dark:text-yellow-400">
                    <li>Check if the API server is running</li>
                    <li>Verify your internet connection</li>
                    <li>Ensure the API URL is correctly configured</li>
                    <li>Check browser console for additional details</li>
                  </ul>
                </div>
              )}
              {message.error.includes('Authentication failed') && (
                <div className="mt-3 p-3 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-900/50 rounded">
                  <Text className="text-yellow-700 dark:text-yellow-400">
                    💡 Please try logging out and logging back in to refresh your session.
                  </Text>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Success indicator */}
        {!message.error && message.executedSql && (
          <div className="mb-3">
            <Badge 
              status="success" 
              text={<Text type="success">Query executed successfully</Text>} 
            />
          </div>
        )}

        {/* Markdown explanation */}
        <div className={`prose dark:prose-invert max-w-none ${message.executedSql ? 'mb-4' : ''}`}>
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
                    customStyle={{ borderRadius: '0.5rem' }}
                  >
                    {String(children).replace(/\n$/, '')}
                  </SyntaxHighlighter>
                ) : (
                  <code className={`${className} bg-slate-100 dark:bg-slate-700 px-1 py-0.5 rounded text-sm`} {...props}>
                    {children}
                  </code>
                );
              },
            }}
          >
            {message.content}
          </ReactMarkdown>
        </div>

        {hasChart && (
          <div className="mb-4">
            <ChartPreview suggestion={message.chartSuggestion} />
          </div>
        )}

        {/* SQL Query (collapsible) */}
        {message.executedSql && (
          <Collapse
            ghost
            activeKey={sqlExpanded ? ['sql'] : []}
            onChange={(keys) => setSqlExpanded(keys.includes('sql'))}
            className="mb-4 border border-slate-200 dark:border-slate-700 rounded-lg"
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
          <div className="mb-4 overflow-hidden rounded-lg border border-slate-200 dark:border-slate-700">
            <ResultsTable csvData={message.dataCsv} />
          </div>
        )}

        {/* Metadata */}
        {(typeof message.rowsReturned === 'number' || typeof message.executionTime === 'number') && (
          <Space size="large" className="mt-2 pt-3 border-t border-slate-100 dark:border-slate-700 w-full">
            {typeof message.rowsReturned === 'number' && (
              <Space size="small">
                <DatabaseOutlined className="text-brand-500" />
                <Text type="secondary">
                  {message.rowsReturned} {message.rowsReturned === 1 ? 'row' : 'rows'}
                </Text>
              </Space>
            )}
            {typeof message.executionTime === 'number' && (
              <Space size="small">
                <ClockCircleOutlined className="text-green-500" />
                <Text type="secondary">{message.executionTime.toFixed(2)}s</Text>
              </Space>
            )}
          </Space>
        )}
      </div>
    </div>
  );
};
