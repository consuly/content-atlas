/**
 * Error Boundary component to catch rendering errors and prevent white screen crashes
 */

import React, { Component, ErrorInfo, ReactNode } from 'react';
import { Result, Button, Typography, Card } from 'antd';
import { BugOutlined, ReloadOutlined } from '@ant-design/icons';

const { Paragraph, Text } = Typography;

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null,
    };
  }

  static getDerivedStateFromError(error: Error): State {
    // Update state so the next render will show the fallback UI
    return {
      hasError: true,
      error,
      errorInfo: null,
    };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    // Log error details for debugging
    console.error('Error Boundary caught an error:', error, errorInfo);
    
    this.setState({
      error,
      errorInfo,
    });
  }

  handleReload = () => {
    // Clear error state and reload the page
    window.location.reload();
  };

  handleReset = () => {
    // Clear error state without reloading
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null,
    });
  };

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ 
          padding: '48px 24px', 
          minHeight: '100vh', 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'center',
          backgroundColor: '#f0f2f5'
        }}>
          <Card style={{ maxWidth: 800, width: '100%' }}>
            <Result
              status="error"
              icon={<BugOutlined />}
              title="Something Went Wrong"
              subTitle="The application encountered an unexpected error. This has been logged for investigation."
              extra={[
                <Button 
                  type="primary" 
                  key="reload" 
                  icon={<ReloadOutlined />}
                  onClick={this.handleReload}
                >
                  Reload Page
                </Button>,
                <Button key="reset" onClick={this.handleReset}>
                  Try Again
                </Button>,
              ]}
            >
              <div style={{ textAlign: 'left' }}>
                <Paragraph>
                  <Text strong>What you can do:</Text>
                </Paragraph>
                <ul>
                  <li>Click "Reload Page" to refresh the application</li>
                  <li>Check your internet connection</li>
                  <li>Clear your browser cache and try again</li>
                  <li>If the problem persists, contact support</li>
                </ul>

                {this.state.error && (
                  <details style={{ marginTop: 24 }}>
                    <summary style={{ cursor: 'pointer', marginBottom: 8 }}>
                      <Text strong>Technical Details (for developers)</Text>
                    </summary>
                    <Card 
                      size="small" 
                      style={{ 
                        backgroundColor: '#f5f5f5',
                        marginTop: 8
                      }}
                    >
                      <Paragraph>
                        <Text strong>Error:</Text>
                        <br />
                        <Text code>{this.state.error.toString()}</Text>
                      </Paragraph>
                      {this.state.errorInfo && (
                        <Paragraph>
                          <Text strong>Component Stack:</Text>
                          <pre style={{ 
                            fontSize: 12, 
                            overflow: 'auto',
                            backgroundColor: '#fff',
                            padding: 12,
                            borderRadius: 4,
                            border: '1px solid #d9d9d9'
                          }}>
                            {this.state.errorInfo.componentStack}
                          </pre>
                        </Paragraph>
                      )}
                    </Card>
                  </details>
                )}
              </div>
            </Result>
          </Card>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
