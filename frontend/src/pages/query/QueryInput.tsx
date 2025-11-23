/**
 * Component for query input with send button
 */

import React, { useState, useRef, useEffect } from 'react';
import { Input, Button, Space } from 'antd';
import { SendOutlined } from '@ant-design/icons';
import type { TextAreaRef } from 'antd/es/input/TextArea';

const { TextArea } = Input;

interface QueryInputProps {
  onSend: (query: string) => void;
  disabled?: boolean;
  placeholder?: string;
}

export const QueryInput: React.FC<QueryInputProps> = ({
  onSend,
  disabled = false,
  placeholder = 'Ask a question about your data...',
}) => {
  const [value, setValue] = useState('');
  const textAreaRef = useRef<TextAreaRef>(null);

  useEffect(() => {
    // Focus input on mount
    textAreaRef.current?.focus();
  }, []);

  const handleSend = () => {
    const trimmedValue = value.trim();
    if (trimmedValue && !disabled) {
      onSend(trimmedValue);
      setValue('');
      // Reset textarea height after submit in case it expanded
      textAreaRef.current?.resizableTextArea?.textArea?.style.setProperty('height', 'auto');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // Send on Enter (without Shift)
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <Space.Compact style={{ width: '100%' }}>
      <TextArea
        ref={textAreaRef}
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        autoSize={{ minRows: 1, maxRows: 6 }}
        disabled={disabled}
        style={{ resize: 'none' }}
      />
      <Button
        type="primary"
        icon={<SendOutlined />}
        onClick={handleSend}
        disabled={disabled || !value.trim()}
        style={{ height: 'auto' }}
      >
        Send
      </Button>
    </Space.Compact>
  );
};
