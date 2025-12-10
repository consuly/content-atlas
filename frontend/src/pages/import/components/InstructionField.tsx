import React from 'react';
import { Space, Select, Input, Checkbox, Button, Typography } from 'antd';
import { InstructionOption } from './types';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

interface InstructionFieldProps {
  llmInstruction: string;
  setLlmInstruction: (value: string) => void;
  instructionTitle: string;
  setInstructionTitle: (value: string) => void;
  saveInstruction: boolean;
  setSaveInstruction: (value: boolean) => void;
  instructionOptions: InstructionOption[];
  selectedInstructionId: string | null;
  setSelectedInstructionId: (value: string | null) => void;
  loadingInstructions: boolean;
  instructionActionLoading: boolean;
  disableActions: boolean;
  onUpdateInstruction: () => void;
  onDeleteInstruction: () => void;
}

export const InstructionField: React.FC<InstructionFieldProps> = ({
  llmInstruction,
  setLlmInstruction,
  instructionTitle,
  setInstructionTitle,
  saveInstruction,
  setSaveInstruction,
  instructionOptions,
  selectedInstructionId,
  setSelectedInstructionId,
  loadingInstructions,
  instructionActionLoading,
  disableActions,
  onUpdateInstruction,
  onDeleteInstruction,
}) => {
  return (
    <div style={{ width: '100%' }}>
      <Text strong>LLM instruction (optional)</Text>
      <Paragraph type="secondary" style={{ marginBottom: 8 }}>
        This note is passed to the AI for every file in this upload (including archives and workbooks).
      </Paragraph>
      <Space direction="vertical" size="small" style={{ width: '100%' }}>
        <Select
          allowClear
          showSearch
          placeholder="Select a saved instruction"
          value={selectedInstructionId || undefined}
          onChange={(value) => {
            setSelectedInstructionId(value || null);
            const selected = instructionOptions.find((option) => option.id === value);
            if (selected) {
              setLlmInstruction(selected.content);
              setInstructionTitle(selected.title);
            }
          }}
          options={instructionOptions.map((option) => ({
            value: option.id,
            label: option.title,
          }))}
          loading={loadingInstructions}
          style={{ width: '100%' }}
        />
        <TextArea
          value={llmInstruction}
          onChange={(e) => {
            setLlmInstruction(e.target.value);
            if (selectedInstructionId) {
              setSelectedInstructionId(null);
            }
          }}
          placeholder="Example: Keep phone numbers as text and do not drop rows when the address is missing."
          autoSize={{ minRows: 2, maxRows: 4 }}
        />
        <Space align="start">
          <Checkbox
            checked={saveInstruction}
            onChange={(e) => setSaveInstruction(e.target.checked)}
          >
            Save this instruction for future imports
          </Checkbox>
          {saveInstruction && (
            <Input
              value={instructionTitle}
              onChange={(e) => setInstructionTitle(e.target.value)}
              placeholder="Instruction name (e.g., Marketing Cleanup Rules)"
              style={{ minWidth: 240 }}
            />
          )}
        </Space>
        {selectedInstructionId && (
          <Space>
            <Button
              size="small"
              onClick={onUpdateInstruction}
              loading={instructionActionLoading}
              disabled={disableActions}
            >
              Update selected
            </Button>
            <Button
              size="small"
              danger
              onClick={onDeleteInstruction}
              loading={instructionActionLoading}
              disabled={disableActions}
            >
              Delete selected
            </Button>
          </Space>
        )}
      </Space>
    </div>
  );
};
