import React, { useState } from 'react';
import { App as AntdApp, Upload, Modal, Button, Space } from 'antd';
import { InboxOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import type { UploadProps, UploadFile } from 'antd';
import axios from 'axios';

const { Dragger } = Upload;

interface UploadedFile {
  id: string;
  file_name: string;
  file_size: number;
  status: string;
  upload_date?: string;
}

interface FileUploadProps {
  onUploadSuccess?: (files: UploadedFile[]) => void;
  onUploadError?: (errors: Error[]) => void;
  maxFileSize?: number; // in MB
  accept?: string;
  multiple?: boolean;
}

interface DuplicateFileInfo {
  file: File;
  existingFile: UploadedFile;
}

export const FileUpload: React.FC<FileUploadProps> = ({
  onUploadSuccess,
  onUploadError,
  maxFileSize = 100, // 100MB default
  accept = '.csv,.xlsx,.xls',
  multiple = true,
}) => {
  const [fileList, setFileList] = useState<UploadFile[]>([]);
  const [duplicateFile, setDuplicateFile] = useState<DuplicateFileInfo | null>(null);
  const { message: messageApi } = AntdApp.useApp();

  const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

  const handleDuplicateAction = async (action: 'overwrite' | 'duplicate' | 'skip') => {
    if (!duplicateFile) return;

    if (action === 'skip') {
      messageApi.info(`Skipped uploading ${duplicateFile.file.name}`);
      setDuplicateFile(null);
      return;
    }

    try {
      const formData = new FormData();
      formData.append('file', duplicateFile.file);

      let endpoint = `${API_URL}/upload-to-b2`;
      if (action === 'overwrite') {
        endpoint = `${API_URL}/upload-to-b2/overwrite`;
      } else if (action === 'duplicate') {
        formData.append('allow_duplicate', 'true');
      }

      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(endpoint, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        messageApi.success(`${duplicateFile.file.name} uploaded successfully`);
        onUploadSuccess?.([response.data.files[0]]);
      }
    } catch (error) {
      const err = error as Error;
      messageApi.error(`Failed to upload ${duplicateFile.file.name}: ${err.message}`);
      onUploadError?.([err]);
    } finally {
      setDuplicateFile(null);
    }
  };

  const uploadFile = async (file: File): Promise<boolean> => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('allow_duplicate', 'false');

    try {
      const token = localStorage.getItem('refine-auth');
      const response = await axios.post(`${API_URL}/upload-to-b2`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
          ...(token && { Authorization: `Bearer ${token}` }),
        },
      });

      if (response.data.success) {
        messageApi.success(`${file.name} uploaded successfully`);
        onUploadSuccess?.([response.data.files[0]]);
        return true;
      } else if (response.data.exists) {
        // File already exists, show duplicate modal
        setDuplicateFile({
          file,
          existingFile: response.data.existing_file,
        });
        return false;
      }
      return false;
    } catch (error) {
      const axiosError = error as { response?: { status?: number; data?: { exists?: boolean; existing_file?: UploadedFile } }; message?: string };
      if (axiosError.response?.status === 409 || axiosError.response?.data?.exists) {
        // Duplicate file detected
        setDuplicateFile({
          file,
          existingFile: axiosError.response?.data?.existing_file as UploadedFile,
        });
        return false;
      }
      const err = error as Error;
      messageApi.error(`Failed to upload ${file.name}: ${err.message}`);
      onUploadError?.([err]);
      return false;
    }
  };

  const customRequest: UploadProps['customRequest'] = async (options) => {
    const { file, onSuccess, onError } = options;
    
    try {
      const success = await uploadFile(file as File);
      if (success) {
        onSuccess?.('ok');
      } else {
        onError?.(new Error('Upload failed or duplicate detected'));
      }
    } catch (error) {
      onError?.(error as Error);
    }
  };

  const beforeUpload = (file: File) => {
    const isValidType = accept.split(',').some(type => 
      file.name.toLowerCase().endsWith(type.trim())
    );
    
    if (!isValidType) {
      messageApi.error(`${file.name} is not a supported file type`);
      return Upload.LIST_IGNORE;
    }

    const isValidSize = file.size / 1024 / 1024 < maxFileSize;
    if (!isValidSize) {
      messageApi.error(`${file.name} must be smaller than ${maxFileSize}MB`);
      return Upload.LIST_IGNORE;
    }

    return true;
  };

  const uploadProps: UploadProps = {
    name: 'file',
    multiple,
    fileList,
    customRequest,
    beforeUpload,
    onChange(info) {
      setFileList(info.fileList);
      
      const { status } = info.file;
      if (status === 'done') {
        setFileList(prev => prev.filter(f => f.uid !== info.file.uid));
      } else if (status === 'error') {
        messageApi.error(`${info.file.name} file upload failed.`);
      }
    },
    onDrop(e) {
      console.log('Dropped files', e.dataTransfer.files);
    },
  };

  return (
    <>
      <Dragger {...uploadProps}>
        <p className="ant-upload-drag-icon">
          <InboxOutlined />
        </p>
        <p className="ant-upload-text">Click or drag file to this area to upload</p>
        <p className="ant-upload-hint">
          Support for CSV and Excel files (.csv, .xlsx, .xls). Maximum file size: {maxFileSize}MB.
          {multiple && ' You can upload multiple files at once.'}
        </p>
      </Dragger>

      <Modal
        title={
          <Space>
            <ExclamationCircleOutlined style={{ color: '#faad14' }} />
            <span>File Already Exists</span>
          </Space>
        }
        open={!!duplicateFile}
        onCancel={() => setDuplicateFile(null)}
        footer={[
          <Button key="skip" onClick={() => handleDuplicateAction('skip')}>
            Skip
          </Button>,
          <Button key="duplicate" onClick={() => handleDuplicateAction('duplicate')}>
            Create Duplicate
          </Button>,
          <Button
            key="overwrite"
            type="primary"
            danger
            onClick={() => handleDuplicateAction('overwrite')}
          >
            Overwrite
          </Button>,
        ]}
      >
        <p>
          The file <strong>{duplicateFile?.file.name}</strong> already exists in the system.
        </p>
        <p>What would you like to do?</p>
        <ul>
          <li><strong>Overwrite:</strong> Replace the existing file with the new one</li>
          <li><strong>Create Duplicate:</strong> Upload as a new file with a different ID</li>
          <li><strong>Skip:</strong> Cancel this upload</li>
        </ul>
      </Modal>
    </>
  );
};
