import React, { useState } from 'react';
import { App as AntdApp, Upload, Modal, Button, Space } from 'antd';
import { ExclamationCircleOutlined } from '@ant-design/icons';
import { 
  FileSpreadsheet, 
  FileJson, 
  FileArchive, 
  CloudUpload,
  CheckCircle2
} from 'lucide-react';
import type { UploadProps, UploadFile } from 'antd';
import axios from 'axios';
import { API_URL, MAX_UPLOAD_SIZE_MB } from '../../config';

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
  maxFileSize = MAX_UPLOAD_SIZE_MB,
  accept = '.csv,.xlsx,.xls,.zip',
  multiple = true,
}) => {
  const [fileList, setFileList] = useState<UploadFile[]>([]);
  const [duplicateFile, setDuplicateFile] = useState<DuplicateFileInfo | null>(null);
  const { message: messageApi } = AntdApp.useApp();

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
    className: "group",
  };

  return (
    <>
      <Dragger {...uploadProps} style={{ padding: '2rem', background: 'transparent', border: 'none' }}>
        <div className="flex flex-col items-center gap-6">
          {/* Header Status */}
          <div className="flex items-center justify-between w-full max-w-2xl border-b border-slate-200 dark:border-slate-700 pb-4 mb-2">
            <div className="flex items-center gap-3">
              <CloudUpload className="text-brand-500 w-6 h-6" />
              <span className="font-mono text-sm text-slate-500 dark:text-slate-400">s3://content-atlas-bucket/</span>
            </div>
            <div className="flex items-center gap-2 bg-green-500/10 text-green-600 dark:text-green-400 px-3 py-1 rounded-full border border-green-500/20 text-xs font-medium">
              <CheckCircle2 size={12} />
              Connected
            </div>
          </div>

          {/* Supported Formats Grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 w-full max-w-2xl mb-4">
            <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-lg border border-slate-200 dark:border-slate-700 flex flex-col items-center gap-2 transition-colors group-hover:border-brand-500/30">
              <FileSpreadsheet className="text-green-500 w-8 h-8" />
              <div className="text-center">
                <div className="text-sm font-semibold text-slate-700 dark:text-slate-200">CSV / TSV</div>
                <div className="text-xs text-slate-500">Auto-detect</div>
              </div>
            </div>
            <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-lg border border-slate-200 dark:border-slate-700 flex flex-col items-center gap-2 transition-colors group-hover:border-brand-500/30">
              <FileSpreadsheet className="text-green-500 w-8 h-8" />
              <div className="text-center">
                <div className="text-sm font-semibold text-slate-700 dark:text-slate-200">Excel</div>
                <div className="text-xs text-slate-500">Multi-sheet</div>
              </div>
            </div>
            <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-lg border border-slate-200 dark:border-slate-700 flex flex-col items-center gap-2 transition-colors group-hover:border-brand-500/30">
              <FileJson className="text-yellow-500 w-8 h-8" />
              <div className="text-center">
                <div className="text-sm font-semibold text-slate-700 dark:text-slate-200">JSON</div>
                <div className="text-xs text-slate-500">Flattening</div>
              </div>
            </div>
            <div className="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-lg border border-slate-200 dark:border-slate-700 flex flex-col items-center gap-2 transition-colors group-hover:border-brand-500/30">
              <FileArchive className="text-purple-500 w-8 h-8" />
              <div className="text-center">
                <div className="text-sm font-semibold text-slate-700 dark:text-slate-200">Archives</div>
                <div className="text-xs text-slate-500">Recursive</div>
              </div>
            </div>
          </div>

          <div className="text-center">
            <p className="text-lg font-medium text-slate-700 dark:text-slate-200 mb-2">
              Click or drag file to this area to upload
            </p>
            <p className="text-sm text-slate-500 dark:text-slate-400">
              Support for CSV/Excel files and ZIP archives. Maximum file size: {maxFileSize}MB.
            </p>
          </div>
        </div>
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
