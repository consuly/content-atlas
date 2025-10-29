import React from 'react';
import { Modal } from 'antd';
import { FileUpload } from '../file-upload';

interface UploadModalProps {
  open: boolean;
  onClose: () => void;
  onUploadSuccess?: () => void;
}

export const UploadModal: React.FC<UploadModalProps> = ({
  open,
  onClose,
  onUploadSuccess,
}) => {
  const handleUploadSuccess = () => {
    onUploadSuccess?.();
    // Keep modal open so user can upload more files if needed
  };

  return (
    <Modal
      title="Upload Documents"
      open={open}
      onCancel={onClose}
      footer={null}
      width={600}
      destroyOnClose
    >
      <FileUpload
        onUploadSuccess={handleUploadSuccess}
        multiple={true}
      />
    </Modal>
  );
};
