/**
 * S3-compatible storage upload utility for large file uploads
 * Uploads files directly from browser to S3-compatible storage (B2, AWS S3, MinIO, etc.)
 * using pre-signed URLs, bypassing the backend
 */

interface UploadAuthorization {
  upload_url: string;
  file_path: string;
  method: string;
  expires_in: number;
  content_type?: string;
}

interface UploadResult {
  fileId: string;
  fileName: string;
  filePath: string;
}

/**
 * Upload a file directly to S3-compatible storage using pre-signed URL
 */
export async function uploadToStorageDirect(
  file: File,
  uploadAuth: UploadAuthorization,
  onProgress?: (progress: number) => void
): Promise<UploadResult> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    
    // Track upload progress
    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable && onProgress) {
        const percentComplete = (e.loaded / e.total) * 100;
        onProgress(percentComplete);
      }
    });
    
    xhr.addEventListener('load', () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        // Extract ETag from response headers (this is the file ID)
        const etag = xhr.getResponseHeader('ETag')?.replace(/"/g, '') || '';
        
        resolve({
          fileId: etag,
          fileName: file.name,
          filePath: uploadAuth.file_path,
        });
      } else {
        reject(new Error(`Storage upload failed: ${xhr.status} ${xhr.responseText}`));
      }
    });
    
    xhr.addEventListener('error', () => {
      reject(new Error('Network error during upload'));
    });
    
    xhr.addEventListener('abort', () => {
      reject(new Error('Upload aborted'));
    });
    
    // Open connection with the pre-signed URL
    xhr.open(uploadAuth.method, uploadAuth.upload_url);
    
    // DO NOT set Content-Type header - let the browser set it automatically
    // Setting custom headers triggers CORS preflight, and B2's CORS may not allow it
    
    // Send the file
    xhr.send(file);
  });
}

/**
 * Calculate SHA-256 hash of file content
 * Used for duplicate detection
 */
export async function calculateFileSHA256(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  return hashHex;
}
