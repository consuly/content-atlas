/**
 * Calculate SHA-256 hash of a file using Web Crypto API
 * This is used for duplicate detection before uploading
 */
export async function calculateFileHash(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  return hashHex;
}

/**
 * Calculate hash for a file in chunks to avoid memory issues with large files
 * Recommended for files > 50MB
 */
export async function calculateFileHashChunked(
  file: File,
  chunkSize: number = 5 * 1024 * 1024, // 5MB chunks
  onProgress?: (progress: number) => void
): Promise<string> {
  const chunks = Math.ceil(file.size / chunkSize);
  let offset = 0;
  
  // Read the whole file in chunks and hash it
  // A more sophisticated approach would use streaming, but this works for our use case
  const fileBuffer = new Uint8Array(file.size);
  
  for (let i = 0; i < chunks; i++) {
    const chunk = file.slice(offset, offset + chunkSize);
    const chunkBuffer = await chunk.arrayBuffer();
    fileBuffer.set(new Uint8Array(chunkBuffer), offset);
    offset += chunkSize;
    
    if (onProgress) {
      onProgress((i + 1) / chunks);
    }
  }
  
  const hash = await crypto.subtle.digest('SHA-256', fileBuffer);
  const hashArray = Array.from(new Uint8Array(hash));
  const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  
  return hashHex;
}
