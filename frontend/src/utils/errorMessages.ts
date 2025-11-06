export interface UserFacingErrorInfo {
  summary: string;
  action: string;
  note?: string;
  technicalDetails?: string;
}

const NETWORK_PATTERNS = [
  'httpsconnectionpool',
  'failed to resolve',
  'nameresolutionerror',
  'getaddrinfo failed',
  'connection error',
  'max retries exceeded',
];

export const formatUserFacingError = (
  rawError?: string | null,
): UserFacingErrorInfo => {
  const trimmedError = (rawError || '').trim();
  const normalizedError = trimmedError.toLowerCase();

  if (
    trimmedError &&
    NETWORK_PATTERNS.some((pattern) => normalizedError.includes(pattern))
  ) {
    return {
      summary:
        "We couldn't reach the file storage service to finish your import.",
      note: 'This type of connection issue is usually temporary.',
      action:
        'Check your internet connection and try the import again in a few minutes. If it keeps failing, contact support so we can investigate.',
      technicalDetails: trimmedError,
    };
  }

  if (trimmedError) {
    return {
      summary: 'Something went wrong while processing the import.',
      action:
        'Try the import again. If the issue persists, contact support with the technical details below.',
      technicalDetails: trimmedError,
    };
  }

  return {
    summary: 'An unexpected error occurred.',
    action:
      'Please try again. If the issue persists, contact support for further help.',
  };
};
