export interface ApiKey {
  id: string;
  app_name: string;
  description?: string;
  created_by: string;
  created_at: string;
  last_used_at?: string;
  expires_at?: string;
  is_active: boolean;
  rate_limit_per_minute: number;
  allowed_endpoints?: string[];
  key_preview?: string;
}

export interface CreateKeyRequest {
  app_name: string;
  description?: string;
  rate_limit_per_minute?: number;
  expires_in_days?: number;
  allowed_endpoints?: string[];
}

export interface CreateKeyResponse {
  success: boolean;
  message: string;
  api_key: string;
  key_id: string;
  app_name: string;
  expires_at?: string;
}

export interface UpdateKeyRequest {
  description?: string;
  rate_limit_per_minute?: number;
  expires_at?: string;
  is_active?: boolean;
}
