type RuntimeConfig = {
  VITE_API_URL?: string;
  VITE_MAX_UPLOAD_SIZE_MB?: string | number;
  VITE_UPLOAD_MODE?: string;
};

declare global {
  interface Window {
    __CONTENT_ATLAS_RUNTIME_CONFIG__?: RuntimeConfig;
  }
}

const runtimeConfig =
  typeof window !== "undefined"
    ? window.__CONTENT_ATLAS_RUNTIME_CONFIG__
    : undefined;

export const API_URL =
  runtimeConfig?.VITE_API_URL ||
  import.meta.env.VITE_API_URL ||
  "http://localhost:8000";

const resolveNumber = (value?: string | number) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
};

export const MAX_UPLOAD_SIZE_MB =
  resolveNumber(runtimeConfig?.VITE_MAX_UPLOAD_SIZE_MB) ||
  resolveNumber(import.meta.env.VITE_MAX_UPLOAD_SIZE_MB) ||
  100;

export const UPLOAD_MODE =
  runtimeConfig?.VITE_UPLOAD_MODE ||
  import.meta.env.VITE_UPLOAD_MODE ||
  "direct";
