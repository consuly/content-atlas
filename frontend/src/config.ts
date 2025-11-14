type RuntimeConfig = {
  VITE_API_URL?: string;
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
