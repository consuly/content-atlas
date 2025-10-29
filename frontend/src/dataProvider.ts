import { DataProvider } from "@refinedev/core";
import axios, { AxiosInstance } from "axios";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Create axios instance with interceptors
const axiosInstance: AxiosInstance = axios.create({
  baseURL: API_URL,
  headers: {
    "Content-Type": "application/json",
  },
});

// Add request interceptor to include auth token
axiosInstance.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem("refine-auth");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Add response interceptor for error handling
axiosInstance.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Token expired or invalid
      localStorage.removeItem("refine-auth");
      window.location.href = "/login";
    }
    return Promise.reject(error);
  }
);

export const dataProvider: DataProvider = {
  getList: async ({ resource, pagination, meta }) => {
    const current = (pagination as { current?: number })?.current ?? 1;
    const pageSize = (pagination as { pageSize?: number })?.pageSize ?? 10;
    
    // Calculate offset for pagination
    const offset = (current - 1) * pageSize;
    
    try {
      const response = await axiosInstance.get(`/${resource}`, {
        params: {
          limit: pageSize,
          offset: offset,
          ...meta,
        },
      });

      // Handle different response formats
      if (resource === "tables") {
        return {
          data: response.data.tables || [],
          total: response.data.tables?.length || 0,
        };
      }

      // Default format for table data
      return {
        data: response.data.data || [],
        total: response.data.total_rows || 0,
      };
    } catch (error) {
      console.error("getList error:", error);
      throw error;
    }
  },

  getOne: async ({ resource, id, meta }) => {
    try {
      const response = await axiosInstance.get(`/${resource}/${id}`, {
        params: meta,
      });

      return {
        data: response.data,
      };
    } catch (error) {
      console.error("getOne error:", error);
      throw error;
    }
  },

  create: async ({ resource, variables, meta }) => {
    try {
      // Handle file uploads
      if (meta?.isFileUpload) {
        const formData = new FormData();
        const vars = variables as Record<string, unknown>;
        Object.keys(vars).forEach((key) => {
          formData.append(key, vars[key] as string | Blob);
        });

        const response = await axiosInstance.post(`/${resource}`, formData, {
          headers: {
            "Content-Type": "multipart/form-data",
          },
        });

        return {
          data: response.data,
        };
      }

      // Regular JSON request
      const response = await axiosInstance.post(`/${resource}`, variables);

      return {
        data: response.data,
      };
    } catch (error) {
      console.error("create error:", error);
      throw error;
    }
  },

  update: async ({ resource, id, variables }) => {
    try {
      const response = await axiosInstance.put(
        `/${resource}/${id}`,
        variables
      );

      return {
        data: response.data,
      };
    } catch (error) {
      console.error("update error:", error);
      throw error;
    }
  },

  deleteOne: async ({ resource, id }) => {
    try {
      const response = await axiosInstance.delete(`/${resource}/${id}`);

      return {
        data: response.data,
      };
    } catch (error) {
      console.error("deleteOne error:", error);
      throw error;
    }
  },

  getApiUrl: () => API_URL,

  custom: async ({ url, method, payload, query, headers }) => {
    try {
      const response = await axiosInstance({
        url,
        method,
        data: payload,
        params: query,
        headers,
      });

      return {
        data: response.data,
      };
    } catch (error) {
      console.error("custom error:", error);
      throw error;
    }
  },
};

export default dataProvider;
