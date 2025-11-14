import type { AuthProvider } from "@refinedev/core";
import axios from "axios";
import { API_URL } from "./config";

export const TOKEN_KEY = "refine-auth";

export const authProvider: AuthProvider = {
  login: async ({ email, password }) => {
    try {
      const response = await axios.post(`${API_URL}/auth/login`, {
        email,
        password,
      });

      if (response.data.success) {
        const { token, user } = response.data;
        
        // Store JWT token
        localStorage.setItem(TOKEN_KEY, token.access_token);
        
        // Store user info
        localStorage.setItem("user", JSON.stringify(user));

        return {
          success: true,
          redirectTo: "/",
        };
      }

      return {
        success: false,
        error: {
          name: "LoginError",
          message: "Invalid email or password",
        },
      };
    } catch (error) {
      const err = error as { response?: { data?: { detail?: string } } };
      return {
        success: false,
        error: {
          name: "LoginError",
          message: err.response?.data?.detail || "Login failed",
        },
      };
    }
  },

  logout: async () => {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem("user");
    return {
      success: true,
      redirectTo: "/login",
    };
  },

  check: async () => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      try {
        // Verify token is still valid by calling /auth/me
        const response = await axios.get(`${API_URL}/auth/me`, {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        });

        if (response.data) {
          return {
            authenticated: true,
          };
        }
      } catch {
        // Token is invalid or expired
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem("user");
        return {
          authenticated: false,
          redirectTo: "/login",
          logout: true,
        };
      }
    }

    return {
      authenticated: false,
      redirectTo: "/login",
    };
  },

  getPermissions: async () => null,

  getIdentity: async () => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) {
      try {
        const response = await axios.get(`${API_URL}/auth/me`, {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        });

        const user = response.data;
        return {
          id: user.id,
          name: user.full_name || user.email,
          email: user.email,
          avatar: `https://ui-avatars.com/api/?name=${encodeURIComponent(
            user.full_name || user.email
          )}&background=1890ff&color=fff`,
        };
      } catch {
        return null;
      }
    }
    return null;
  },

  onError: async (error) => {
    if (error.response?.status === 401) {
      return {
        logout: true,
        redirectTo: "/login",
        error,
      };
    }

    return { error };
  },

  register: async ({ email, password, full_name }) => {
    try {
      const response = await axios.post(`${API_URL}/auth/register`, {
        email,
        password,
        full_name,
      });

      if (response.data.success) {
        const { token, user } = response.data;
        
        // Store JWT token
        localStorage.setItem(TOKEN_KEY, token.access_token);
        
        // Store user info
        localStorage.setItem("user", JSON.stringify(user));

        return {
          success: true,
          redirectTo: "/",
        };
      }

      return {
        success: false,
        error: {
          name: "RegisterError",
          message: "Registration failed",
        },
      };
    } catch (error) {
      const err = error as { response?: { data?: { detail?: string } } };
      return {
        success: false,
        error: {
          name: "RegisterError",
          message: err.response?.data?.detail || "Registration failed",
        },
      };
    }
  },
};
