import { useEffect, useState } from "react";
import { useLogin } from "@refinedev/core";
import { Alert, Button, Form, Input, Typography } from "antd";
import { LockOutlined, MailOutlined } from "@ant-design/icons";
import axios from "axios";
import { API_URL } from "../../config";
import { AuthLayout } from "../auth/AuthLayout";

const { Text } = Typography;

interface LoginFormValues {
  email: string;
  password: string;
}

export const Login = () => {
  const [form] = Form.useForm<LoginFormValues>();
  const { mutate: login } = useLogin<LoginFormValues>();
  const [errorMessage, setErrorMessage] = useState<string>("");
  const [isLoading, setIsLoading] = useState(false);
  const [requiresAdminSetup, setRequiresAdminSetup] = useState(false);

  const onFinish = async (values: LoginFormValues) => {
    setErrorMessage("");
    setIsLoading(true);

    login(values, {
      onSuccess: () => {
        setIsLoading(false);
      },
      onError: (error) => {
        setIsLoading(false);
        const message = error?.message || "Login failed. Please try again.";
        setErrorMessage(message);
      },
    });
  };

  useEffect(() => {
    let isMounted = true;

    const checkBootstrapStatus = async () => {
      try {
        const response = await axios.get(`${API_URL}/auth/bootstrap-status`, {
          headers: { "Content-Type": "application/json" },
        });

        if (!isMounted) return;
        setRequiresAdminSetup(!!response?.data?.requires_admin_setup);
      } catch (error) {
        if (!isMounted) return;
        // Leave requiresAdminSetup as-is; login flow will surface errors.
        console.warn("Unable to determine bootstrap status", error);
      }
    };

    checkBootstrapStatus();
    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <AuthLayout
      formTitle="Access your workspace"
      formSubtitle="Sign in to keep imports, mappings, and AI checks in sync."
      footer={
        <div className="flex flex-col gap-2 text-sm text-slate-400 sm:flex-row sm:items-center sm:justify-between">
          <Text type="secondary">
            Need an account?{" "}
            <a href="/register" className="text-brand-500">
              Start a workspace
            </a>
          </Text>
          <a href="/forgot-password" className="text-brand-500">
            Forgot password?
          </a>
        </div>
      }
    >
      {requiresAdminSetup && (
        <Alert
          className="mb-3"
          type="info"
          message="Create your admin account"
          description="No users exist yet. Create the first account to bootstrap your workspace."
          showIcon
          action={
            <Button type="link" href="/register" style={{ paddingLeft: 0 }}>
              Go to registration
            </Button>
          }
        />
      )}

      {errorMessage && (
        <Alert
          className="mb-3"
          message={errorMessage}
          type="error"
          showIcon
          closable
          onClose={() => setErrorMessage("")}
        />
      )}

      <Form
        form={form}
        layout="vertical"
        onFinish={onFinish}
        requiredMark={false}
        initialValues={{
          email: "",
          password: "",
        }}
      >
        <Form.Item
          name="email"
          label="Email"
          rules={[
            {
              required: true,
              message: "Please enter your email address",
            },
            {
              type: "email",
              message: "Please enter a valid email address",
            },
          ]}
          validateTrigger={["onBlur", "onChange"]}
        >
          <Input
            prefix={<MailOutlined />}
            placeholder="your.email@example.com"
            size="large"
            autoComplete="email"
          />
        </Form.Item>

        <Form.Item
          name="password"
          label="Password"
          rules={[
            {
              required: true,
              message: "Please enter your password",
            },
            {
              min: 8,
              message: "Password must be at least 8 characters",
            },
          ]}
          validateTrigger={["onBlur", "onChange"]}
        >
          <Input.Password
            prefix={<LockOutlined />}
            placeholder="Enter your password"
            size="large"
            autoComplete="current-password"
          />
        </Form.Item>

        <Form.Item style={{ marginBottom: 0 }}>
          <Button
            type="primary"
            htmlType="submit"
            size="large"
            block
            loading={isLoading}
          >
            Sign In
          </Button>
        </Form.Item>
      </Form>
    </AuthLayout>
  );
};
