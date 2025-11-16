import { useEffect, useState } from "react";
import { useLogin } from "@refinedev/core";
import { Form, Input, Button, Card, Typography, Alert, Space } from "antd";
import { MailOutlined, LockOutlined } from "@ant-design/icons";
import axios from "axios";
import { API_URL } from "../../config";

const { Title, Text } = Typography;

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
        // Display server error message
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
    <div
      style={{
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        minHeight: "100vh",
        background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
      }}
    >
      <Card
        style={{
          width: "100%",
          maxWidth: 400,
          padding: "24px",
          boxShadow: "0 8px 24px rgba(0, 0, 0, 0.12)",
        }}
      >
        <Space direction="vertical" size="large" style={{ width: "100%" }}>
          {/* Header */}
          <div style={{ textAlign: "center" }}>
            <Title level={2} style={{ marginBottom: 8 }}>
              Content Atlas
            </Title>
            <Text type="secondary">Sign in to your account</Text>
          </div>

          {requiresAdminSetup && (
            <Alert
              type="info"
              message="Create your admin account"
              description="No users exist yet. Please create the first account to continue."
              showIcon
              action={
                <Button type="link" href="/register" style={{ paddingLeft: 0 }}>
                  Go to registration
                </Button>
              }
            />
          )}

          {/* Error Alert */}
          {errorMessage && (
            <Alert
              message={errorMessage}
              type="error"
              showIcon
              closable
              onClose={() => setErrorMessage("")}
            />
          )}

          {/* Login Form */}
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

          {/* Footer */}
          <div style={{ textAlign: "center" }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              Don't have an account?{" "}
              <a href="/register">Create one</a>
            </Text>
          </div>
        </Space>
      </Card>
    </div>
  );
};
