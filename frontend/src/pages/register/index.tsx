import { useState } from "react";
import { useRegister } from "@refinedev/core";
import { Alert, Button, Form, Input, Typography } from "antd";
import { LockOutlined, MailOutlined, UserOutlined } from "@ant-design/icons";
import { AuthLayout } from "../auth/AuthLayout";

const { Text } = Typography;

interface RegisterPayload {
  full_name: string;
  email: string;
  password: string;
}

interface RegisterFormValues extends RegisterPayload {
  confirmPassword: string;
}

export const Register = () => {
  const [form] = Form.useForm<RegisterFormValues>();
  const { mutate: register } = useRegister<RegisterPayload>();
  const [errorMessage, setErrorMessage] = useState<string>("");
  const [isLoading, setIsLoading] = useState(false);

  const onFinish = (values: RegisterFormValues) => {
    setErrorMessage("");
    setIsLoading(true);

    const payload: RegisterPayload = {
      full_name: values.full_name,
      email: values.email,
      password: values.password,
    };

    register(payload, {
      onSuccess: () => {
        setIsLoading(false);
      },
      onError: (error) => {
        setIsLoading(false);
        const message =
          error?.message || "Registration failed. Please try again.";
        setErrorMessage(message);
      },
    });
  };

  return (
    <AuthLayout
      formTitle="Create your workspace"
      formSubtitle="Set up your Consuly Content Atlas account to start importing with fidelity."
      footer={
        <div className="flex flex-col gap-2 text-sm text-slate-500 sm:items-center sm:justify-between">
          <span>
            Invite your team from the dashboard once you're in.
          </span>
          <Text>
            Already have an account?{" "}
            <a href="/login" className="text-brand-500">
              Sign in instead
            </a>
          </Text>
        </div>
      }
    >
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
          full_name: "",
          email: "",
          password: "",
          confirmPassword: "",
        }}
      >
        <Form.Item
          name="full_name"
          label="Full name"
          rules={[
            {
              required: true,
              message: "Please enter your name",
            },
          ]}
          validateTrigger={["onBlur", "onChange"]}
        >
          <Input
            prefix={<UserOutlined />}
            placeholder="Alex Rivera"
            size="large"
            autoComplete="name"
          />
        </Form.Item>

        <Form.Item
          name="email"
          label="Work email"
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
            placeholder="you@company.com"
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
              message: "Please enter a password",
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
            placeholder="Create a strong password"
            size="large"
            autoComplete="new-password"
          />
        </Form.Item>

        <Form.Item
          name="confirmPassword"
          label="Confirm password"
          dependencies={["password"]}
          rules={[
            {
              required: true,
              message: "Please confirm your password",
            },
            ({ getFieldValue }) => ({
              validator(_, value) {
                if (!value || getFieldValue("password") === value) {
                  return Promise.resolve();
                }
                return Promise.reject(
                  new Error("Passwords do not match")
                );
              },
            }),
          ]}
          validateTrigger={["onBlur", "onChange"]}
        >
          <Input.Password
            prefix={<LockOutlined />}
            placeholder="Re-enter your password"
            size="large"
            autoComplete="new-password"
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
            Create account
          </Button>
        </Form.Item>
      </Form>
    </AuthLayout>
  );
};
