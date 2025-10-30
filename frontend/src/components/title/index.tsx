import React from "react";
import { TitleProps } from "@refinedev/core";
import { Typography } from "antd";

const { Title: AntdTitle } = Typography;

export const Title: React.FC<TitleProps> = ({ collapsed }) => {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: collapsed ? "center" : "flex-start",
        padding: collapsed ? "12px 0" : "12px 16px",
        height: "64px",
      }}
    >
      {!collapsed && (
        <AntdTitle
          level={4}
          style={{
            margin: 0,
            fontWeight: 600,
          }}
        >
          Content Atlas
        </AntdTitle>
      )}
      {collapsed && (
        <AntdTitle
          level={5}
          style={{
            margin: 0,
            fontWeight: 600,
          }}
        >
          CA
        </AntdTitle>
      )}
    </div>
  );
};
