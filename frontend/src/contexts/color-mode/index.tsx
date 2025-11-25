import { RefineThemes } from "@refinedev/antd";
import { ConfigProvider, theme } from "antd";
import { type PropsWithChildren, useEffect, useState } from "react";
import { ColorModeContext } from "./context";

const getStoredMode = () => {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem("colorMode");
};

const getSystemMode = () => {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function")
    return null;

  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
};

export const ColorModeContextProvider: React.FC<PropsWithChildren> = ({
  children,
}) => {
  const [mode, setMode] = useState(() => {
    return getStoredMode() || getSystemMode() || "light";
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem("colorMode", mode);
  }, [mode]);

  useEffect(() => {
    const root = document.documentElement;
    const isDark = mode === "dark";

    root.classList.toggle("dark", isDark);
    root.style.colorScheme = isDark ? "dark" : "light";
  }, [mode]);

  const setColorMode = () => {
    if (mode === "light") {
      setMode("dark");
    } else {
      setMode("light");
    }
  };

  const { darkAlgorithm, defaultAlgorithm } = theme;

  return (
    <ColorModeContext.Provider
      value={{
        setMode: setColorMode,
        mode,
      }}
    >
      <ConfigProvider
        // you can change the theme colors here. example: ...RefineThemes.Magenta,
        theme={{
          ...RefineThemes.Blue,
          algorithm: mode === "light" ? defaultAlgorithm : darkAlgorithm,
          token: {
            ...RefineThemes.Blue.token,
            colorPrimary: "#0ea5e9",
            fontFamily: "Inter, system-ui, sans-serif",
            ...(mode === "dark"
              ? {
                  colorBgBase: "#0f172a", // Slate 900
                  colorBgContainer: "#0b1220",
                  colorBgElevated: "#0b1220",
                  colorBorder: "#1f2937",
                  colorBorderSecondary: "#263040",
                  colorTextBase: "#e2e8f0",
                  colorBgLayout: "transparent", // Show body grid
                }
              : {
                  colorBgContainer: "#ffffff",
                  colorBgElevated: "#ffffff",
                  colorBorder: "#d7e0eb",
                  colorBorderSecondary: "#e2e8f0",
                  colorTextBase: "#0f172a",
                  colorBgLayout: "transparent", // Show body grid
              }),
          },
          components: {
            Layout: {
              colorBgBody: "transparent",
              colorBgHeader: "transparent",
            },
            Card: {
              colorBgContainer: mode === "dark" ? "#0b1220" : "#ffffff",
              colorBorderSecondary: mode === "dark" ? "#263040" : "#e2e8f0",
            },
          },
        }}
      >
        {children}
      </ConfigProvider>
    </ColorModeContext.Provider>
  );
};
