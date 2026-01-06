import React from "react";
import { Authenticated, Refine } from "@refinedev/core";
import { RefineKbar, RefineKbarProvider } from "@refinedev/kbar";

import {
  ErrorComponent,
  ThemedLayout,
  ThemedSider,
  useNotificationProvider,
} from "@refinedev/antd";
import "@refinedev/antd/dist/reset.css";

import routerProvider, {
  CatchAllNavigate,
  DocumentTitleHandler,
  NavigateToResource,
  UnsavedChangesNotifier,
} from "@refinedev/react-router";
import { App as AntdApp } from "antd";
import { BrowserRouter, Outlet, Route, Routes } from "react-router";
import { authProvider } from "./authProvider";
import { dataProvider } from "./dataProvider";
import { Header, ErrorBoundary } from "./components";
import { Title } from "./components/title";
import { ColorModeContextProvider } from "./contexts/color-mode";
import { ForgotPassword } from "./pages/forgotPassword";
import { Login } from "./pages/login";
import { Register } from "./pages/register";
import { ImportPage } from "./pages/import";
import { ImportMappingPage } from "./pages/import/[id]";
import { QueryPage } from "./pages/query";
import { ApiKeysPage } from "./pages/api-keys";
import { DataIssuesPage } from "./pages/data-issues";
import { TableViewerPage } from "./pages/tables/[tableName]";
import { TablesListPage } from "./pages/tables";
import { Database, KeyRound, MessageSquareText, UploadCloud } from "lucide-react";
import { ExclamationCircleOutlined } from "@ant-design/icons";

function App() {
  const appContent = (
    <Refine
      dataProvider={dataProvider}
      notificationProvider={useNotificationProvider}
      routerProvider={routerProvider}
      authProvider={authProvider}
      resources={[
        {
          name: "query",
          list: "/query",
          meta: {
            label: "Query Database",
            icon: <MessageSquareText size={20} />,
          },
        },
        {
          name: "import",
          list: "/import",
          meta: {
            label: "Import Data",
            icon: <UploadCloud size={20} />,
          },
        },
        {
          name: "tables",
          list: "/tables",
          meta: {
            label: "Tables",
            icon: <Database size={20} />,
          },
        },
        {
          name: "api-keys",
          list: "/api-keys",
          meta: {
            label: "API Keys",
            icon: <KeyRound size={20} />,
          },
        },
        {
          name: "data-issues",
          list: "/data-issues",
          meta: {
            label: "Data Issues",
            icon: <ExclamationCircleOutlined />,
          },
        },
      ]}
      options={{
        syncWithLocation: true,
        warnWhenUnsavedChanges: true,
        projectId: "DW5oH5-gHYaON-yppWTj",
      }}
    >
      <Routes>
        <Route
          element={
            <Authenticated
              key="authenticated-inner"
              fallback={<CatchAllNavigate to="/login" />}
            >
              <ThemedLayout
                Header={Header}
                Sider={(props) => <ThemedSider {...props} fixed />}
                Title={Title}
              >
                <Outlet />
              </ThemedLayout>
            </Authenticated>
          }
        >
          <Route index element={<NavigateToResource resource="query" />} />
          <Route path="/import">
            <Route index element={<ImportPage />} />
            <Route path=":id" element={<ImportMappingPage />} />
          </Route>
          <Route path="/tables">
            <Route index element={<TablesListPage />} />
            <Route path=":tableName" element={<TableViewerPage />} />
          </Route>
          <Route path="/query">
            <Route
              index
              element={
                <ErrorBoundary>
                  <QueryPage />
                </ErrorBoundary>
              }
            />
          </Route>
          <Route path="/api-keys">
            <Route index element={<ApiKeysPage />} />
          </Route>
          <Route path="/data-issues">
            <Route index element={<DataIssuesPage />} />
          </Route>
          <Route path="*" element={<ErrorComponent />} />
        </Route>
        <Route
          element={
            <Authenticated
              key="authenticated-outer"
              fallback={<Outlet />}
            >
              <NavigateToResource />
            </Authenticated>
          }
        >
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
          <Route path="/forgot-password" element={<ForgotPassword />} />
        </Route>
      </Routes>

      <RefineKbar />
      <UnsavedChangesNotifier />
      <DocumentTitleHandler />
    </Refine>
  );

  return (
    <BrowserRouter>
      <RefineKbarProvider>
        <ColorModeContextProvider>
          <AntdApp>
            {appContent}
          </AntdApp>
        </ColorModeContextProvider>
      </RefineKbarProvider>
    </BrowserRouter>
  );
}

export default App;
