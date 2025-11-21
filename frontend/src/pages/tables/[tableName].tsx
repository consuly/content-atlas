import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import axios from 'axios';
import { API_URL } from '../../config';
import {
  App as AntdApp,
  Breadcrumb,
  Button,
  Card,
  Empty,
  Input,
  Result,
  Select,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from 'antd';
import type { BreadcrumbProps } from 'antd';
import type { TablePaginationConfig } from 'antd/es/table';
import type { FilterValue, SortOrder, SorterResult } from 'antd/es/table/interface';
import {
  ArrowLeftOutlined,
  FilterOutlined,
  ReloadOutlined,
  SearchOutlined,
  TableOutlined,
} from '@ant-design/icons';

const { Title, Text } = Typography;
const { Search } = Input;
const DEFAULT_PAGE_SIZE = 25;
const DEFAULT_VISIBLE_COLUMNS = 20;

type FilterOperator = 'eq' | 'contains';

interface ColumnDefinition {
  name: string;
  type: string;
  nullable: boolean;
}

interface FilterCondition {
  column: string;
  operator: FilterOperator;
  value: string;
}

interface TableRow extends Record<string, unknown> {
  __rowKey: string;
}

interface TableStats {
  total_rows: number;
  columns_count: number;
}

const operatorOptions = [
  { label: 'Equals', value: 'eq' },
  { label: 'Contains', value: 'contains' },
];

export const TableViewerPage: React.FC = () => {
  const { tableName } = useParams<{ tableName: string }>();
  const navigate = useNavigate();
  const { message } = AntdApp.useApp();

  const [columns, setColumns] = useState<ColumnDefinition[]>([]);
  const [rows, setRows] = useState<TableRow[]>([]);
  const [tableStats, setTableStats] = useState<TableStats | null>(null);
  const [loadingTable, setLoadingTable] = useState(false);
  const [loadingSchema, setLoadingSchema] = useState(false);
  const [pagination, setPagination] = useState<{ current: number; pageSize: number; total: number }>({
    current: 1,
    pageSize: DEFAULT_PAGE_SIZE,
    total: 0,
  });
  const [sorter, setSorter] = useState<{ field?: string; order?: SortOrder }>({});
  const [searchValue, setSearchValue] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [filters, setFilters] = useState<FilterCondition[]>([]);
  const [filterDraft, setFilterDraft] = useState<Partial<FilterCondition>>({ operator: 'eq' });
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);

  const encodedTableName = useMemo(() => {
    if (!tableName) return '';
    return encodeURIComponent(tableName);
  }, [tableName]);

  const authHeaders = () => {
    const token = localStorage.getItem('refine-auth');
    return token ? { Authorization: `Bearer ${token}` } : {};
  };

  const fetchSchema = useCallback(async () => {
    if (!tableName) return;
    setLoadingSchema(true);
    try {
      const response = await axios.get(`${API_URL}/tables/${encodedTableName}/schema`, {
        headers: authHeaders(),
      });

      if (response.data.success) {
        const nextColumns: ColumnDefinition[] = response.data.columns || [];
        setColumns(nextColumns);
        // Limit initial render cost by only showing a subset of columns; users can expand as needed.
        setVisibleColumns((prev) =>
          prev.length ? prev : nextColumns.slice(0, DEFAULT_VISIBLE_COLUMNS).map((col) => col.name),
        );
      }
    } catch (err) {
      console.error('Failed to load table schema', err);
      message.error('Failed to load table schema.');
    } finally {
      setLoadingSchema(false);
    }
  }, [encodedTableName, tableName, message]);

  const fetchStats = useCallback(async () => {
    if (!tableName) return;
    try {
      const response = await axios.get(`${API_URL}/tables/${encodedTableName}/stats`, {
        headers: authHeaders(),
      });

      if (response.data.success) {
        setTableStats({
          total_rows: response.data.total_rows,
          columns_count: response.data.columns_count,
        });
      }
    } catch (err) {
      // stats are optional, so just log in console
      console.warn('Failed to load table stats', err);
    }
  }, [encodedTableName, tableName]);

  const currentPage = pagination.current;
  const pageSize = pagination.pageSize;

  const fetchRows = useCallback(async () => {
    if (!tableName) return;

    setLoadingTable(true);
    try {
      const params: Record<string, unknown> = {
        limit: pageSize,
        offset: (currentPage - 1) * pageSize,
      };

      if (sorter.field) {
        params.sort_by = sorter.field;
        params.sort_order = sorter.order === 'descend' ? 'desc' : 'asc';
      }

      if (searchValue.trim()) {
        params.search = searchValue.trim();
      }

      if (filters.length > 0) {
        params.filters = JSON.stringify(filters);
      }

      const response = await axios.get(`${API_URL}/tables/${encodedTableName}`, {
        params,
        headers: authHeaders(),
      });

      if (response.data.success) {
        const rawData = response.data.data as Record<string, unknown>[];
        const enrichedRows: TableRow[] = rawData.map((row, index) => {
          const candidate =
            (row.id ?? row.ID ?? row.Id ?? row.uuid ?? row.UUID) as string | number | undefined;
          const fallback = `${tableName}-${currentPage}-${index}`;

          return {
            __rowKey: candidate ? String(candidate) : fallback,
            ...row,
          };
        });

        setRows(enrichedRows);
        setPagination((prev) => {
          const nextTotal = response.data.total_rows;
          // Avoid rerenders/fetch loops when the total hasn't changed
          if (prev.total === nextTotal) {
            return prev;
          }
          return { ...prev, total: nextTotal };
        });
      }
    } catch (err) {
      console.error('Failed to load table data', err);
      message.error('Failed to load table data.');
    } finally {
      setLoadingTable(false);
    }
  }, [
    tableName,
    encodedTableName,
    currentPage,
    pageSize,
    sorter.field,
    sorter.order,
    searchValue,
    filters,
    message,
  ]);

  useEffect(() => {
    fetchSchema();
    fetchStats();
  }, [fetchSchema, fetchStats]);

  useEffect(() => {
    fetchRows();
  }, [fetchRows]);

  const handleTableChange = (
    nextPagination: TablePaginationConfig,
    _filters: Record<string, FilterValue | null>,
    sorterInfo: SorterResult<TableRow> | SorterResult<TableRow>[],
  ) => {
    const currentSorter = Array.isArray(sorterInfo) ? sorterInfo[0] : sorterInfo;
    setPagination((prev) => ({
      ...prev,
      current: nextPagination.current || 1,
      pageSize: nextPagination.pageSize || prev.pageSize,
    }));
    setSorter({
      field: (currentSorter?.field as string) || undefined,
      order: currentSorter?.order,
    });
  };

  const handleSearch = (value: string) => {
    setPagination((prev) => ({ ...prev, current: 1 }));
    setSearchValue(value.trim());
  };

  const handleSearchInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setSearchInput(value);
    if (!value) {
      setSearchValue('');
      setPagination((prev) => ({ ...prev, current: 1 }));
    }
  };

  const handleAddFilter = () => {
    if (!filterDraft.column || !filterDraft.value?.trim()) {
      return;
    }

    const newFilter: FilterCondition = {
      column: filterDraft.column,
      operator: (filterDraft.operator as FilterOperator) || 'eq',
      value: filterDraft.value.trim(),
    };

    setFilters((prev) => [...prev, newFilter]);
    setFilterDraft((prev) => ({
      column: prev?.column,
      operator: 'eq',
      value: '',
    }));
    setPagination((prev) => ({ ...prev, current: 1 }));
  };

  const handleRemoveFilter = (index: number) => {
    setFilters((prev) => prev.filter((_, idx) => idx !== index));
    setPagination((prev) => ({ ...prev, current: 1 }));
  };

  const clearAllFilters = () => {
    setFilters([]);
    setPagination((prev) => ({ ...prev, current: 1 }));
  };

  const activeColumns = columns
    .filter((column) => visibleColumns.length === 0 || visibleColumns.includes(column.name))
    .map((column) => ({
      title: column.name,
      dataIndex: column.name,
      key: column.name,
      width: 200,
      sorter: true,
      sortOrder: sorter.field === column.name ? sorter.order : null,
      ellipsis: true,
      render: (value: unknown) => {
        if (value === null || value === undefined || value === '') {
          return <Text type="secondary">—</Text>;
        }
        if (typeof value === 'object') {
          try {
            return JSON.stringify(value);
          } catch {
            return String(value);
          }
        }
        return String(value);
      },
    }));

  const breadcrumbItems: BreadcrumbProps['items'] = [
    {
      key: 'import',
      title: (
        <span style={{ cursor: 'pointer' }} onClick={() => navigate('/import')}>
          Import
        </span>
      ),
    },
    {
      key: 'table',
      title: tableName || 'Table',
    },
  ];

  if (!tableName) {
    return (
      <Result
        status="404"
        title="Missing table name"
        subTitle="Please provide a table name to view its rows."
        extra={
          <Button type="primary" onClick={() => navigate('/import')}>
            Back to Imports
          </Button>
        }
      />
    );
  }

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Breadcrumb items={breadcrumbItems} />

      <Card
        title={
          <Space>
            <TableOutlined />
            <span>{tableName}</span>
          </Space>
        }
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={fetchRows}>
              Refresh
            </Button>
            <Button icon={<ArrowLeftOutlined />} onClick={() => navigate(-1)}>
              Back
            </Button>
          </Space>
        }
      >
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Space direction="vertical" size={4}>
            <Title level={5} style={{ marginBottom: 0 }}>
              Table Overview
            </Title>
            <Text type="secondary">
              Use the controls below to search, filter, and sort {tableStats?.total_rows?.toLocaleString() ?? '0'}{' '}
              rows across {tableStats?.columns_count ?? columns.length} columns.
            </Text>
        </Space>

          <Space wrap style={{ width: '100%' }} size="large">
            <Search
              value={searchInput}
              onChange={handleSearchInputChange}
              onSearch={handleSearch}
              enterButton={<SearchOutlined />}
              allowClear
              placeholder="Search across all columns"
              style={{ maxWidth: 320 }}
            />

            <Space>
              <Select
                placeholder="Column"
                style={{ width: 180 }}
                value={filterDraft.column}
                onChange={(value) => setFilterDraft((prev) => ({ ...prev, column: value }))}
                options={columns.map((col) => ({ label: col.name, value: col.name }))}
                disabled={loadingSchema || columns.length === 0}
              />
              <Select
                style={{ width: 140 }}
                value={filterDraft.operator}
                onChange={(value) => setFilterDraft((prev) => ({ ...prev, operator: value as FilterOperator }))}
                options={operatorOptions}
                disabled={!columns.length}
              />
              <Input
                placeholder="Value"
                style={{ width: 200 }}
                value={filterDraft.value ?? ''}
                onChange={(event) => setFilterDraft((prev) => ({ ...prev, value: event.target.value }))}
                onPressEnter={handleAddFilter}
                disabled={!filterDraft.column}
              />
              <Button
                type="primary"
                icon={<FilterOutlined />}
                onClick={handleAddFilter}
                disabled={!filterDraft.column || !filterDraft.value}
              >
                Add Filter
              </Button>
            </Space>
          </Space>

          <Space wrap>
            <Select
              mode="multiple"
              allowClear
              style={{ minWidth: 260, maxWidth: 520 }}
              placeholder="Select columns to display"
              value={visibleColumns}
              onChange={(value) => setVisibleColumns(value)}
              options={columns.map((col) => ({ label: col.name, value: col.name }))}
              maxTagCount="responsive"
            />
            <Button
              type="link"
              onClick={() => setVisibleColumns(columns.map((col) => col.name))}
              disabled={visibleColumns.length === columns.length}
            >
              Show all columns
            </Button>
            <Button
              type="link"
              onClick={() =>
                setVisibleColumns(columns.slice(0, DEFAULT_VISIBLE_COLUMNS).map((col) => col.name))
              }
              disabled={
                visibleColumns.length === DEFAULT_VISIBLE_COLUMNS &&
                visibleColumns.every((name, idx) => columns[idx]?.name === name)
              }
            >
              Show first {DEFAULT_VISIBLE_COLUMNS}
            </Button>
          </Space>

          {filters.length > 0 && (
            <Space wrap>
              {filters.map((filter, index) => (
                <Tag key={`${filter.column}-${index}`} closable onClose={() => handleRemoveFilter(index)}>
                  {filter.column} {filter.operator === 'eq' ? '=' : 'contains'} "{filter.value}"
                </Tag>
              ))}
              <Button type="link" onClick={clearAllFilters}>
                Clear Filters
              </Button>
            </Space>
          )}
        </Space>
      </Card>

      <Card>
        {loadingSchema ? (
          <div style={{ textAlign: 'center', padding: '40px 0' }}>
            <Spin />
          </div>
        ) : columns.length === 0 ? (
          <Empty description="No user columns found for this table." />
        ) : (
          <Table
            bordered
            size="small"
            scroll={{ x: 'max-content' }}
            rowKey="__rowKey"
            columns={activeColumns}
            dataSource={rows}
            loading={loadingTable}
            pagination={{
              current: pagination.current,
              pageSize: pagination.pageSize,
              total: pagination.total,
              showSizeChanger: true,
              pageSizeOptions: ['25', '50', '100', '250'],
            }}
            onChange={handleTableChange}
          />
        )}
      </Card>
    </Space>
  );
};

export default TableViewerPage;
