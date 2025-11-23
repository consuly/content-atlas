import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router';
import axios from 'axios';
import { API_URL } from '../../config';
import {
  App as AntdApp,
  Button,
  Card,
  Col,
  Empty,
  Input,
  InputNumber,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type {
  FilterValue,
  SortOrder,
  SorterResult,
  TableCurrentDataSource,
  TablePaginationConfig,
} from 'antd/es/table/interface';
import { ArrowRightOutlined, DownloadOutlined, ReloadOutlined, SearchOutlined, TableOutlined } from '@ant-design/icons';

const { Title, Text, Link } = Typography;
const { Search } = Input;

type TableInfo = {
  table_name: string;
  row_count: number;
};

type RowStateFilter = 'all' | 'non-empty' | 'empty-only';

type LocalSorter = {
  field?: string;
  order?: SortOrder;
};

const rowStateOptions = [
  { label: 'All tables', value: 'all' },
  { label: 'Has rows', value: 'non-empty' },
  { label: 'Empty only', value: 'empty-only' },
];

export const TablesListPage: React.FC = () => {
  const navigate = useNavigate();
  const { message } = AntdApp.useApp();
  const messageRef = useRef(message);
  messageRef.current = message;

  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [minRows, setMinRows] = useState<number | undefined>();
  const [rowState, setRowState] = useState<RowStateFilter>('all');
  const [sorter, setSorter] = useState<LocalSorter>({ field: 'table_name', order: 'ascend' });
  const [downloadingTable, setDownloadingTable] = useState<string | null>(null);

  const authHeaders = useCallback(() => {
    const token = localStorage.getItem('refine-auth');
    return token ? { Authorization: `Bearer ${token}` } : {};
  }, []);

  const downloadTable = useCallback(
    async (tableName: string) => {
      const safeName = `${tableName.replace(/[^\w.-]+/g, '_') || 'table'}.csv`;
      setDownloadingTable(tableName);
      try {
        const response = await axios.get(`${API_URL}/tables/${encodeURIComponent(tableName)}/export`, {
          headers: authHeaders(),
          responseType: 'blob',
        });

        const blob = new Blob([response.data], { type: 'text/csv;charset=utf-8' });
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', safeName);
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(url);
        messageRef.current.success(`Downloaded ${tableName} as CSV.`);
      } catch (err) {
        console.error(`Failed to download table ${tableName}`, err);
        messageRef.current.error('Failed to download CSV for this table.');
      } finally {
        setDownloadingTable(null);
      }
    },
    [authHeaders],
  );

  const fetchTables = useCallback(async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/tables`, {
        headers: authHeaders(),
      });

      if (response.data?.success) {
        setTables(response.data.tables || []);
      } else {
        messageRef.current.error('Failed to load tables.');
      }
    } catch (err) {
      console.error('Failed to load tables', err);
      messageRef.current.error('Failed to load tables.');
    } finally {
      setLoading(false);
    }
  }, [authHeaders]);

  useEffect(() => {
    fetchTables();
  }, [fetchTables]);

  const filteredAndSortedTables = useMemo(() => {
    let list = [...tables];

    if (searchTerm.trim()) {
      const normalized = searchTerm.trim().toLowerCase();
      list = list.filter((table) => table.table_name.toLowerCase().includes(normalized));
    }

    if (typeof minRows === 'number') {
      list = list.filter((table) => table.row_count >= minRows);
    }

    if (rowState === 'non-empty') {
      list = list.filter((table) => table.row_count > 0);
    } else if (rowState === 'empty-only') {
      list = list.filter((table) => table.row_count === 0);
    }

    if (sorter.field) {
      const direction = sorter.order === 'descend' ? -1 : 1;
      list.sort((a, b) => {
        if (sorter.field === 'row_count') {
          return (a.row_count - b.row_count) * direction;
        }

        return a.table_name.localeCompare(b.table_name) * direction;
      });
    }

    return list;
  }, [tables, searchTerm, minRows, rowState, sorter.field, sorter.order]);

  const handleTableChange = (
    _pagination: TablePaginationConfig,
    _filters: Record<string, FilterValue | null>,
    sorterInfo: SorterResult<TableInfo> | SorterResult<TableInfo>[],
    _extra: TableCurrentDataSource<TableInfo>,
  ) => {
    const currentSorter = Array.isArray(sorterInfo) ? sorterInfo[0] : sorterInfo;
    setSorter({
      field: (currentSorter?.field as string) || undefined,
      order: currentSorter?.order,
    });
  };

  const totalTables = tables.length;
  const totalRows = tables.reduce((sum, table) => sum + (table.row_count || 0), 0);

  const columns = [
    {
      title: 'Table',
      dataIndex: 'table_name',
      key: 'table_name',
      sorter: true,
      sortOrder: sorter.field === 'table_name' ? sorter.order : null,
      render: (value: string) => (
        <Space>
          <TableOutlined />
          <Link onClick={() => navigate(`/tables/${encodeURIComponent(value)}`)}>{value}</Link>
        </Space>
      ),
    },
    {
      title: 'Rows',
      dataIndex: 'row_count',
      key: 'row_count',
      align: 'right' as const,
      sorter: true,
      sortOrder: sorter.field === 'row_count' ? sorter.order : null,
      render: (value: number) => (
        <Tag color={value > 0 ? 'green' : 'default'} style={{ marginRight: 0 }}>
          {value.toLocaleString()}
        </Tag>
      ),
      width: 180,
    },
    {
      title: '',
      key: 'actions',
      width: 220,
      render: (_: unknown, record: TableInfo) => (
        <Space>
          <Button
            type="link"
            icon={<ArrowRightOutlined />}
            onClick={() => navigate(`/tables/${encodeURIComponent(record.table_name)}`)}
          >
            Open
          </Button>
          <Tooltip title="Download CSV">
            <Button
              type="link"
              icon={<DownloadOutlined />}
              loading={downloadingTable === record.table_name}
              onClick={() => downloadTable(record.table_name)}
            >
              CSV
            </Button>
          </Tooltip>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: 12,
        }}
      >
        <Space direction="vertical" size={4}>
          <Title level={3} style={{ marginBottom: 0 }}>
            Tables
          </Title>
          <Text type="secondary">
            Browse the tables created from your mappings. Search, filter, and sort to jump into the data quickly.
          </Text>
        </Space>
        <Button icon={<ReloadOutlined />} onClick={fetchTables}>
          Refresh
        </Button>
      </div>

      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} md={8} lg={6}>
          <Card>
            <Statistic title="Tables available" value={totalTables} />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={8} lg={6}>
          <Card>
            <Statistic title="Total rows across tables" value={totalRows} />
          </Card>
        </Col>
      </Row>

      <Card>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Space size="middle" wrap>
            <Search
              placeholder="Search table name"
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              onSearch={(value) => setSearchTerm(value)}
              allowClear
              enterButton={<SearchOutlined />}
              style={{ width: 280 }}
            />

            <InputNumber
              placeholder="Min rows"
              min={0}
              value={minRows}
              onChange={(value) => setMinRows(typeof value === 'number' ? value : undefined)}
              style={{ width: 160 }}
            />

            <Select<RowStateFilter>
              value={rowState}
              onChange={setRowState}
              options={rowStateOptions}
              style={{ width: 160 }}
            />

            <Button
              onClick={() => {
                setSearchTerm('');
                setMinRows(undefined);
                setRowState('all');
                setSorter({ field: 'table_name', order: 'ascend' });
              }}
            >
              Reset
            </Button>
          </Space>

          {filteredAndSortedTables.length === 0 && !loading ? (
            <Empty
              description={
                searchTerm || typeof minRows === 'number' || rowState !== 'all'
                  ? 'No tables match your filters.'
                  : 'No tables have been created yet.'
              }
            />
          ) : (
            <Table
              rowKey="table_name"
              columns={columns}
              dataSource={filteredAndSortedTables}
              pagination={false}
              loading={loading}
              onChange={handleTableChange}
            />
          )}
        </Space>
      </Card>
    </Space>
  );
};

export default TablesListPage;
