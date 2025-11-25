/**
 * Component to display query results in a table format
 */

import React, { useMemo, useEffect, useState } from 'react';
import { Table, Button, Space, Typography } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import Papa from 'papaparse';

const { Text } = Typography;

interface ResultsTableProps {
  csvData: string;
  maxHeight?: number;
}

export const ResultsTable: React.FC<ResultsTableProps> = ({ 
  csvData, 
  maxHeight = 400 
}) => {
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 10,
  });

  const handleDownload = () => {
    // Create a blob from the CSV data
    const blob = new Blob([csvData], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    
    // Create a temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `query-results-${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(link);
    link.click();
    
    // Cleanup
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const { columns, dataSource } = useMemo(() => {
    try {
      // Parse CSV data
      const parsed = Papa.parse(csvData, {
        header: true,
        skipEmptyLines: true,
      });

      if (parsed.errors.length > 0) {
        console.error('CSV parsing errors:', parsed.errors);
      }

      const data = parsed.data as Record<string, unknown>[];

      if (data.length === 0) {
        return { columns: [], dataSource: [] };
      }

      // Generate columns from first row
      const cols: ColumnsType<Record<string, unknown>> = Object.keys(data[0]).map(
        (key) => ({
          title: key,
          dataIndex: key,
          key: key,
          ellipsis: true,
          width: 150,
          render: (value: unknown) => {
            if (value === null || value === undefined || value === '') {
              return <span style={{ color: '#999' }}>-</span>;
            }
            
            // Check if value is numeric and format with thousand separators
            const numValue = Number(value);
            if (!isNaN(numValue) && typeof value !== 'boolean' && value !== '') {
              return numValue.toLocaleString('en-US', {
                maximumFractionDigits: 10, // Preserve decimals
                minimumFractionDigits: 0   // Don't force decimals on integers
              });
            }
            
            return String(value);
          },
        })
      );

      // Add row number column
      const columnsWithIndex: ColumnsType<Record<string, unknown>> = [
        {
          title: '#',
          key: 'index',
          width: 60,
          fixed: 'left',
          render: (_: unknown, __: unknown, index: number) => index + 1,
        },
        ...cols,
      ];

      // Add unique key to each row
      const dataWithKeys = data.map((row, index) => ({
        ...row,
        key: `row-${index}`,
      }));

      return {
        columns: columnsWithIndex,
        dataSource: dataWithKeys,
      };
    } catch (error) {
      console.error('Error parsing CSV:', error);
      return { columns: [], dataSource: [] };
    }
  }, [csvData]);

  useEffect(() => {
    // Reset pagination when new results arrive
    setPagination((prev) => ({
      ...prev,
      current: 1,
    }));
  }, [csvData]);

  useEffect(() => {
    // Clamp the current page if the dataset shrinks
    setPagination((prev) => {
      const maxPage = Math.max(1, Math.ceil(dataSource.length / prev.pageSize));
      if (prev.current > maxPage) {
        return { ...prev, current: maxPage };
      }
      return prev;
    });
  }, [dataSource.length]);

  if (dataSource.length === 0) {
    return (
      <div style={{ padding: '20px', textAlign: 'center', color: '#999' }}>
        No data to display
      </div>
    );
  }

  return (
    <div>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <Text strong>Query Results</Text>
          <Text type="secondary">({dataSource.length} rows)</Text>
        </Space>
        <Button
          icon={<DownloadOutlined />}
          onClick={handleDownload}
          size="small"
        >
          Download CSV
        </Button>
      </div>
      <Table
        columns={columns}
        dataSource={dataSource}
        pagination={{
          current: pagination.current,
          pageSize: pagination.pageSize,
          showSizeChanger: true,
          showTotal: (total) => `Total ${total} rows`,
          pageSizeOptions: ['10', '20', '50', '100'],
          total: dataSource.length,
        }}
        onChange={(nextPagination) => {
          setPagination({
            current: nextPagination.current || 1,
            pageSize: nextPagination.pageSize || pagination.pageSize,
          });
        }}
        scroll={{ x: 'max-content', y: maxHeight }}
        size="small"
        bordered
      />
    </div>
  );
};
