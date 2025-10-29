/**
 * Component to display query results in a table format
 */

import React, { useMemo } from 'react';
import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import Papa from 'papaparse';

interface ResultsTableProps {
  csvData: string;
  maxHeight?: number;
}

export const ResultsTable: React.FC<ResultsTableProps> = ({ 
  csvData, 
  maxHeight = 400 
}) => {
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

  if (dataSource.length === 0) {
    return (
      <div style={{ padding: '20px', textAlign: 'center', color: '#999' }}>
        No data to display
      </div>
    );
  }

  return (
    <Table
      columns={columns}
      dataSource={dataSource}
      pagination={{
        pageSize: 10,
        showSizeChanger: true,
        showTotal: (total) => `Total ${total} rows`,
        pageSizeOptions: ['10', '20', '50', '100'],
      }}
      scroll={{ x: 'max-content', y: maxHeight }}
      size="small"
      bordered
    />
  );
};
