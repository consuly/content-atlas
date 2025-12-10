import React from 'react';
import { Card, Row, Col, Statistic, Alert, Space, Button, Table, Typography, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  ArchiveAutoProcessResult,
  ArchiveFileResult,
  ArchiveFileStatus,
} from './types';

const { Text } = Typography;

interface ArchiveResultsPanelProps {
  effectiveArchiveResult: ArchiveAutoProcessResult | null;
  archiveAggregates: {
    totalRecords: number;
    totalDuplicates: number;
    tablesTouched: number;
  } | null;
  suppressArchiveFailureAlert: boolean;
  failedArchiveResults: ArchiveFileResult[];
  disableMappingActions: boolean;
  archiveResumeLoading: boolean;
  onArchiveResume: (options: { resumeAll?: boolean }) => void;
  onNavigate: (path: string) => void;
}

export const ArchiveResultsPanel: React.FC<ArchiveResultsPanelProps> = ({
  effectiveArchiveResult,
  archiveAggregates,
  suppressArchiveFailureAlert,
  failedArchiveResults,
  disableMappingActions,
  archiveResumeLoading,
  onArchiveResume,
  onNavigate,
}) => {
  if (!effectiveArchiveResult) return null;

  const archiveResultRows = effectiveArchiveResult.results.map((item, index) => ({
    ...item,
    key: `${item.archive_path}-${index}`,
  }));

  const archiveResultsColumns: ColumnsType<ArchiveFileResult & { key: string }> = [
    {
      title: 'Actions',
      key: 'actions',
      fixed: 'left',
      width: 60,
      render: (_: unknown, record) =>
        record.uploaded_file_id ? (
          <Button
            type="link"
            size="small"
            onClick={() => onNavigate(`/import/${record.uploaded_file_id}`)}
          >
            View
          </Button>
        ) : null,
    },
    {
      title: 'Archive Path',
      dataIndex: 'archive_path',
      key: 'archive_path',
      width: 250,
      ellipsis: true,
      render: (text: string) => <Text code>{text}</Text>,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (value: ArchiveFileStatus) => {
        const color =
          value === 'processed' ? 'green' : value === 'failed' ? 'red' : 'default';
        return <Tag color={color}>{value}</Tag>;
      },
    },
    {
      title: 'Table',
      dataIndex: 'table_name',
      key: 'table_name',
      width: 180,
      ellipsis: true,
      render: (value?: string | null) => value || '-',
    },
    {
      title: 'Records',
      dataIndex: 'records_processed',
      key: 'records_processed',
      width: 120,
      render: (value?: number | null) =>
        typeof value === 'number' ? value.toLocaleString() : '-',
    },
    {
      title: 'Duplicates',
      dataIndex: 'duplicates_skipped',
      key: 'duplicates_skipped',
      width: 120,
      render: (value?: number | null) =>
        typeof value === 'number' ? value.toLocaleString() : '-',
    },
  ];

  return (
    <Card title="Archive Results" style={{ marginTop: 24 }}>
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Statistic title="Processed" value={effectiveArchiveResult.processed_files} />
        </Col>
        <Col span={6}>
          <Statistic title="Failed" value={effectiveArchiveResult.failed_files} />
        </Col>
        <Col span={6}>
          <Statistic title="Skipped" value={effectiveArchiveResult.skipped_files} />
        </Col>
        <Col span={6}>
          <Statistic title="Total Files" value={effectiveArchiveResult.total_files} />
        </Col>
      </Row>
      {archiveAggregates && (
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={8}>
            <Statistic
              title="Rows Inserted"
              value={archiveAggregates.totalRecords}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Duplicates Skipped"
              value={archiveAggregates.totalDuplicates}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Tables Updated"
              value={archiveAggregates.tablesTouched}
            />
          </Col>
        </Row>
      )}
      {effectiveArchiveResult.failed_files > 0 && !suppressArchiveFailureAlert && (
        <>
          <Alert
            type="error"
            showIcon
            style={{ marginBottom: 12 }}
            message={`We could not import ${effectiveArchiveResult.failed_files} file${
              effectiveArchiveResult.failed_files === 1 ? '' : 's'
            } from this archive.`}
            description={
              failedArchiveResults.length > 0
                ? `First failure: ${failedArchiveResults[0].archive_path} — ${failedArchiveResults[0].message || 'No details reported.'}`
                : undefined
            }
          />
          <Space style={{ marginBottom: 12 }} wrap>
            <Button
              type="primary"
              onClick={() => onArchiveResume({ resumeAll: false })}
              disabled={disableMappingActions || archiveResumeLoading}
              loading={archiveResumeLoading}
            >
              Retry Failed Files
            </Button>
            <Button
              onClick={() => onArchiveResume({ resumeAll: true })}
              disabled={disableMappingActions || archiveResumeLoading}
              loading={archiveResumeLoading}
            >
              Reprocess Entire Archive
            </Button>
          </Space>
        </>
      )}
      <Table
        dataSource={archiveResultRows}
        columns={archiveResultsColumns}
        pagination={false}
        size="small"
        scroll={{ x: 'max-content' }}
      />
    </Card>
  );
};
