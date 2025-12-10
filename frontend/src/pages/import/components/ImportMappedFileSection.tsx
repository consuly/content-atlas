import React, { useMemo } from 'react';
import {
  Card,
  Button,
  Space,
  Alert,
  Spin,
  Typography,
  Statistic,
  Row,
  Col,
  Descriptions,
  Table,
  Tag,
  Divider,
  Modal,
  Switch,
  Input,
} from 'antd';
import type { DescriptionsProps } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  InfoCircleOutlined,
  DatabaseOutlined,
  MergeCellsOutlined,
  EyeOutlined,
  ArrowLeftOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import {
  UploadedFile,
  ImportHistory,
  TableData,
  DuplicateRowsState,
  DuplicateDetail,
  ArchiveAutoProcessResult,
  ImportJobInfo,
  ArchiveFileResult,
  ArchiveFileStatus,
} from './types';

const { Text } = Typography;
const { TextArea } = Input;

interface ImportMappedFileSectionProps {
  file: UploadedFile;
  importHistory: ImportHistory | null;
  tableData: TableData | null;
  duplicateData: DuplicateRowsState | null;
  loadingDetails: boolean;
  loadingDuplicates: boolean;
  
  // Archive Props
  isArchiveFile: boolean;
  archiveResult: ArchiveAutoProcessResult | null;
  archiveHistorySummaryResult: ArchiveAutoProcessResult | null;
  archiveJobDetails: ImportJobInfo | null;
  archiveResumeLoading: boolean;
  disableMappingActions: boolean;
  onArchiveResume: (options: { resumeAll?: boolean }) => void;

  // Duplicate Management Props
  selectedDuplicateRowIds: number[];
  bulkMergeLoading: boolean;
  mergeModalVisible: boolean;
  mergeDetail: DuplicateDetail | null;
  mergeSelections: Record<string, boolean>;
  mergeNote: string;
  mergeDetailLoading: boolean;
  mergeLoading: boolean;
  
  onSelectAllDuplicates: () => void;
  onClearDuplicateSelection: () => void;
  onBulkDuplicateMerge: () => void;
  onOpenMergeModal: (id: number) => void;
  onMergeSubmit: () => void;
  onMergeSelectionChange: (column: string, checked: boolean) => void;
  onMergeModalCancel: () => void;
  onMergeNoteChange: (note: string) => void;
  onNavigate: (path: string) => void;
  onSelectionChange: (selectedIds: number[]) => void;
}

export const ImportMappedFileSection: React.FC<ImportMappedFileSectionProps> = ({
  file,
  importHistory,
  tableData,
  duplicateData,
  loadingDetails,
  loadingDuplicates,
  isArchiveFile,
  archiveResult,
  archiveHistorySummaryResult,
  archiveJobDetails,
  archiveResumeLoading,
  disableMappingActions,
  onArchiveResume,
  selectedDuplicateRowIds,
  bulkMergeLoading,
  mergeModalVisible,
  mergeDetail,
  mergeSelections,
  mergeNote,
  mergeDetailLoading,
  mergeLoading,
  onSelectAllDuplicates,
  onClearDuplicateSelection,
  onBulkDuplicateMerge,
  onOpenMergeModal,
  onMergeSubmit,
  onMergeSelectionChange,
  onMergeModalCancel,
  onMergeNoteChange,
  onNavigate,
  onSelectionChange,
}) => {
  const effectiveArchiveResult = archiveResult ?? archiveHistorySummaryResult ?? null;

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const formatDate = (dateString?: string): string => {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString();
  };

  const renderDuplicateValue = (value: unknown): React.ReactNode => {
    if (value === null || value === undefined) {
      return <Text type="secondary">-</Text>;
    }
    if (Array.isArray(value) || typeof value === 'object') {
      try {
        const asJson = JSON.stringify(value);
        return (
          <Text code style={{ maxWidth: 220 }} ellipsis={{ tooltip: asJson }}>
            {asJson}
          </Text>
        );
      } catch (err) {
        console.error('Failed to render duplicate value as JSON', err);
        return String(value);
      }
    }
    const textValue = String(value);
    return (
      <Text style={{ maxWidth: 200 }} ellipsis={{ tooltip: textValue }}>
        {textValue}
      </Text>
    );
  };

  const failedArchiveResults = useMemo(
    () => effectiveArchiveResult?.results.filter((item) => item.status === 'failed') ?? [],
    [effectiveArchiveResult]
  );

  const archiveFailureSummary = useMemo(() => {
    if (!isArchiveFile || !effectiveArchiveResult) {
      return null;
    }

    const failedFiles =
      typeof effectiveArchiveResult.failed_files === 'number' && effectiveArchiveResult.failed_files > 0
        ? effectiveArchiveResult.failed_files
        : 0;
    const processedFiles =
      typeof effectiveArchiveResult.processed_files === 'number'
        ? effectiveArchiveResult.processed_files
        : 0;
    const skippedFiles =
      typeof effectiveArchiveResult.skipped_files === 'number'
        ? effectiveArchiveResult.skipped_files
        : 0;
    const resultCount = Array.isArray(effectiveArchiveResult.results)
      ? effectiveArchiveResult.results.length
      : 0;
    const totalFilesFromResult =
      typeof effectiveArchiveResult.total_files === 'number'
        ? effectiveArchiveResult.total_files
        : resultCount;
    const derivedTotal =
      totalFilesFromResult ||
      processedFiles + failedFiles + skippedFiles ||
      resultCount;
    const totalFiles = Math.max(derivedTotal, 0);
    const successfulFiles = Math.max(0, totalFiles - failedFiles - skippedFiles);
    const hasPartialFailure =
      failedFiles > 0 && totalFiles > 0 && failedFiles < totalFiles;

    return {
      totalFiles,
      failedFiles,
      successfulFiles,
      skippedFiles,
      hasPartialFailure,
    };
  }, [effectiveArchiveResult, isArchiveFile]);

  const archiveAggregates = useMemo(() => {
    if (!effectiveArchiveResult) {
      return null;
    }

    const aggregate = effectiveArchiveResult.results.reduce(
      (acc, item) => {
        if (item.status === 'processed') {
          acc.totalRecords += item.records_processed ?? 0;
          acc.totalDuplicates += item.duplicates_skipped ?? 0;
          if (item.table_name) {
            acc.tableNames.add(item.table_name);
          }
        }
        return acc;
      },
      { totalRecords: 0, totalDuplicates: 0, tableNames: new Set<string>() }
    );

    return {
      totalRecords: aggregate.totalRecords,
      totalDuplicates: aggregate.totalDuplicates,
      tablesTouched: aggregate.tableNames.size,
    };
  }, [effectiveArchiveResult]);

  const suppressArchiveFailureAlert =
    file?.status === 'failed' && !!archiveFailureSummary?.hasPartialFailure;

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

  const archiveResultRows = effectiveArchiveResult
    ? effectiveArchiveResult.results.map((item, index) => ({
        ...item,
        key: `${item.archive_path}-${index}`,
      }))
    : [];

  // ---------------------------------------------------------------------------
  // Render Logic
  // ---------------------------------------------------------------------------

  if (isArchiveFile) {
    const summaryResult = effectiveArchiveResult;
    const jobId = archiveJobDetails?.id ?? archiveResult?.job_id ?? null;
    const jobSource = archiveJobDetails?.trigger_source || 'Auto Process Archive';
    const jobCompletedAt = archiveJobDetails?.completed_at ?? file.mapped_date;
    const filesInArchiveMeta =
      archiveJobDetails?.metadata && typeof archiveJobDetails.metadata['files_in_archive'] === 'number'
        ? (archiveJobDetails.metadata['files_in_archive'] as number)
        : undefined;
    const filesInArchiveCount = filesInArchiveMeta ?? summaryResult?.total_files;
    const summaryTagColor = summaryResult
      ? summaryResult.failed_files > 0
        ? 'orange'
        : 'green'
      : 'default';
    const summaryTagText = summaryResult
      ? summaryResult.failed_files > 0
        ? 'Completed with warnings'
        : 'Completed'
      : 'Awaiting summary';

    const archiveSummaryItems: DescriptionsProps['items'] = [
      {
        key: 'archive-name',
        label: 'Archive',
        children: <Text>{file.file_name}</Text>,
      },
      {
        key: 'file-size',
        label: 'File Size',
        children: formatBytes(file.file_size),
      },
      {
        key: 'uploaded',
        label: 'Uploaded',
        children: formatDate(file.upload_date),
      },
      {
        key: 'last-processed',
        label: 'Last Processed',
        children: formatDate(jobCompletedAt),
      },
      {
        key: 'job-id',
        label: 'Import Job',
        children: jobId ? <Text code>{jobId}</Text> : '-',
      },
      {
        key: 'trigger',
        label: 'Trigger Source',
        children: jobSource,
      },
      {
        key: 'files-total',
        label: 'Files in Archive',
        children:
          typeof filesInArchiveCount === 'number'
            ? filesInArchiveCount.toLocaleString()
            : '-',
      },
      {
        key: 'status',
        label: 'Status',
        children: <Tag color={summaryTagColor}>{summaryTagText}</Tag>,
      },
    ];

    const archiveAlertType =
      summaryResult && summaryResult.failed_files > 0
        ? 'warning'
        : summaryResult
          ? 'success'
          : 'info';
    const archiveAlertDescription = summaryResult
      ? summaryResult.failed_files > 0
        ? 'Some files in this archive failed to import. Review the table below for details.'
        : 'All supported files in this archive were imported successfully.'
      : 'We could not find a previous auto-process summary for this archive.';

    return (
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Descriptions
          title="Archive Details"
          bordered
          size="middle"
          column={2}
          items={archiveSummaryItems}
        />
        <Alert
          type={archiveAlertType}
          message="Archive Import Summary"
          description={archiveAlertDescription}
          showIcon
        />
        {summaryResult ? (
          <>
            <Row gutter={16}>
              <Col span={6}>
                <Statistic title="Processed" value={summaryResult.processed_files} />
              </Col>
              <Col span={6}>
                <Statistic title="Failed" value={summaryResult.failed_files} />
              </Col>
              <Col span={6}>
                <Statistic title="Skipped" value={summaryResult.skipped_files} />
              </Col>
              <Col span={6}>
                <Statistic title="Total Files" value={summaryResult.total_files} />
              </Col>
            </Row>
            {archiveAggregates && (
              <Row gutter={16} style={{ marginTop: 16 }}>
                <Col span={8}>
                  <Statistic title="Rows Inserted" value={archiveAggregates.totalRecords} />
                </Col>
                <Col span={8}>
                  <Statistic
                    title="Duplicates Skipped"
                    value={archiveAggregates.totalDuplicates}
                  />
                </Col>
                <Col span={8}>
                  <Statistic title="Tables Updated" value={archiveAggregates.tablesTouched} />
                </Col>
              </Row>
            )}
            <Card title="Archive Results" style={{ marginTop: 24 }}>
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
          </>
        ) : (
          <Alert
            type="warning"
            showIcon
            message="Archive summary not available"
            description="This ZIP was marked as mapped, but we couldn't locate a completed Auto Process Archive job. Run Auto Process Archive again to rebuild the summary."
          />
        )}
      </Space>
    );
  }

  // Regular File View
  const summaryItems: DescriptionsProps['items'] = [
    {
      key: 'table-name',
      label: 'Table Name',
      children: <Tag color="blue">{file.mapped_table_name}</Tag>,
    },
    {
      key: 'mapped-date',
      label: 'Mapped Date',
      children: formatDate(file.mapped_date),
    },
    {
      key: 'rows-imported',
      label: 'Rows Imported',
      children: <Text strong>{file.mapped_rows?.toLocaleString() || 0}</Text>,
    },
    {
      key: 'file-size',
      label: 'File Size',
      children: formatBytes(file.file_size),
    },
    {
      key: 'upload-date',
      label: 'Upload Date',
      children: formatDate(file.upload_date),
    },
    {
      key: 'status',
      label: 'Status',
      children: <Tag color="success">Mapped</Tag>,
    },
  ];

  const historyItems: DescriptionsProps['items'] = importHistory
    ? [
        ...(importHistory.import_strategy
          ? [
              {
                key: 'import-strategy',
                label: 'Import Strategy',
                children: <Tag>{importHistory.import_strategy}</Tag>,
                span: 2,
              } as const,
            ]
          : []),
        {
          key: 'total-rows',
          label: 'Total Rows in File',
          children:
            importHistory.total_rows_in_file?.toLocaleString() || '-',
        },
        {
          key: 'rows-inserted',
          label: 'Rows Inserted',
          children: importHistory.rows_inserted?.toLocaleString() || '-',
        },
        ...(importHistory.duplicates_found !== undefined &&
        importHistory.duplicates_found > 0
          ? [
              {
                key: 'duplicates-found',
                label: 'Duplicates Found',
                children: (
                  <Text type="warning">
                    {importHistory.duplicates_found.toLocaleString()}
                  </Text>
                ),
                span: 2,
              } as const,
            ]
          : []),
        ...(importHistory.data_validation_errors !== undefined &&
        importHistory.data_validation_errors > 0
          ? [
              {
                key: 'validation-errors',
                label: 'Validation Errors',
                children: (
                  <Text type="danger">
                    {importHistory.data_validation_errors.toLocaleString()}
                  </Text>
                ),
                span: 2,
              } as const,
            ]
          : []),
        ...(importHistory.duration_seconds
          ? [
              {
                key: 'processing-time',
                label: 'Processing Time',
                children: `${importHistory.duration_seconds.toFixed(2)}s`,
                span: 2,
              } as const,
            ]
          : []),
        {
          key: 'import-id',
          label: 'Import ID',
          children: (
            <Text code style={{ fontSize: '11px' }}>
              {importHistory.import_id}
            </Text>
          ),
          span: 2,
        },
      ]
    : [];

  const duplicateRows = duplicateData?.rows ?? [];
  const duplicateKeys = new Set<string>();
  duplicateRows.forEach((row) => {
    Object.keys(row.record || {}).forEach((key) => {
      if (!key.startsWith('_')) {
        duplicateKeys.add(key);
      }
    });
  });

  const resolvedDuplicateIds = new Set(
    duplicateRows
      .filter((row) => typeof row.id === 'number' && !!row.resolved_at)
      .map((row) => row.id as number)
  );
  const selectableDuplicateIds = duplicateRows
    .filter((row) => typeof row.id === 'number' && !row.resolved_at)
    .map((row) => row.id as number);

  const duplicateTableData = duplicateRows.map((row, index) => ({
    key: row.id ?? `duplicate-${index}`,
    duplicate_id: row.id,
    record_number: row.record_number ?? '-',
    detected_at: row.detected_at,
    record: row.record || {},
  }));

  const duplicateTableColumns =
    duplicateRows.length > 0
      ? [
          {
            title: 'Actions',
            key: 'actions',
            fixed: 'left' as const,
            width: 120,
            render: (_: unknown, row: (typeof duplicateTableData)[number]) => (
              <Button
                type="link"
                icon={<MergeCellsOutlined />}
                onClick={() => onOpenMergeModal(row.duplicate_id)}
              >
                Merge
              </Button>
            ),
          },
          {
            title: '#',
            dataIndex: 'record_number',
            key: 'record_number',
            width: 70,
          },
          ...Array.from(duplicateKeys).map((key) => ({
            title: key,
            key,
            ellipsis: true,
            width: 180,
            render: (_: unknown, row: (typeof duplicateTableData)[number]) =>
              renderDuplicateValue(row.record?.[key]),
          })),
          {
            title: 'Detected At',
            dataIndex: 'detected_at',
            key: 'detected_at',
            width: 200,
            render: (value: string | null | undefined) =>
              value ? formatDate(value) : '-',
          },
        ]
      : [];

  const duplicateRowSelection =
    duplicateTableColumns.length > 0
      ? {
          selectedRowKeys: duplicateTableData
            .filter(
              (row) =>
                typeof row.duplicate_id === 'number' &&
                selectedDuplicateRowIds.includes(row.duplicate_id)
            )
            .map((row) => row.key),
          onChange: (
            _selectedRowKeys: React.Key[],
            selectedRows: (typeof duplicateTableData)[number][]
          ) => {
            const ids = selectedRows
              .map((row) => row.duplicate_id)
              .filter((id): id is number => typeof id === 'number');
            onSelectionChange(ids);
          },
          getCheckboxProps: (record: (typeof duplicateTableData)[number]) => ({
            disabled:
              !record.duplicate_id ||
              resolvedDuplicateIds.has(record.duplicate_id) ||
              bulkMergeLoading,
          }),
        }
      : undefined;

  const duplicatesTotal = duplicateData?.total ?? importHistory?.duplicates_found ?? 0;

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Alert
        message="File Already Mapped"
        description="This file has been successfully imported into the database. View the details below."
        type="success"
        showIcon
        icon={<CheckCircleOutlined />}
      />

      {/* Import Summary */}
      <Card title={<><InfoCircleOutlined /> Import Summary</>} size="small">
        <Descriptions column={2} bordered size="small" items={summaryItems} />
      </Card>

      {/* Import Details */}
      {importHistory && (
        <Card title={<><DatabaseOutlined /> Import Details</>} size="small" loading={loadingDetails}>
          <Descriptions
            column={2}
            bordered
            size="small"
            items={historyItems}
          />
        </Card>
      )}

      {duplicatesTotal > 0 && (
        <Card
          title={
            <>
              <InfoCircleOutlined /> Duplicate Rows Skipped
            </>
          }
          size="small"
          loading={loadingDuplicates}
        >
          {duplicateTableColumns.length > 0 ? (
            <>
              <Space style={{ marginBottom: 12 }} wrap>
                <Button
                  onClick={onSelectAllDuplicates}
                  disabled={selectableDuplicateIds.length === 0 || bulkMergeLoading}
                >
                  Select All
                </Button>
                <Button
                  onClick={onClearDuplicateSelection}
                  disabled={selectedDuplicateRowIds.length === 0 || bulkMergeLoading}
                >
                  Clear Selection
                </Button>
                <Button
                  type="primary"
                  onClick={onBulkDuplicateMerge}
                  disabled={selectedDuplicateRowIds.length === 0}
                  loading={bulkMergeLoading}
                >
                  Map Selected ({selectedDuplicateRowIds.length})
                </Button>
              </Space>
              <Table
                dataSource={duplicateTableData}
                columns={duplicateTableColumns}
                rowSelection={duplicateRowSelection}
                pagination={false}
                size="small"
                scroll={{ x: 'max-content' }}
              />
              <Divider />
              <Text type="secondary">
                Showing {duplicateTableData.length} of {duplicatesTotal}{' '}
                duplicate rows
              </Text>
            </>
          ) : (
            <Text type="secondary">
              Duplicate rows were detected, but no preview data is available.
            </Text>
          )}
        </Card>
      )}
      <Modal
        open={mergeModalVisible}
        title={
          <Space>
            <MergeCellsOutlined />
            <span>Merge Duplicate Row</span>
          </Space>
        }
        onCancel={onMergeModalCancel}
        onOk={onMergeSubmit}
        okButtonProps={{
          loading: mergeLoading,
          disabled: mergeDetailLoading || !mergeDetail || !mergeDetail.existing_row,
        }}
        cancelButtonProps={{
          disabled: mergeLoading,
        }}
        width={780}
      >
        {mergeDetailLoading ? (
          <div style={{ textAlign: 'center', padding: '24px 0' }}>
            <Spin />
          </div>
        ) : mergeDetail ? (
          <>
            {!mergeDetail.existing_row && (
              <Alert
                type="warning"
                message="Matching row not found"
                description="We could not find a matching row in the destination table for this duplicate. No merge is possible."
                style={{ marginBottom: 16 }}
              />
            )}
            {mergeDetail.existing_row && (
              <>
                <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
                  Matching row identified using uniqueness columns:{' '}
                  {mergeDetail.uniqueness_columns.join(', ')}
                </Text>
                <Text strong style={{ marginBottom: 12, display: 'block' }}>
                  Select which values to apply from the duplicate row:
                </Text>
                <Table
                  dataSource={Object.keys(mergeDetail.duplicate.record)
                    .filter((column) => !column.startsWith('_'))
                    .map((column) => ({
                      key: column,
                      column,
                      existing: mergeDetail.existing_row?.record?.[column],
                      incoming: mergeDetail.duplicate.record[column],
                      selected: mergeSelections[column] ?? false,
                    }))}
                  pagination={false}
                  size="small"
                  rowKey="column"
                  columns={[
                    {
                      title: 'Column',
                      dataIndex: 'column',
                      key: 'column',
                      width: 160,
                    },
                    {
                      title: 'Existing Value',
                      dataIndex: 'existing',
                      key: 'existing',
                      render: (value: unknown) => renderDuplicateValue(value),
                    },
                    {
                      title: 'Incoming Value',
                      dataIndex: 'incoming',
                      key: 'incoming',
                      render: (value: unknown) => renderDuplicateValue(value),
                    },
                    {
                      title: 'Use Incoming',
                      key: 'selected',
                      width: 140,
                      render: (_: unknown, row: { column: string; selected: boolean }) => (
                        <Switch
                          checked={!!mergeSelections[row.column]}
                          onChange={(checked) => onMergeSelectionChange(row.column, checked)}
                        />
                      ),
                    },
                  ]}
                />
                <Divider />
                <TextArea
                  value={mergeNote}
                  onChange={(event) => onMergeNoteChange(event.target.value)}
                  placeholder="Optional note about this merge"
                  rows={3}
                />
              </>
            )}
          </>
        ) : (
          <Text type="secondary">Select a duplicate row to merge.</Text>
        )}
      </Modal>

      {/* Data Preview */}
      {tableData && tableData.data.length > 0 && (
        <Card 
          title={<><EyeOutlined /> Imported Data Preview</>} 
          size="small"
          loading={loadingDetails}
          extra={
            <Button
              type="link"
              onClick={() =>
                file?.mapped_table_name &&
                onNavigate(`/tables/${encodeURIComponent(file.mapped_table_name)}`)
              }
              disabled={!file?.mapped_table_name}
            >
              View Full Table
            </Button>
          }
        >
          <Table
            dataSource={tableData.data}
            columns={Object.keys(tableData.data[0] || {})
              .filter((key) => key !== "__rowKey")
              .map((key) => ({
                title: key,
                dataIndex: key,
                key,
                ellipsis: true,
                width: 150,
              }))}
            pagination={false}
            scroll={{ x: 'max-content' }}
            size="small"
            rowKey="__rowKey"
          />
          <Divider />
          <Text type="secondary">
            Showing {tableData.data.length} of {tableData.total_rows.toLocaleString()} rows from this import
          </Text>
        </Card>
      )}

      {/* Action Buttons */}
      <Space>
        <Button 
          type="primary" 
          icon={<DatabaseOutlined />}
          onClick={() => onNavigate(`/query`)}
        >
          Query This Data
        </Button>
        <Button
          icon={<EyeOutlined />}
          onClick={() =>
            file?.mapped_table_name &&
            onNavigate(`/tables/${encodeURIComponent(file.mapped_table_name)}`)
          }
          disabled={!file?.mapped_table_name}
        >
          View Full Table
        </Button>
        <Button 
          icon={<ArrowLeftOutlined />}
          onClick={() => onNavigate('/import')}
        >
          Back to Import List
        </Button>
      </Space>
    </Space>
  );
};
