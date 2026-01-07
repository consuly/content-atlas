import React, { useState, useEffect } from "react";
import { List, useTable } from "@refinedev/antd";
import { useCustom, useCustomMutation, useInvalidate } from "@refinedev/core";
import { 
    Table, Tabs, Tag, Space, Button, Input, Modal, Form, 
    Radio, Descriptions, Card, Checkbox, Row, Col, message, Typography,
    Spin, Divider, Empty, Alert, theme
} from "antd";
import { 
    ExclamationCircleOutlined, SearchOutlined, CheckOutlined, CloseOutlined,
    SafetyCertificateOutlined, SwapOutlined, EditOutlined,
    DeleteOutlined, WarningOutlined, FormOutlined, PlusOutlined
} from "@ant-design/icons";
import { useLocation } from "react-router";

const { Text, Paragraph } = Typography;

// Helper component for consistent selection cards
interface SelectionCardProps {
    title: string;
    description: string;
    icon: React.ReactNode;
    selected: boolean;
    onClick: () => void;
    color: string;
    bgColor: string;
}

const SelectionCard: React.FC<SelectionCardProps> = ({
    title,
    description,
    icon,
    selected,
    onClick,
    color,
    bgColor,
}) => (
    <Card 
        hoverable 
        onClick={onClick}
        style={{ 
            height: '100%',
            borderColor: selected ? color : undefined,
            backgroundColor: selected ? bgColor : undefined,
            borderWidth: selected ? 2 : 1,
            cursor: 'pointer'
        }}
        bodyStyle={{ padding: 16 }}
    >
        <Space direction="vertical" align="center" style={{ width: '100%', textAlign: 'center' }}>
            <div style={{ fontSize: 24, color: color }}>
                {icon}
            </div>
            <Text strong style={{ color: selected ? color : undefined }}>{title}</Text>
            <Text type="secondary" style={{ fontSize: 12 }}>{description}</Text>
        </Space>
    </Card>
);

interface DuplicateResolutionModalProps {
    visible: boolean;
    onCancel: () => void;
    onSuccess: () => void;
    duplicate: any;
}

const DuplicateResolutionModal: React.FC<DuplicateResolutionModalProps> = ({ 
    visible, 
    onCancel, 
    onSuccess, 
    duplicate 
}) => {
    const { token } = theme.useToken();
    const [selectedValues, setSelectedValues] = useState<Record<string, any>>({});
    const [mergeStrategy, setMergeStrategy] = useState<'keep_existing' | 'merge' | 'create_new'>('keep_existing');

    const isEnabled = !!duplicate?.import_id && !!duplicate?.id;
    const result = useCustom<any>({
        url: isEnabled ? `import-history/${duplicate.import_id}/duplicates/${duplicate.id}` : "",
        method: "get",
        config: {
            query: {},
        },
        queryOptions: {
            enabled: isEnabled
        }
    });
    
    // Handle potential wrapper around hook result (as seen in logs: { query, result, ... })
    const hookResult = (result as any).query || result;

    // Cast to any to avoid TS errors with opaque types
    const { data: detailData, isLoading, isError, error } = hookResult as any;

    const { mutate: resolveDuplicate, mutation } = useCustomMutation<any>();
    const isSubmitting = (mutation as any).isLoading || (mutation as any).isPending;

    // Handle both wrapped and unwrapped data structures to ensure we get the detail object
    const detail = detailData?.data || detailData;

    const existingRow = detail?.existing_row?.record || {};
    const newRecord = detail?.duplicate?.record || {};
    const allKeys = Array.from(new Set([...Object.keys(existingRow), ...Object.keys(newRecord)]));

    // Initialize selected values when data loads or strategy changes
    useEffect(() => {
        if (!detail) return;

        if (mergeStrategy === 'keep_existing') {
            setSelectedValues(existingRow);
        } else if (mergeStrategy === 'create_new') {
            // When creating new, we are effectively keeping the Incoming record as a NEW entry.
            // Highlight incoming values to show what is being added.
            setSelectedValues(newRecord);
        } else {
            // Merge: default to newRecord (incoming), let user override
            setSelectedValues(newRecord);
        }
    }, [detail, mergeStrategy]);

    const handleMerge = () => {
        if (!duplicate) return;

        resolveDuplicate({
            url: `import-history/${duplicate.import_id}/duplicates/${duplicate.id}/merge`,
            method: "post",
            values: {
                updates: mergeStrategy === 'merge' ? selectedValues : {},
                resolved_by: "user", // In a real app, get from auth context
                note: `Resolved via UI with strategy: ${mergeStrategy}`,
                strategy: mergeStrategy
            },
            successNotification: () => {
                return {
                    message: "Duplicate Resolved",
                    description: "The duplicate row has been successfully processed.",
                    type: "success",
                };
            },
            errorNotification: (error: any) => {
                return {
                    message: "Resolution Failed",
                    description: error?.message || "Something went wrong",
                    type: "error",
                };
            }
        }, {
            onSuccess: () => {
                onSuccess();
                onCancel();
            }
        });
    };

    const handleValueSelection = (key: string, value: any) => {
        if (mergeStrategy !== 'merge') {
            setMergeStrategy('merge');
        }
        setSelectedValues(prev => ({ ...prev, [key]: value }));
    };

    const renderContent = () => {
        if (isLoading) {
            return (
                <div style={{ textAlign: 'center', padding: 20 }}>
                    <Spin />
                </div>
            );
        }
        
        if (isError) {
            return (
                <Alert 
                    type="error" 
                    message="Failed to load details" 
                    description={error?.message || "An unknown error occurred"} 
                />
            );
        }

        if (!isEnabled) {
             return <Alert type="warning" message="Invalid duplicate record" description="Missing ID or Import ID" />;
        }

        if (!detail) {
            return <Empty description="No details found for this duplicate record." />;
        }

        return (
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                <Row gutter={16}>
                    <Col span={8}>
                        <SelectionCard
                            title="Keep Existing"
                            description="Ignore the new record"
                            icon={<SafetyCertificateOutlined />}
                            selected={mergeStrategy === 'keep_existing'}
                            onClick={() => setMergeStrategy('keep_existing')}
                            color={token.colorSuccess}
                            bgColor={token.colorSuccessBg}
                        />
                    </Col>
                    <Col span={8}>
                        <SelectionCard
                            title="Merge"
                            description="Select specific fields"
                            icon={<EditOutlined />}
                            selected={mergeStrategy === 'merge'}
                            onClick={() => setMergeStrategy('merge')}
                            color={token.colorPrimary}
                            bgColor={token.colorPrimaryBg}
                        />
                    </Col>
                    <Col span={8}>
                        <SelectionCard
                            title="Add as New Entry"
                            description="Create separate record"
                            icon={<PlusOutlined />}
                            selected={mergeStrategy === 'create_new'}
                            onClick={() => setMergeStrategy('create_new')}
                            color={token.colorWarning}
                            bgColor={token.colorWarningBg}
                        />
                    </Col>
                </Row>

                <Table 
                    dataSource={allKeys.map(key => ({ key }))}
                    rowKey="key"
                    pagination={false}
                    size="small"
                    scroll={{ y: 400 }}
                >
                    <Table.Column 
                        title="Field" 
                        dataIndex="key" 
                        width={150}
                        fixed="left"
                    />
                    <Table.Column 
                        title="Current Database Value" 
                        render={(_, record: any) => {
                            const val = existingRow[record.key];
                            const isSelected = JSON.stringify(selectedValues[record.key]) === JSON.stringify(val);
                            
                            // Visual logic:
                            // If selected -> Green (Success)
                            // If not selected -> Grey (Inactive)
                            
                            const bg = isSelected ? token.colorSuccessBg : token.colorFillAlter;
                            const border = isSelected ? token.colorSuccess : 'transparent';
                            const textStyle = isSelected ? {} : { color: token.colorTextDisabled };

                            return (
                                <div 
                                    style={{ 
                                        cursor: 'pointer',
                                        backgroundColor: bg,
                                        padding: 8,
                                        border: `1px solid ${border}`,
                                        borderRadius: token.borderRadius,
                                        ...textStyle
                                    }}
                                    onClick={() => handleValueSelection(record.key, val)}
                                >
                                    <Text style={textStyle}>{JSON.stringify(val)}</Text>
                                    {isSelected && <CheckOutlined style={{ color: token.colorSuccess, marginLeft: 8 }} />}
                                </div>
                            );
                        }}
                    />
                    <Table.Column 
                        title="Incoming Value" 
                        render={(_, record: any) => {
                            const val = newRecord[record.key];
                            const isSelected = JSON.stringify(selectedValues[record.key]) === JSON.stringify(val);
                            
                            const bg = isSelected ? token.colorSuccessBg : token.colorFillAlter;
                            const border = isSelected ? token.colorSuccess : 'transparent';
                            const textStyle = isSelected ? {} : { color: token.colorTextDisabled };

                            return (
                                <div 
                                    style={{ 
                                        cursor: 'pointer',
                                        backgroundColor: bg,
                                        padding: 8,
                                        border: `1px solid ${border}`,
                                        borderRadius: token.borderRadius,
                                        ...textStyle
                                    }}
                                    onClick={() => handleValueSelection(record.key, val)}
                                >
                                    <Text style={textStyle}>{JSON.stringify(val)}</Text>
                                    {isSelected && <CheckOutlined style={{ color: token.colorSuccess, marginLeft: 8 }} />}
                                </div>
                            );
                        }}
                    />
                </Table>
            </Space>
        );
    };

    return (
        <Modal
            title="Resolve Duplicate"
            open={visible}
            onCancel={onCancel}
            width={900}
            footer={[
                <Button key="cancel" onClick={onCancel}>Cancel</Button>,
                <Button 
                    key="submit" 
                    type="primary" 
                    onClick={handleMerge}
                    loading={isSubmitting}
                    disabled={isLoading || !detail}
                >
                    Confirm Resolution
                </Button>
            ]}
        >
            {renderContent()}
        </Modal>
    );
};

interface ValidationResolutionModalProps {
    visible: boolean;
    onCancel: () => void;
    onSuccess: () => void;
    failure: any;
}

const ValidationResolutionModal: React.FC<ValidationResolutionModalProps> = ({
    visible,
    onCancel,
    onSuccess,
    failure
}) => {
    const { token } = theme.useToken();
    const [form] = Form.useForm();
    const [action, setAction] = useState<'inserted_corrected' | 'discarded' | 'inserted_as_is'>('inserted_corrected');
    const { mutate: resolveFailure, mutation } = useCustomMutation<any>();
    const isSubmitting = (mutation as any).isLoading || (mutation as any).isPending;

    useEffect(() => {
        if (failure) {
            form.setFieldsValue(failure.record);
        }
    }, [failure, form]);

    const handleSubmit = async () => {
        try {
            const values = await form.validateFields();
            
            resolveFailure({
                url: `import-history/${failure.import_id}/validation-failures/${failure.id}/resolve`,
                method: "post",
                values: {
                    action: action,
                    corrected_data: action === 'inserted_corrected' ? values : undefined,
                    resolved_by: "user",
                    note: `Resolved via UI with action: ${action}`
                },
                successNotification: (data: any) => ({
                    message: "Validation Failure Resolved",
                    description: data?.message || "Success",
                    type: "success",
                })
            }, {
                onSuccess: () => {
                    onSuccess();
                    onCancel();
                }
            });
        } catch (error) {
            console.error("Validation failed", error);
        }
    };

    return (
        <Modal
            title="Resolve Validation Failure"
            open={visible}
            onCancel={onCancel}
            width={800}
            footer={[
                <Button key="cancel" onClick={onCancel}>Cancel</Button>,
                <Button 
                    key="submit" 
                    type="primary" 
                    onClick={handleSubmit}
                    loading={isSubmitting}
                    danger={action === 'discarded'}
                >
                    {action === 'discarded' ? 'Discard Record' : 'Confirm & Insert'}
                </Button>
            ]}
        >
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                {failure?.validation_errors && (
                    <Alert
                        message="Validation Errors"
                        description={
                            <ul>
                                {failure.validation_errors.map((err: any, idx: number) => (
                                    <li key={idx}>
                                        <Text type="danger">
                                            {err.column ? <strong>{err.column}: </strong> : null}
                                            {err.message || err.error_message}
                                        </Text>
                                    </li>
                                ))}
                            </ul>
                        }
                        type="error"
                        showIcon
                    />
                )}

                <Row gutter={16}>
                    <Col span={8}>
                        <SelectionCard
                            title="Correct & Insert"
                            description="Edit and fix errors"
                            icon={<FormOutlined />}
                            selected={action === 'inserted_corrected'}
                            onClick={() => setAction('inserted_corrected')}
                            color={token.colorPrimary}
                            bgColor={token.colorPrimaryBg}
                        />
                    </Col>
                    <Col span={8}>
                        <SelectionCard
                            title="Insert As Is"
                            description="Force insert despite errors"
                            icon={<WarningOutlined />}
                            selected={action === 'inserted_as_is'}
                            onClick={() => setAction('inserted_as_is')}
                            color={token.colorWarning}
                            bgColor={token.colorWarningBg}
                        />
                    </Col>
                    <Col span={8}>
                        <SelectionCard
                            title="Discard Record"
                            description="Remove this record"
                            icon={<DeleteOutlined />}
                            selected={action === 'discarded'}
                            onClick={() => setAction('discarded')}
                            color={token.colorError}
                            bgColor={token.colorErrorBg}
                        />
                    </Col>
                </Row>

                {/* Keep form mounted but hidden to avoid useForm warning */}
                <div style={{ display: action === 'discarded' ? 'none' : 'block' }}>
                    <Card size="small" title="Record Details">
                        <Form
                            form={form}
                            layout="vertical"
                            initialValues={failure?.record}
                            disabled={action === 'inserted_as_is'}
                        >
                            <Row gutter={16}>
                                {failure?.record && Object.keys(failure.record).map(key => (
                                    <Col span={12} key={key}>
                                        <Form.Item name={key} label={key}>
                                            <Input />
                                        </Form.Item>
                                    </Col>
                                ))}
                            </Row>
                        </Form>
                    </Card>
                </div>
            </Space>
        </Modal>
    );
};

const DuplicatesTable = ({ initialFileName }: { initialFileName?: string | null }) => {
  const invalidate = useInvalidate();
  const { tableProps, searchFormProps } = useTable({
    resource: "import-history/duplicates",
    pagination: { pageSize: 20 },
    filters: {
        initial: initialFileName ? [
            { field: "file_name", operator: "contains", value: initialFileName }
        ] : [],
    },
    onSearch: (params: any) => {
        const { file_name } = params;
        return [{ field: "file_name", operator: "contains", value: file_name }];
    },
  });

  const [modalVisible, setModalVisible] = useState(false);
  const [selectedDuplicate, setSelectedDuplicate] = useState<any>(null);

  const handleResolve = (record: any) => {
      setSelectedDuplicate(record);
      setModalVisible(true);
  };

  const handleModalClose = () => {
      setModalVisible(false);
      setSelectedDuplicate(null);
  };

  return (
    <List title="Duplicates">
        <Space style={{ marginBottom: 16 }}>
            <Input 
                placeholder="Search by File Name" 
                prefix={<SearchOutlined />} 
                defaultValue={initialFileName || undefined}
                onPressEnter={(e: any) => {
                    searchFormProps.onFinish?.({ file_name: e.target.value });
                }}
            />
        </Space>
      <Table {...tableProps} rowKey="id">
        <Table.Column 
            dataIndex="file_name" 
            title="File Name"
            render={(value) => <Tag color="blue">{value}</Tag>}
        />
        <Table.Column dataIndex="table_name" title="Table" />
        <Table.Column 
            dataIndex="record" 
            title="Record Preview"
            render={(record) => (
                <div style={{ maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {JSON.stringify(record)}
                </div>
            )}
        />
        <Table.Column 
            dataIndex="detected_at" 
            title="Detected At"
            render={(value) => value ? new Date(value).toLocaleString() : "-"}
        />
        <Table.Column 
            title="Actions"
            render={(_, record: any) => (
                <Button 
                    size="small" 
                    type="link" 
                    onClick={() => handleResolve(record)}
                    disabled={!!record.resolved_at}
                >
                    {record.resolved_at ? "Resolved" : "Resolve"}
                </Button>
            )}
        />
      </Table>

      {modalVisible && selectedDuplicate && (
          <DuplicateResolutionModal 
            visible={modalVisible}
            duplicate={selectedDuplicate}
            onCancel={handleModalClose}
            onSuccess={() => invalidate({ resource: "import-history/duplicates", invalidates: ["list"] })}
          />
      )}
    </List>
  );
};

const ValidationFailuresTable = ({ initialFileName }: { initialFileName?: string | null }) => {
  const invalidate = useInvalidate();
  const { tableProps, searchFormProps } = useTable({
    resource: "import-history/validation-failures",
    pagination: { pageSize: 20 },
    filters: {
        initial: initialFileName ? [
            { field: "file_name", operator: "contains", value: initialFileName }
        ] : [],
    },
    onSearch: (params: any) => {
        const { file_name } = params;
        return [{ field: "file_name", operator: "contains", value: file_name }];
    },
  });

  const [modalVisible, setModalVisible] = useState(false);
  const [selectedFailure, setSelectedFailure] = useState<any>(null);

  const handleResolve = (record: any) => {
      setSelectedFailure(record);
      setModalVisible(true);
  };

  const handleModalClose = () => {
      setModalVisible(false);
      setSelectedFailure(null);
  };

  return (
    <List title="Validation Failures">
        <Space style={{ marginBottom: 16 }}>
            <Input 
                placeholder="Search by File Name" 
                prefix={<SearchOutlined />} 
                defaultValue={initialFileName || undefined}
                onPressEnter={(e: any) => {
                    searchFormProps.onFinish?.({ file_name: e.target.value });
                }}
            />
        </Space>
      <Table {...tableProps} rowKey="id">
        <Table.Column 
            dataIndex="file_name" 
            title="File Name"
            render={(value) => <Tag color="blue">{value}</Tag>}
        />
        <Table.Column dataIndex="table_name" title="Table" />
        <Table.Column 
            dataIndex="validation_errors" 
            title="Errors"
            render={(errors) => (
                <ul>
                    {Array.isArray(errors) && errors.map((err: any, idx: number) => (
                        <li key={idx}>
                            <Text type="danger">{err.message || err.error_message}</Text> 
                            {err.column && <Tag style={{ marginLeft: 8 }}>{err.column}</Tag>}
                        </li>
                    ))}
                </ul>
            )}
        />
        <Table.Column 
            dataIndex="detected_at" 
            title="Detected At"
            render={(value) => value ? new Date(value).toLocaleString() : "-"}
        />
        <Table.Column 
            title="Actions"
            render={(_, record: any) => (
                <Button 
                    size="small" 
                    type="link" 
                    onClick={() => handleResolve(record)}
                    disabled={!!record.resolved_at}
                >
                    {record.resolved_at ? "Resolved" : "Resolve"}
                </Button>
            )}
        />
      </Table>

      {modalVisible && selectedFailure && (
          <ValidationResolutionModal
            visible={modalVisible}
            failure={selectedFailure}
            onCancel={handleModalClose}
            onSuccess={() => invalidate({ resource: "import-history/validation-failures", invalidates: ["list"] })}
          />
      )}
    </List>
  );
};

const MappingErrorsTable = ({ initialFileName }: { initialFileName?: string | null }) => {
    const { tableProps, searchFormProps } = useTable({
      resource: "import-history/mapping-errors",
      pagination: { pageSize: 20 },
      filters: {
        initial: initialFileName ? [
            { field: "file_name", operator: "contains", value: initialFileName }
        ] : [],
      },
      onSearch: (params: any) => {
        const { file_name } = params;
        return [{ field: "file_name", operator: "contains", value: file_name }];
      },
    });
  
    return (
      <List title="Mapping Errors">
          <Space style={{ marginBottom: 16 }}>
              <Input 
                  placeholder="Search by File Name" 
                  prefix={<SearchOutlined />} 
                  defaultValue={initialFileName || undefined}
                  onPressEnter={(e: any) => {
                      searchFormProps.onFinish?.({ file_name: e.target.value });
                  }}
              />
          </Space>
        <Table {...tableProps} rowKey="id">
          <Table.Column 
              dataIndex="file_name" 
              title="File Name"
              render={(value) => <Tag color="blue">{value}</Tag>}
          />
          <Table.Column dataIndex="table_name" title="Table" />
          <Table.Column dataIndex="error_message" title="Error Message" />
          <Table.Column dataIndex="source_value" title="Source Value" />
          <Table.Column dataIndex="target_field" title="Target Field" />
        </Table>
      </List>
    );
  };

interface RollbackUpdateModalProps {
    visible: boolean;
    onCancel: () => void;
    onSuccess: () => void;
    update: any;
}

const RollbackUpdateModal: React.FC<RollbackUpdateModalProps> = ({
    visible,
    onCancel,
    onSuccess,
    update
}) => {
    const { token } = theme.useToken();
    const [force, setForce] = useState(false);
    const { mutate: rollbackUpdate, mutation } = useCustomMutation<any>();
    const isSubmitting = (mutation as any).isLoading || (mutation as any).isPending;

    const isEnabled = !!update?.import_id && !!update?.id;
    const result = useCustom<any>({
        url: isEnabled ? `import-history/${update.import_id}/updates/${update.id}` : "",
        method: "get",
        config: {
            query: {},
        },
        queryOptions: {
            enabled: isEnabled
        }
    });

    const hookResult = (result as any).query || result;
    const { data: detailData, isLoading, isError, error } = hookResult as any;
    const detail = detailData?.data || detailData;

    const handleRollback = () => {
        if (!update) return;

        rollbackUpdate({
            url: `import-history/${update.import_id}/updates/${update.id}/rollback`,
            method: "post",
            values: {
                rolled_back_by: "user",
                force: force
            },
            successNotification: () => ({
                message: "Update Rolled Back",
                description: "The row has been restored to its previous values.",
                type: "success",
            }),
            errorNotification: (error: any) => ({
                message: "Rollback Failed",
                description: error?.message || "Something went wrong",
                type: "error",
            })
        }, {
            onSuccess: () => {
                onSuccess();
                onCancel();
            }
        });
    };

    const renderContent = () => {
        if (isLoading) {
            return (
                <div style={{ textAlign: 'center', padding: 20 }}>
                    <Spin />
                </div>
            );
        }

        if (isError) {
            return (
                <Alert
                    type="error"
                    message="Failed to load details"
                    description={error?.message || "An unknown error occurred"}
                />
            );
        }

        if (!isEnabled) {
            return <Alert type="warning" message="Invalid update record" description="Missing ID or Import ID" />;
        }

        if (!detail) {
            return <Empty description="No details found for this update record." />;
        }

        const updateInfo = detail.update;
        const currentRow = detail.current_row;
        const updatedColumns = updateInfo.updated_columns || [];

        // Check if there's a conflict
        const hasConflict = updateInfo.has_conflict;

        return (
            <Space direction="vertical" style={{ width: '100%' }} size="large">
                {hasConflict && (
                    <Alert
                        message="Conflict Detected"
                        description="The row has been modified since this update. The current values differ from expected values."
                        type="warning"
                        showIcon
                    />
                )}

                <Descriptions title="Update Information" bordered size="small">
                    <Descriptions.Item label="Table" span={3}>{updateInfo.table_name}</Descriptions.Item>
                    <Descriptions.Item label="Row ID" span={3}>{updateInfo.row_id}</Descriptions.Item>
                    <Descriptions.Item label="Updated At" span={3}>
                        {updateInfo.updated_at ? new Date(updateInfo.updated_at).toLocaleString() : "-"}
                    </Descriptions.Item>
                    <Descriptions.Item label="Updated Columns" span={3}>
                        {updatedColumns.map((col: string) => <Tag key={col}>{col}</Tag>)}
                    </Descriptions.Item>
                </Descriptions>

                <Table
                    dataSource={updatedColumns.map((col: string) => ({ column: col }))}
                    rowKey="column"
                    pagination={false}
                    size="small"
                    scroll={{ y: 400 }}
                >
                    <Table.Column
                        title="Column"
                        dataIndex="column"
                        width={150}
                        fixed="left"
                    />
                    <Table.Column
                        title="Previous Value (Will be restored)"
                        render={(_, record: any) => {
                            const val = updateInfo.previous_values?.[record.column];
                            return (
                                <div
                                    style={{
                                        backgroundColor: token.colorSuccessBg,
                                        padding: 8,
                                        border: `1px solid ${token.colorSuccess}`,
                                        borderRadius: token.borderRadius,
                                    }}
                                >
                                    <Text>{JSON.stringify(val)}</Text>
                                </div>
                            );
                        }}
                    />
                    <Table.Column
                        title="Current Value"
                        render={(_, record: any) => {
                            const val = currentRow?.[record.column];
                            const previousVal = updateInfo.previous_values?.[record.column];
                            const isDifferent = JSON.stringify(val) !== JSON.stringify(previousVal);
                            
                            return (
                                <div
                                    style={{
                                        backgroundColor: isDifferent ? token.colorWarningBg : token.colorFillAlter,
                                        padding: 8,
                                        border: `1px solid ${isDifferent ? token.colorWarning : 'transparent'}`,
                                        borderRadius: token.borderRadius,
                                    }}
                                >
                                    <Text>{JSON.stringify(val)}</Text>
                                    {isDifferent && (
                                        <WarningOutlined style={{ color: token.colorWarning, marginLeft: 8 }} />
                                    )}
                                </div>
                            );
                        }}
                    />
                </Table>

                {hasConflict && (
                    <Checkbox checked={force} onChange={(e) => setForce(e.target.checked)}>
                        Force rollback even with conflicts
                    </Checkbox>
                )}
            </Space>
        );
    };

    return (
        <Modal
            title="Rollback Row Update"
            open={visible}
            onCancel={onCancel}
            width={900}
            footer={[
                <Button key="cancel" onClick={onCancel}>Cancel</Button>,
                <Button
                    key="submit"
                    type="primary"
                    danger
                    onClick={handleRollback}
                    loading={isSubmitting}
                    disabled={isLoading || !detail}
                >
                    Rollback Update
                </Button>
            ]}
        >
            {renderContent()}
        </Modal>
    );
};

const UpdatedRowsTable = ({ initialFileName }: { initialFileName?: string | null }) => {
    const invalidate = useInvalidate();
    const { tableProps, searchFormProps } = useTable({
        resource: "import-history/row-updates",
        pagination: { pageSize: 20 },
        filters: {
            initial: initialFileName ? [
                { field: "file_name", operator: "contains", value: initialFileName }
            ] : [],
        },
        onSearch: (params: any) => {
            const { file_name } = params;
            return [{ field: "file_name", operator: "contains", value: file_name }];
        },
    });

    const [modalVisible, setModalVisible] = useState(false);
    const [selectedUpdate, setSelectedUpdate] = useState<any>(null);

    const handleRollback = (record: any) => {
        setSelectedUpdate(record);
        setModalVisible(true);
    };

    const handleModalClose = () => {
        setModalVisible(false);
        setSelectedUpdate(null);
    };

    return (
        <List title="Row Updates">
            <Space style={{ marginBottom: 16 }}>
                <Input
                    placeholder="Search by File Name"
                    prefix={<SearchOutlined />}
                    defaultValue={initialFileName || undefined}
                    onPressEnter={(e: any) => {
                        searchFormProps.onFinish?.({ file_name: e.target.value });
                    }}
                />
            </Space>
            <Table {...tableProps} rowKey="id">
                <Table.Column
                    dataIndex="file_name"
                    title="File Name"
                    render={(value) => <Tag color="blue">{value}</Tag>}
                />
                <Table.Column dataIndex="table_name" title="Table" />
                <Table.Column
                    dataIndex="updated_columns"
                    title="Updated Columns"
                    render={(columns: string[]) => (
                        <Space size="small" wrap>
                            {columns?.map((col, idx) => <Tag key={idx}>{col}</Tag>)}
                        </Space>
                    )}
                />
                <Table.Column
                    dataIndex="previous_values"
                    title="Previous Values"
                    render={(values) => (
                        <div style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {JSON.stringify(values)}
                        </div>
                    )}
                />
                <Table.Column
                    dataIndex="new_values"
                    title="New Values"
                    render={(values) => (
                        <div style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {JSON.stringify(values)}
                        </div>
                    )}
                />
                <Table.Column
                    dataIndex="updated_at"
                    title="Updated At"
                    render={(value) => value ? new Date(value).toLocaleString() : "-"}
                />
                <Table.Column
                    dataIndex="rolled_back_at"
                    title="Status"
                    render={(value) => (
                        value ? <Tag color="default">Rolled Back</Tag> : <Tag color="success">Active</Tag>
                    )}
                />
                <Table.Column
                    title="Actions"
                    render={(_, record: any) => (
                        <Button
                            size="small"
                            type="link"
                            danger
                            onClick={() => handleRollback(record)}
                            disabled={!!record.rolled_back_at}
                        >
                            {record.rolled_back_at ? "Rolled Back" : "Rollback"}
                        </Button>
                    )}
                />
            </Table>

            {modalVisible && selectedUpdate && (
                <RollbackUpdateModal
                    visible={modalVisible}
                    update={selectedUpdate}
                    onCancel={handleModalClose}
                    onSuccess={() => invalidate({ resource: "import-history/row-updates", invalidates: ["list"] })}
                />
            )}
        </List>
    );
};

export const DataIssuesPage = () => {
  const location = useLocation();
  const queryParams = new URLSearchParams(location.search);
  const tabParam = queryParams.get("tab");
  const fileNameParam = queryParams.get("file_name");
  const [activeTab, setActiveTab] = useState(tabParam || "duplicates");

  useEffect(() => {
    if (tabParam) {
        setActiveTab(tabParam);
    }
  }, [tabParam]);

  const items = [
    {
      key: "duplicates",
      label: "Duplicates",
      children: <DuplicatesTable initialFileName={fileNameParam} />,
    },
    {
      key: "validation",
      label: "Validation Failures",
      children: <ValidationFailuresTable initialFileName={fileNameParam} />,
    },
    {
      key: "mapping",
      label: "Mapping Errors",
      children: <MappingErrorsTable initialFileName={fileNameParam} />,
    },
    {
      key: "updated",
      label: "Row Updates",
      children: <UpdatedRowsTable initialFileName={fileNameParam} />,
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      <Tabs activeKey={activeTab} onChange={setActiveTab} items={items} />
    </div>
  );
};
