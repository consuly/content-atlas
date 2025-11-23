import React from 'react';
import { Card, Space, Typography } from 'antd';
import {
  Chart as ChartJS,
  ArcElement,
  BarElement,
  CategoryScale,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip,
} from 'chart.js';
import { Bar, Line, Pie } from 'react-chartjs-2';
import type { ChartData, ChartOptions } from 'chart.js';
import { ChartSuggestion } from './types';

ChartJS.register(
  ArcElement,
  BarElement,
  CategoryScale,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Tooltip
);

const { Text } = Typography;

interface ChartPreviewProps {
  suggestion?: ChartSuggestion;
}

export const ChartPreview: React.FC<ChartPreviewProps> = ({ suggestion }) => {
  if (!suggestion) {
    return null;
  }

  if (!suggestion.should_display || !suggestion.spec) {
    return (
      <Card size="small" bodyStyle={{ padding: '12px 16px' }} style={{ background: '#f7f9fc' }}>
        <Space direction="vertical" size={4}>
          <Text strong>Chart not shown</Text>
          <Text type="secondary">{suggestion.reason}</Text>
        </Space>
      </Card>
    );
  }

  const { spec } = suggestion;
  const data: ChartData<'bar' | 'line' | 'pie', number[], string> = {
    labels: spec.labels,
    datasets: spec.datasets,
  };
  const options = {
    maintainAspectRatio: false,
    ...(spec.options ?? {}),
  } as ChartOptions<'bar' | 'line' | 'pie'>;

  const renderChart = () => {
    switch (spec.type) {
      case 'line':
        return (
          <Line
            data={data as ChartData<'line', number[], string>}
            options={options as ChartOptions<'line'>}
          />
        );
      case 'pie':
        return (
          <Pie
            data={data as ChartData<'pie', number[], string>}
            options={options as ChartOptions<'pie'>}
          />
        );
      default:
        return (
          <Bar
            data={data as ChartData<'bar', number[], string>}
            options={options as ChartOptions<'bar'>}
          />
        );
    }
  };

  return (
    <Card
      size="small"
      title="Suggested Chart"
      bodyStyle={{ padding: '12px 16px' }}
      style={{ marginBottom: 12 }}
    >
      <div style={{ height: 320 }}>{renderChart()}</div>
      <Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
        {suggestion.reason}
      </Text>
    </Card>
  );
};

export default ChartPreview;
