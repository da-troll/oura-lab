"use client";

import { memo } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { TooltipProps } from "recharts";

export interface ChatChartSeries {
  key: string;
  label: string;
  color?: string;
}

export interface ChatChartArtifact {
  chartType:
    | "line"
    | "multi_line"
    | "area"
    | "stacked_area"
    | "bar"
    | "grouped_bar"
    | "histogram"
    | "scatter"
    | "scatter_xy"
    | "single_value"
    | "stacked_bar"
    | "radar";
  title: string;
  xKey: string;
  yKey?: string;
  xAxisLabel?: string;
  yAxisLabel?: string;
  series: ChatChartSeries[];
  data: Array<Record<string, unknown>>;
  unit?: string;
  source?: string;
  dateRange?: string;
  yDomain?: [number, number];
}

const TOOL_SOURCE_LABELS: Record<string, string> = {
  get_summary: "Health Summary",
  get_metric_series: "Metric Series",
  get_multi_metric_series: "Multi-metric Series",
  get_scatter_data: "Scatter Analysis",
  get_correlations: "Correlation Analysis",
  get_anomalies: "Anomaly Detection",
  get_trends: "Trend Detection",
  get_metric_distribution: "Metric Distribution",
  get_period_comparison: "Period Comparison",
  get_sleep_architecture: "Sleep Architecture",
  get_chronotype: "Chronotype Analysis",
};

function formatDateRangeLabel(dateRange?: string): string {
  if (!dateRange) return "";
  const trimmed = dateRange.trim();
  if (!trimmed) return "";
  return trimmed.charAt(0).toUpperCase() + trimmed.slice(1);
}

const UPPERCASE_WORDS = new Set(["hrv", "rhr", "spo2", "bmi"]);

function titleCaseWords(value: string): string {
  return value
    .split(" ")
    .filter(Boolean)
    .map((word) =>
      UPPERCASE_WORDS.has(word.toLowerCase())
        ? word.toUpperCase()
        : word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    )
    .join(" ");
}

function humanizeLabel(value: string): string {
  if (!value) return value;
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return value;
  }
  const normalized = value.replace(/[_-]+/g, " ").trim();
  if (!normalized) return value;
  return titleCaseWords(normalized);
}

function formatSourceLabel(source?: string): string {
  if (!source) return "";
  return TOOL_SOURCE_LABELS[source] || humanizeLabel(source);
}

function formatNumericTick(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return value == null ? "" : String(value);
  }

  const rounded = Math.round(value);
  if (Math.abs(value - rounded) < 1e-6) {
    return String(rounded);
  }

  return String(Number(value.toFixed(2)));
}

function ChatChartTooltip({
  active,
  payload,
  label,
  unit,
}: TooltipProps<number, string> & { unit?: string }) {
  if (!active || !payload?.length) return null;

  const renderedLabel =
    typeof label === "string" ? humanizeLabel(label) : label;

  return (
    <div className="rounded-lg border bg-background p-3 shadow-lg">
      <div className="text-xs text-muted-foreground mb-1">{renderedLabel}</div>
      <div className="space-y-1">
        {payload.map((entry) => {
          const value = typeof entry.value === "number"
            ? Number(entry.value.toFixed(2))
            : entry.value;
          const payloadRecord =
            entry.payload && typeof entry.payload === "object"
              ? (entry.payload as Record<string, unknown>)
              : null;
          const pointColor =
            typeof payloadRecord?.__cellColor === "string"
              ? payloadRecord.__cellColor
              : entry.color || "#8b5cf6";
          return (
            <div key={String(entry.dataKey)} className="flex items-center gap-2 text-xs">
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: pointColor }}
              />
              <span className="text-muted-foreground">{entry.name}:</span>
              <span className="font-semibold">
                {String(value)}
                {unit ?? ""}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function RadarTooltip({
  active,
  payload,
  label,
}: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null;

  const renderedLabel =
    typeof label === "string" ? humanizeLabel(label) : label;

  return (
    <div className="rounded-lg border bg-background p-3 shadow-lg">
      <div className="text-xs font-medium mb-1">{renderedLabel}</div>
      <div className="space-y-1">
        {payload.map((entry) => {
          const payloadRecord =
            entry.payload && typeof entry.payload === "object"
              ? (entry.payload as Record<string, unknown>)
              : null;
          const raw = payloadRecord?.raw;
          const unit = typeof payloadRecord?.unit === "string" ? payloadRecord.unit : "";
          const score = typeof entry.value === "number"
            ? Number(entry.value.toFixed(1))
            : entry.value;
          return (
            <div key={String(entry.dataKey)} className="text-xs">
              <div className="flex items-center gap-2">
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ backgroundColor: entry.color || "#6366f1" }}
                />
                <span className="font-semibold">{String(score)}/100</span>
              </div>
              {raw != null && (
                <div className="text-muted-foreground ml-4">
                  {String(raw)} ({unit})
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export const ChatChart = memo(function ChatChart({ chart }: { chart: ChatChartArtifact }) {
  if (!chart.data?.length || !chart.series?.length) return null;

  const hasSingleNumericPoint =
    chart.data.length === 1 &&
    chart.series.length === 1 &&
    typeof chart.data[0]?.[chart.series[0].key] === "number";
  const renderAsSingleValue =
    chart.chartType === "single_value" ||
    (hasSingleNumericPoint &&
      (chart.chartType === "bar" ||
        chart.chartType === "grouped_bar" ||
        chart.chartType === "histogram"));

  const sourceLabel = formatSourceLabel(chart.source);
  const singleValueData = renderAsSingleValue
    ? chart.data.map((row) => {
        const value = Number(row?.[chart.series[0].key]);
        return { ...row, __cellColor: value >= 0 ? "#22c55e" : "#ef4444" };
      })
    : chart.data;
  const chartNode = renderAsSingleValue ? (
    <BarChart data={singleValueData} layout="vertical" margin={{ top: 8, right: 16, left: 16, bottom: 8 }}>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        type="number"
        domain={chart.yDomain || [-1, 1]}
        tick={{ fontSize: 11 }}
        tickFormatter={formatNumericTick}
      />
      <YAxis
        type="category"
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
        width={170}
      />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      <ReferenceLine x={0} stroke="hsl(var(--muted-foreground))" strokeOpacity={0.4} />
      <Bar
        dataKey={chart.series[0].key}
        name={chart.series[0].label}
        radius={[4, 4, 4, 4]}
        barSize={28}
      >
        {singleValueData.map((row, idx) => {
          const fill =
            typeof row.__cellColor === "string" ? row.__cellColor : "#6366f1";
          return <Cell key={`single-value-cell-${idx}`} fill={fill} />;
        })}
      </Bar>
    </BarChart>
  ) : chart.chartType === "line" || chart.chartType === "multi_line" ? (
    <LineChart data={chart.data}>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
      />
      <YAxis tick={{ fontSize: 11 }} domain={chart.yDomain} width={48} tickFormatter={formatNumericTick} />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      {chart.series.map((series) => (
        <Line
          key={series.key}
          type="monotone"
          dataKey={series.key}
          name={series.label}
          stroke={series.color || "#8b5cf6"}
          strokeWidth={2}
          dot={{ r: 2, fill: series.color || "#8b5cf6" }}
          connectNulls={true}
        />
      ))}
    </LineChart>
  ) : chart.chartType === "area" || chart.chartType === "stacked_area" ? (
    <AreaChart data={chart.data}>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
      />
      <YAxis tick={{ fontSize: 11 }} domain={chart.yDomain} width={48} tickFormatter={formatNumericTick} />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      {chart.series.map((series) => (
        <Area
          key={series.key}
          type="monotone"
          dataKey={series.key}
          name={series.label}
          stroke={series.color || "#8b5cf6"}
          fill={series.color || "#8b5cf6"}
          fillOpacity={0.25}
          stackId={chart.chartType === "stacked_area" ? "stacked-area" : undefined}
          isAnimationActive={false}
          connectNulls={true}
        />
      ))}
    </AreaChart>
  ) : chart.chartType === "bar" || chart.chartType === "grouped_bar" || chart.chartType === "histogram" ? (
    <BarChart data={chart.data}>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
        interval={0}
        angle={-35}
        textAnchor="end"
        height={70}
      />
      <YAxis tick={{ fontSize: 11 }} domain={chart.yDomain} width={48} tickFormatter={formatNumericTick} />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      {chart.series.map((series) => (
        <Bar
          key={series.key}
          dataKey={series.key}
          name={series.label}
          fill={series.color || "#6366f1"}
          radius={[3, 3, 0, 0]}
        />
      ))}
    </BarChart>
  ) : chart.chartType === "scatter" ? (
    <ScatterChart>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        type="category"
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
      />
      <YAxis
        type="number"
        dataKey={chart.series[0].key}
        tick={{ fontSize: 11 }}
        domain={chart.yDomain}
        width={48}
        tickFormatter={formatNumericTick}
      />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      {chart.series.map((series) => (
        <Scatter
          key={series.key}
          data={chart.data}
          dataKey={series.key}
          name={series.label}
          fill={series.color || "#f97316"}
        />
      ))}
    </ScatterChart>
  ) : chart.chartType === "radar" ? (
    <RadarChart data={chart.data} cx="50%" cy="50%" outerRadius="75%">
      <PolarGrid gridType="circle" stroke="#444" />
      <PolarAngleAxis
        dataKey={chart.xKey}
        tick={{ fill: "#aaa", fontSize: 13 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
      />
      <PolarRadiusAxis
        angle={30}
        domain={[0, 100]}
        tick={{ fill: "#666", fontSize: 11 }}
        tickFormatter={formatNumericTick}
      />
      <Tooltip content={<RadarTooltip />} cursor={false} />
      {chart.series.map((series) => (
        <Radar
          key={series.key}
          dataKey={series.key}
          name={series.label}
          stroke={series.color || "#6366f1"}
          strokeWidth={2}
          fill={series.color || "#6366f1"}
          fillOpacity={0.35}
          dot={{ r: 3, fill: "#fff", strokeWidth: 2, stroke: series.color || "#6366f1" }}
        />
      ))}
    </RadarChart>
  ) : chart.chartType === "scatter_xy" ? (
    <ScatterChart>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        type="number"
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        name={chart.xAxisLabel}
        tickFormatter={formatNumericTick}
      />
      <YAxis
        type="number"
        dataKey={chart.yKey || chart.series[0].key}
        tick={{ fontSize: 11 }}
        name={chart.yAxisLabel || chart.series[0].label}
        domain={chart.yDomain}
        width={48}
        tickFormatter={formatNumericTick}
      />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      {chart.series.map((series) => (
        <Scatter
          key={series.key}
          data={chart.data}
          name={series.label}
          fill={series.color || "#22c55e"}
          line={false}
        />
      ))}
    </ScatterChart>
  ) : chart.chartType === "stacked_bar" ? (
    <BarChart data={chart.data}>
      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
      <XAxis
        dataKey={chart.xKey}
        tick={{ fontSize: 11 }}
        tickFormatter={(value) => (typeof value === "string" ? humanizeLabel(value) : String(value))}
      />
      <YAxis
        tick={{ fontSize: 11 }}
        domain={chart.yDomain || [0, 100]}
        width={48}
        tickFormatter={formatNumericTick}
        allowDecimals={false}
      />
      <Tooltip content={<ChatChartTooltip unit={chart.unit} />} cursor={false} />
      {chart.series.map((series) => (
        <Bar
          key={series.key}
          stackId="sleep-stages"
          dataKey={series.key}
          name={series.label}
          fill={series.color || "#8b5cf6"}
        />
      ))}
    </BarChart>
  ) : null;

  if (!chartNode) return null;

  return (
    <div className="mt-3 rounded-lg border bg-background/50 p-3">
      <div className="mb-2">
        <p className="text-sm font-medium">{chart.title}</p>
        {(chart.dateRange || chart.source) && (
          <p className="text-xs text-muted-foreground">
            {chart.dateRange ? formatDateRangeLabel(chart.dateRange) : ""}
            {chart.dateRange && chart.source ? " · " : ""}
            {chart.source ? `Source: ${sourceLabel}` : ""}
          </p>
        )}
      </div>
      <div className="h-64 w-full">
        <ResponsiveContainer width="100%" height="100%">
          {chartNode}
        </ResponsiveContainer>
      </div>
    </div>
  );
});
