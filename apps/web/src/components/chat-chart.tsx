"use client";

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart,
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

function formatDateRangeLabel(dateRange?: string): string {
  if (!dateRange) return "";
  const trimmed = dateRange.trim();
  if (!trimmed) return "";
  return trimmed.charAt(0).toUpperCase() + trimmed.slice(1);
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

  return (
    <div className="rounded-lg border bg-background p-3 shadow-lg">
      <div className="text-xs text-muted-foreground mb-1">{label}</div>
      <div className="space-y-1">
        {payload.map((entry) => {
          const value = typeof entry.value === "number"
            ? Number(entry.value.toFixed(2))
            : entry.value;
          return (
            <div key={String(entry.dataKey)} className="flex items-center gap-2 text-xs">
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: entry.color || "#8b5cf6" }}
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

export function ChatChart({ chart }: { chart: ChatChartArtifact }) {
  if (!chart.data?.length || !chart.series?.length) return null;

  return (
    <div className="mt-3 rounded-lg border bg-background/50 p-3">
      <div className="mb-2">
        <p className="text-sm font-medium">{chart.title}</p>
        {(chart.dateRange || chart.source) && (
          <p className="text-xs text-muted-foreground">
            {chart.dateRange ? formatDateRangeLabel(chart.dateRange) : ""}
            {chart.dateRange && chart.source ? " · " : ""}
            {chart.source ? `Source: ${chart.source}` : ""}
          </p>
        )}
      </div>
      <div className="h-64 w-full">
        <ResponsiveContainer width="100%" height="100%">
          {chart.chartType === "line" || chart.chartType === "multi_line" ? (
            <LineChart data={chart.data}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey={chart.xKey} tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} domain={chart.yDomain} width={48} tickFormatter={formatNumericTick} />
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
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
          ) : null}

          {chart.chartType === "area" || chart.chartType === "stacked_area" ? (
            <AreaChart data={chart.data}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey={chart.xKey} tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} domain={chart.yDomain} width={48} tickFormatter={formatNumericTick} />
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
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
          ) : null}

          {chart.chartType === "bar" || chart.chartType === "grouped_bar" || chart.chartType === "histogram" ? (
            <BarChart data={chart.data}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey={chart.xKey} tick={{ fontSize: 11 }} interval={0} angle={-35} textAnchor="end" height={70} />
              <YAxis tick={{ fontSize: 11 }} domain={chart.yDomain} width={48} tickFormatter={formatNumericTick} />
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
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
          ) : null}

          {chart.chartType === "scatter" ? (
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis type="category" dataKey={chart.xKey} tick={{ fontSize: 11 }} />
              <YAxis
                type="number"
                dataKey={chart.series[0].key}
                tick={{ fontSize: 11 }}
                domain={chart.yDomain}
                width={48}
                tickFormatter={formatNumericTick}
              />
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
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
          ) : null}

          {chart.chartType === "radar" ? (
            <RadarChart data={chart.data}>
              <PolarGrid />
              <PolarAngleAxis dataKey={chart.xKey} />
              <PolarRadiusAxis tickFormatter={formatNumericTick} />
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
              {chart.series.map((series) => (
                <Radar
                  key={series.key}
                  dataKey={series.key}
                  name={series.label}
                  stroke={series.color || "#6366f1"}
                  fill={series.color || "#6366f1"}
                  fillOpacity={0.25}
                />
              ))}
            </RadarChart>
          ) : null}

          {chart.chartType === "scatter_xy" ? (
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
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
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
          ) : null}

          {chart.chartType === "stacked_bar" ? (
            <BarChart data={chart.data}>
              <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
              <XAxis dataKey={chart.xKey} tick={{ fontSize: 11 }} />
              <YAxis
                tick={{ fontSize: 11 }}
                domain={chart.yDomain || [0, 100]}
                width={48}
                tickFormatter={formatNumericTick}
                allowDecimals={false}
              />
              <Tooltip content={<ChatChartTooltip unit={chart.unit} />} />
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
          ) : null}
        </ResponsiveContainer>
      </div>
    </div>
  );
}
