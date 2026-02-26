"use client";

import Link from "next/link";
import { useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
  ComposedChart,
  Scatter,
  ScatterChart,
  ZAxis,
} from "recharts";

import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Settings } from "lucide-react";
import { useRouter } from "next/navigation";

const AVAILABLE_METRICS = [
  { value: "readiness_score", label: "Readiness Score" },
  { value: "sleep_score", label: "Sleep Score" },
  { value: "activity_score", label: "Activity Score" },
  { value: "steps", label: "Steps" },
  { value: "hrv_average", label: "HRV Average" },
  { value: "hr_lowest", label: "Resting Heart Rate" },
  { value: "sleep_total_seconds", label: "Sleep Duration" },
  { value: "sleep_efficiency", label: "Sleep Efficiency" },
  { value: "sleep_deep_seconds", label: "Deep Sleep" },
  { value: "sleep_rem_seconds", label: "REM Sleep" },
  { value: "cal_total", label: "Total Calories" },
  { value: "cal_active", label: "Active Calories" },
];

interface SpearmanCorrelation {
  metric: string;
  rho: number;
  p_value: number;
  n: number;
}

interface LaggedCorrelation {
  lag: number;
  rho: number;
  p_value: number;
  n: number;
}

interface ControlledResult {
  metric_x: string;
  metric_y: string;
  rho: number;
  p_value: number;
  n: number;
  controlled_for: string[];
}

interface MatrixData {
  metrics: string[];
  matrix: number[][];
  p_values: number[][];
  n_matrix: number[][];
}

interface ScatterPointData {
  x: number;
  y: number;
  date: string;
}

interface ScatterData {
  metric_x: string;
  metric_y: string;
  points: ScatterPointData[];
  n: number;
}

type SpearmanView = "lollipop" | "diverging" | "heatmap" | "scatter";

function getCorrelationColor(rho: number): string {
  // Stops: -1.0 → #7c3aed, -0.5 → #a78bfa, 0.0 → #9ca3af, +0.5 → #5eead4, +1.0 → #14b8a6
  const stops: [number, [number, number, number]][] = [
    [-1.0, [124, 58, 237]],
    [-0.5, [167, 139, 250]],
    [0.0, [156, 163, 175]],
    [0.5, [94, 234, 212]],
    [1.0, [20, 184, 166]],
  ];

  const clamped = Math.max(-1, Math.min(1, rho));

  for (let i = 0; i < stops.length - 1; i++) {
    const [t0, c0] = stops[i];
    const [t1, c1] = stops[i + 1];
    if (clamped >= t0 && clamped <= t1) {
      const t = (clamped - t0) / (t1 - t0);
      const r = Math.round(c0[0] + t * (c1[0] - c0[0]));
      const g = Math.round(c0[1] + t * (c1[1] - c0[1]));
      const b = Math.round(c0[2] + t * (c1[2] - c0[2]));
      return `rgb(${r}, ${g}, ${b})`;
    }
  }
  return "#9ca3af";
}

export default function CorrelationsPage() {
  const router = useRouter();
  // Spearman state
  const [spearmanTarget, setSpearmanTarget] = useState("readiness_score");
  const [spearmanCandidates, setSpearmanCandidates] = useState<string[]>([
    "sleep_score",
    "hrv_average",
    "steps",
    "sleep_total_seconds",
  ]);
  const [spearmanResults, setSpearmanResults] = useState<SpearmanCorrelation[] | null>(null);
  const [spearmanLoading, setSpearmanLoading] = useState(false);
  const [spearmanView, setSpearmanView] = useState<SpearmanView>("lollipop");

  // Matrix state (heatmap)
  const [matrixData, setMatrixData] = useState<MatrixData | null>(null);
  const [matrixLoading, setMatrixLoading] = useState(false);

  // Scatter state
  const [scatterData, setScatterData] = useState<ScatterData | null>(null);
  const [scatterLoading, setScatterLoading] = useState(false);
  const [selectedScatterMetric, setSelectedScatterMetric] = useState<string | null>(null);

  // Lagged state
  const [laggedX, setLaggedX] = useState("steps");
  const [laggedY, setLaggedY] = useState("sleep_score");
  const [maxLag, setMaxLag] = useState(7);
  const [laggedResults, setLaggedResults] = useState<{
    lags: LaggedCorrelation[];
    best_lag: number;
  } | null>(null);
  const [laggedLoading, setLaggedLoading] = useState(false);

  // Controlled state
  const [controlledX, setControlledX] = useState("hrv_average");
  const [controlledY, setControlledY] = useState("readiness_score");
  const [controlVars, setControlVars] = useState<string[]>(["sleep_total_seconds"]);
  const [controlledResult, setControlledResult] = useState<ControlledResult | null>(null);
  const [controlledLoading, setControlledLoading] = useState(false);

  const [error, setError] = useState<string | null>(null);

  function getCsrfToken(): string {
    const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  const runSpearman = async () => {
    setSpearmanLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.append("target", spearmanTarget);
      spearmanCandidates.forEach((c) => params.append("candidates", c));

      const response = await fetch(`/api/analytics/analyze/correlations/spearman?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to compute correlations");
      const data = await response.json();
      setSpearmanResults(data.correlations);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setSpearmanLoading(false);
    }
  };

  const fetchMatrix = async () => {
    setMatrixLoading(true);
    setError(null);
    try {
      const allMetrics = [spearmanTarget, ...spearmanCandidates];
      const params = new URLSearchParams();
      allMetrics.forEach((m) => params.append("metrics", m));

      const response = await fetch(`/api/analytics/analyze/correlations/matrix?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to fetch correlation matrix");
      const data = await response.json();
      setMatrixData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setMatrixLoading(false);
    }
  };

  const fetchScatterData = async (metric: string) => {
    setScatterLoading(true);
    setSelectedScatterMetric(metric);
    setError(null);
    try {
      const params = new URLSearchParams({
        metric_x: metric,
        metric_y: spearmanTarget,
      });

      const response = await fetch(`/api/analytics/analyze/correlations/scatter-data?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to fetch scatter data");
      const data = await response.json();
      setScatterData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setScatterLoading(false);
    }
  };

  const handleViewChange = (view: SpearmanView) => {
    setSpearmanView(view);
    if (view === "heatmap" && !matrixData) {
      fetchMatrix();
    }
    if (view === "scatter" && !selectedScatterMetric && spearmanResults && spearmanResults.length > 0) {
      fetchScatterData(spearmanResults[0].metric);
    }
  };

  const runLagged = async () => {
    setLaggedLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        metric_x: laggedX,
        metric_y: laggedY,
        max_lag: maxLag.toString(),
      });

      const response = await fetch(`/api/analytics/analyze/correlations/lagged?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to compute lagged correlations");
      const data = await response.json();
      setLaggedResults({ lags: data.lags, best_lag: data.best_lag });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLaggedLoading(false);
    }
  };

  const runControlled = async () => {
    setControlledLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.append("metric_x", controlledX);
      params.append("metric_y", controlledY);
      controlVars.forEach((v) => params.append("control_vars", v));

      const response = await fetch(`/api/analytics/analyze/correlations/controlled?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to compute controlled correlation");
      const data = await response.json();
      setControlledResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setControlledLoading(false);
    }
  };

  const toggleCandidate = (metric: string) => {
    setSpearmanCandidates((prev) =>
      prev.includes(metric) ? prev.filter((m) => m !== metric) : [...prev, metric]
    );
  };

  const toggleControlVar = (metric: string) => {
    setControlVars((prev) =>
      prev.includes(metric) ? prev.filter((m) => m !== metric) : [...prev, metric]
    );
  };

  const getMetricLabel = (value: string) =>
    AVAILABLE_METRICS.find((m) => m.value === value)?.label || value;

  const formatPValue = (p: number) => {
    if (p < 0.001) return "< 0.001";
    return p.toFixed(3);
  };

  // Compute linear regression for scatter trend line
  const computeTrendLine = (points: ScatterPointData[]) => {
    if (points.length < 2) return null;
    const n = points.length;
    const sumX = points.reduce((s, p) => s + p.x, 0);
    const sumY = points.reduce((s, p) => s + p.y, 0);
    const sumXY = points.reduce((s, p) => s + p.x * p.y, 0);
    const sumX2 = points.reduce((s, p) => s + p.x * p.x, 0);
    const denom = n * sumX2 - sumX * sumX;
    if (denom === 0) return null;
    const slope = (n * sumXY - sumX * sumY) / denom;
    const intercept = (sumY - slope * sumX) / n;
    const minX = Math.min(...points.map((p) => p.x));
    const maxX = Math.max(...points.map((p) => p.x));
    return [
      { x: minX, y: slope * minX + intercept },
      { x: maxX, y: slope * maxX + intercept },
    ];
  };

  // Prepare sorted data for charts
  const getSortedChartData = () => {
    if (!spearmanResults) return [];
    return spearmanResults
      .map((r) => ({
        metric: getMetricLabel(r.metric),
        rawMetric: r.metric,
        rho: r.rho,
        p_value: r.p_value,
        n: r.n,
      }))
      .sort((a, b) => Math.abs(b.rho) - Math.abs(a.rho));
  };

  const viewLabels: { key: SpearmanView; label: string }[] = [
    { key: "lollipop", label: "Lollipop" },
    { key: "diverging", label: "Diverging" },
    { key: "heatmap", label: "Heatmap" },
    { key: "scatter", label: "Scatter" },
  ];

  const renderSpearmanChart = () => {
    if (!spearmanResults) return null;

    const chartData = getSortedChartData();
    const chartHeight = Math.max(300, chartData.length * 40);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const customTooltip = ({ active, payload }: any) => {
      if (active && payload && payload.length) {
        const data = payload[0].payload;
        return (
          <div className="rounded-lg border bg-background p-3 shadow-lg">
            <div className="grid gap-2">
              <p className="text-sm font-bold">{data.metric}</p>
              <div className="space-y-1">
                <p className="text-sm">
                  <span className="font-medium">rho =</span> {data.rho.toFixed(3)}
                </p>
                <p className="text-sm">
                  <span className="font-medium">p =</span> {formatPValue(data.p_value)}
                </p>
                <p className="text-sm">
                  <span className="font-medium">n =</span> {data.n}
                </p>
              </div>
            </div>
          </div>
        );
      }
      return null;
    };

    switch (spearmanView) {
      case "lollipop":
        return (
          <div>
            <div style={{ height: chartHeight }}>
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart
                  data={chartData}
                  layout="vertical"
                  margin={{ left: 120 }}
                >
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" horizontal={false} />
                  <XAxis type="number" domain={[-1, 1]} />
                  <YAxis type="category" dataKey="metric" tick={{ fontSize: 12 }} width={110} />
                  <Tooltip content={customTooltip} />
                  <ReferenceLine x={0} stroke="#888" />
                  <Bar dataKey="rho" barSize={3}>
                    {chartData.map((entry, index) => (
                      <Cell key={index} fill={getCorrelationColor(entry.rho)} />
                    ))}
                  </Bar>
                  <Scatter dataKey="rho" fill="#8884d8" shape="circle">
                    {chartData.map((entry, index) => (
                      <Cell key={index} fill={getCorrelationColor(entry.rho)} r={5} />
                    ))}
                  </Scatter>
                </ComposedChart>
              </ResponsiveContainer>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              Purple = negative, Teal = positive. Dot size shows strength.
            </p>
          </div>
        );

      case "diverging":
        return (
          <div>
            <div style={{ height: chartHeight }}>
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={[...chartData].sort((a, b) => a.rho - b.rho)}
                  layout="vertical"
                  margin={{ left: 120 }}
                >
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" horizontal={false} />
                  <XAxis type="number" domain={[-1, 1]} />
                  <YAxis type="category" dataKey="metric" tick={{ fontSize: 12 }} width={110} />
                  <Tooltip content={customTooltip} />
                  <ReferenceLine x={0} stroke="#888" />
                  <Bar dataKey="rho" name="Correlation">
                    {[...chartData].sort((a, b) => a.rho - b.rho).map((entry, index) => (
                      <Cell key={index} fill={getCorrelationColor(entry.rho)} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              Sorted by correlation value. Purple = negative, Teal = positive.
            </p>
          </div>
        );

      case "heatmap":
        return renderHeatmap();

      case "scatter":
        return renderScatterView();
    }
  };

  const renderHeatmap = () => {
    if (matrixLoading) {
      return <p className="text-sm text-muted-foreground">Loading matrix...</p>;
    }
    if (!matrixData || matrixData.metrics.length === 0) {
      return <p className="text-sm text-muted-foreground">No matrix data. Switch to heatmap view to load.</p>;
    }

    const { metrics, matrix, p_values, n_matrix } = matrixData;
    const size = metrics.length;

    const cellSize = 48;

    return (
      <div>
        <div className="overflow-x-auto">
          <div
            className="inline-grid gap-[2px]"
            style={{
              gridTemplateColumns: `100px repeat(${size}, ${cellSize}px)`,
            }}
          >
            {/* Header row */}
            <div />
            {metrics.map((m) => (
              <div
                key={`h-${m}`}
                className="text-[10px] font-medium text-center truncate px-1"
                style={{ writingMode: "vertical-rl", transform: "rotate(180deg)", height: 80 }}
              >
                {getMetricLabel(m)}
              </div>
            ))}

            {/* Data rows */}
            {metrics.flatMap((rowMetric, i) => [
              <div key={`l-${rowMetric}`} className="text-xs font-medium flex items-center truncate pr-2">
                {getMetricLabel(rowMetric)}
              </div>,
              ...metrics.map((_, j) => {
                const rho = matrix[i][j];
                const p = p_values[i][j];
                const n = n_matrix[i][j];
                return (
                  <div
                    key={`c-${i}-${j}`}
                    className="flex items-center justify-center text-[10px] font-medium rounded-sm cursor-default"
                    style={{
                      backgroundColor: getCorrelationColor(rho),
                      color: Math.abs(rho) > 0.4 ? "white" : "inherit",
                      width: cellSize,
                      height: cellSize,
                    }}
                    title={`${getMetricLabel(metrics[i])} vs ${getMetricLabel(metrics[j])}\nrho = ${rho.toFixed(3)}\np = ${formatPValue(p)}\nn = ${n}`}
                  >
                    {rho.toFixed(2)}
                  </div>
                );
              }),
            ])}
          </div>
        </div>
        <p className="text-xs text-muted-foreground mt-2">
          Hover cells for details. Purple = negative, Grey = near zero, Teal = positive.
        </p>
      </div>
    );
  };

  const renderScatterView = () => {
    if (!spearmanResults) return null;

    const sortedMetrics = [...spearmanResults].sort(
      (a, b) => Math.abs(b.rho) - Math.abs(a.rho)
    );

    return (
      <div className="grid grid-cols-3 gap-4">
        {/* Left column: metric list */}
        <div className="col-span-1 space-y-1 max-h-[400px] overflow-y-auto">
          <p className="text-xs font-medium text-muted-foreground mb-2">Select metric</p>
          {sortedMetrics.map((r) => (
            <button
              key={r.metric}
              onClick={() => fetchScatterData(r.metric)}
              className={`w-full text-left px-3 py-2 rounded text-sm transition-colors ${
                selectedScatterMetric === r.metric
                  ? "bg-primary text-primary-foreground"
                  : "hover:bg-muted"
              }`}
            >
              <div className="flex justify-between items-center">
                <span className="truncate">{getMetricLabel(r.metric)}</span>
                <span
                  className="text-xs font-mono ml-2 shrink-0"
                  style={{ color: selectedScatterMetric === r.metric ? "inherit" : getCorrelationColor(r.rho) }}
                >
                  {r.rho.toFixed(2)}
                </span>
              </div>
            </button>
          ))}
        </div>

        {/* Right column: scatter plot */}
        <div className="col-span-2">
          {scatterLoading && (
            <p className="text-sm text-muted-foreground">Loading scatter data...</p>
          )}
          {!scatterLoading && scatterData && scatterData.points.length > 0 && (
            <div>
              <p className="text-sm font-medium mb-2">
                {getMetricLabel(scatterData.metric_x)} vs {getMetricLabel(scatterData.metric_y)}
                <span className="text-xs text-muted-foreground ml-2">({scatterData.n} points)</span>
              </p>
              <div className="h-[350px]">
                <ResponsiveContainer width="100%" height="100%">
                  <ScatterChart margin={{ bottom: 20, left: 10, right: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                    <XAxis
                      type="number"
                      dataKey="x"
                      name={getMetricLabel(scatterData.metric_x)}
                      label={{ value: getMetricLabel(scatterData.metric_x), position: "bottom", offset: 0 }}
                      tick={{ fontSize: 11 }}
                    />
                    <YAxis
                      type="number"
                      dataKey="y"
                      name={getMetricLabel(scatterData.metric_y)}
                      tick={{ fontSize: 11 }}
                    />
                    <ZAxis range={[20, 20]} />
                    <Tooltip
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          const p = payload[0].payload as ScatterPointData;
                          return (
                            <div className="rounded-lg border bg-background p-3 shadow-lg">
                              <p className="text-xs text-muted-foreground">{p.date}</p>
                              <p className="text-sm">
                                {getMetricLabel(scatterData.metric_x)}: {p.x.toFixed(1)}
                              </p>
                              <p className="text-sm">
                                {getMetricLabel(scatterData.metric_y)}: {p.y.toFixed(1)}
                              </p>
                            </div>
                          );
                        }
                        return null;
                      }}
                    />
                    <Scatter data={scatterData.points} fill="#7c3aed" fillOpacity={0.5} />
                    {(() => {
                      const trendLine = computeTrendLine(scatterData.points);
                      if (!trendLine) return null;
                      return (
                        <Scatter
                          data={trendLine}
                          fill="none"
                          line={{ stroke: "#14b8a6", strokeWidth: 2 }}
                          lineType="joint"
                          shape={(props: Record<string, unknown>) => (
                            <circle cx={props.cx as number} cy={props.cy as number} r={0} />
                          )}
                        />
                      );
                    })()}
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
          {!scatterLoading && scatterData && scatterData.points.length === 0 && (
            <p className="text-sm text-muted-foreground">No overlapping data for these metrics.</p>
          )}
          {!scatterLoading && !scatterData && (
            <p className="text-sm text-muted-foreground">Select a metric to view scatter plot.</p>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="container mx-auto py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold">Correlations</h1>
        <div className="flex items-center gap-2">
          <Select value="correlations" onValueChange={(value) => router.push(`/${value}`)}>
            <SelectTrigger className="w-[135px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="dashboard">Dashboard</SelectItem>
              <SelectItem value="correlations">Correlations</SelectItem>
              <SelectItem value="patterns">Patterns</SelectItem>
              <SelectItem value="insights">Insights</SelectItem>
              <SelectItem value="chat">Chat</SelectItem>
            </SelectContent>
          </Select>
          <ThemeToggle />
          <Link href="/settings">
            <Button variant="outline" size="icon">
              <Settings className="h-4 w-4" />
            </Button>
          </Link>
        </div>
      </div>

      {error && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
          {error}
        </div>
      )}

      <Tabs defaultValue="spearman" className="w-full">
        <TabsList className="grid w-full grid-cols-3 mb-4">
          <TabsTrigger value="spearman">Spearman</TabsTrigger>
          <TabsTrigger value="lagged">Lagged</TabsTrigger>
          <TabsTrigger value="controlled">Controlled</TabsTrigger>
        </TabsList>

        {/* Spearman Correlations */}
        <TabsContent value="spearman">
          <Card>
            <CardHeader>
              <CardTitle>Spearman Rank Correlations</CardTitle>
              <CardDescription>
                Find which metrics are most correlated with your target metric
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-2">Target Metric</label>
                <select
                  value={spearmanTarget}
                  onChange={(e) => setSpearmanTarget(e.target.value)}
                  className="w-full border rounded px-3 py-2 bg-background"
                >
                  {AVAILABLE_METRICS.map((m) => (
                    <option key={m.value} value={m.value}>
                      {m.label}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">Candidate Metrics</label>
                <div className="flex flex-wrap gap-2">
                  {AVAILABLE_METRICS.filter((m) => m.value !== spearmanTarget).map((m) => (
                    <Badge
                      key={m.value}
                      variant={spearmanCandidates.includes(m.value) ? "default" : "outline"}
                      className="cursor-pointer"
                      onClick={() => toggleCandidate(m.value)}
                    >
                      {m.label}
                    </Badge>
                  ))}
                </div>
              </div>

              <Button onClick={runSpearman} disabled={spearmanLoading || spearmanCandidates.length === 0}>
                {spearmanLoading ? "Computing..." : "Compute Correlations"}
              </Button>

              {spearmanResults && (
                <div className="mt-6">
                  <div className="flex items-center justify-between mb-4">
                    <h4 className="text-sm font-medium">
                      Correlations with {getMetricLabel(spearmanTarget)}
                    </h4>
                    <div className="flex gap-1">
                      {viewLabels.map((v) => (
                        <Button
                          key={v.key}
                          variant={spearmanView === v.key ? "default" : "outline"}
                          size="sm"
                          className="text-xs h-7 px-2"
                          onClick={() => handleViewChange(v.key)}
                        >
                          {v.label}
                        </Button>
                      ))}
                    </div>
                  </div>

                  {renderSpearmanChart()}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Lagged Correlations */}
        <TabsContent value="lagged">
          <Card>
            <CardHeader>
              <CardTitle>Lagged Correlations</CardTitle>
              <CardDescription>
                Find if metric X predicts metric Y days later (e.g., does exercise today predict better sleep tomorrow?)
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-2">Predictor (X)</label>
                  <select
                    value={laggedX}
                    onChange={(e) => setLaggedX(e.target.value)}
                    className="w-full border rounded px-3 py-2 bg-background"
                  >
                    {AVAILABLE_METRICS.map((m) => (
                      <option key={m.value} value={m.value}>
                        {m.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-2">Target (Y)</label>
                  <select
                    value={laggedY}
                    onChange={(e) => setLaggedY(e.target.value)}
                    className="w-full border rounded px-3 py-2 bg-background"
                  >
                    {AVAILABLE_METRICS.map((m) => (
                      <option key={m.value} value={m.value}>
                        {m.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">Max Lag (days)</label>
                <input
                  type="number"
                  value={maxLag}
                  onChange={(e) => setMaxLag(parseInt(e.target.value) || 7)}
                  min={1}
                  max={30}
                  className="w-24 border rounded px-3 py-2 bg-background"
                />
              </div>

              <Button onClick={runLagged} disabled={laggedLoading}>
                {laggedLoading ? "Computing..." : "Compute Lagged Correlations"}
              </Button>

              {laggedResults && (
                <div className="mt-6">
                  <div className="flex items-center gap-2 mb-4">
                    <h4 className="text-sm font-medium">
                      {getMetricLabel(laggedX)} → {getMetricLabel(laggedY)}
                    </h4>
                    <Badge variant="secondary">Best lag: {laggedResults.best_lag} days</Badge>
                  </div>
                  <div className="h-[250px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={laggedResults.lags}>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                        <XAxis
                          dataKey="lag"
                          label={{ value: "Lag (days)", position: "bottom", offset: -5 }}
                        />
                        <YAxis domain={[-1, 1]} />
                        <Tooltip
                          content={({ active, payload }) => {
                            if (active && payload && payload.length) {
                              const data = payload[0].payload;
                              return (
                                <div className="rounded-lg border bg-background p-3 shadow-lg">
                                  <div className="grid gap-2">
                                    <p className="text-sm font-bold">Lag: {data.lag} days</p>
                                    <div className="space-y-1">
                                      <p className="text-sm">
                                        <span className="font-medium">rho =</span> {data.rho.toFixed(3)}
                                      </p>
                                      <p className="text-sm">
                                        <span className="font-medium">p =</span> {formatPValue(data.p_value)}
                                      </p>
                                      <p className="text-sm">
                                        <span className="font-medium">n =</span> {data.n}
                                      </p>
                                    </div>
                                  </div>
                                </div>
                              );
                            }
                            return null;
                          }}
                        />
                        <ReferenceLine y={0} stroke="#888" />
                        <Bar dataKey="rho" name="Correlation">
                          {laggedResults.lags.map((entry, index) => (
                            <Cell
                              key={index}
                              fill={
                                entry.lag === laggedResults.best_lag
                                  ? "#2563eb"
                                  : getCorrelationColor(entry.rho)
                              }
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <p className="text-xs text-muted-foreground mt-2">
                    Lag 0 = same day. Lag 1 = X predicts Y the next day. Blue bar = strongest correlation.
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Controlled Correlations */}
        <TabsContent value="controlled">
          <Card>
            <CardHeader>
              <CardTitle>Controlled (Partial) Correlations</CardTitle>
              <CardDescription>
                Find the true correlation between X and Y while controlling for confounding variables
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-2">Metric X</label>
                  <select
                    value={controlledX}
                    onChange={(e) => setControlledX(e.target.value)}
                    className="w-full border rounded px-3 py-2 bg-background"
                  >
                    {AVAILABLE_METRICS.map((m) => (
                      <option key={m.value} value={m.value}>
                        {m.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-2">Metric Y</label>
                  <select
                    value={controlledY}
                    onChange={(e) => setControlledY(e.target.value)}
                    className="w-full border rounded px-3 py-2 bg-background"
                  >
                    {AVAILABLE_METRICS.map((m) => (
                      <option key={m.value} value={m.value}>
                        {m.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">Control Variables</label>
                <div className="flex flex-wrap gap-2">
                  {AVAILABLE_METRICS.filter(
                    (m) => m.value !== controlledX && m.value !== controlledY
                  ).map((m) => (
                    <Badge
                      key={m.value}
                      variant={controlVars.includes(m.value) ? "default" : "outline"}
                      className="cursor-pointer"
                      onClick={() => toggleControlVar(m.value)}
                    >
                      {m.label}
                    </Badge>
                  ))}
                </div>
              </div>

              <Button onClick={runControlled} disabled={controlledLoading || controlVars.length === 0}>
                {controlledLoading ? "Computing..." : "Compute Partial Correlation"}
              </Button>

              {controlledResult && (
                <div className="mt-6 p-4 border rounded-lg">
                  <h4 className="text-lg font-medium mb-4">Result</h4>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <p className="text-sm text-muted-foreground">Correlation (rho)</p>
                      <p
                        className="text-2xl font-bold"
                        style={{ color: getCorrelationColor(controlledResult.rho) }}
                      >
                        {controlledResult.rho.toFixed(3)}
                      </p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">P-value</p>
                      <p className="text-2xl font-bold">
                        {formatPValue(controlledResult.p_value)}
                      </p>
                    </div>
                  </div>
                  <div className="mt-4">
                    <p className="text-sm text-muted-foreground">Relationship</p>
                    <p className="font-medium">
                      {getMetricLabel(controlledResult.metric_x)} ↔{" "}
                      {getMetricLabel(controlledResult.metric_y)}
                    </p>
                    <p className="text-sm text-muted-foreground mt-2">Controlling for:</p>
                    <div className="flex flex-wrap gap-1 mt-1">
                      {controlledResult.controlled_for.map((v) => (
                        <Badge key={v} variant="secondary">
                          {getMetricLabel(v)}
                        </Badge>
                      ))}
                    </div>
                  </div>
                  <p className="text-xs text-muted-foreground mt-4">
                    Sample size: {controlledResult.n} days.{" "}
                    {controlledResult.p_value < 0.05
                      ? "Statistically significant (p < 0.05)"
                      : "Not statistically significant (p >= 0.05)"}
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
