"use client";

import Link from "next/link";
import { useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  ScatterChart,
  Scatter,
  Cell,
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

interface ChangePoint {
  date: string;
  index: number;
  before_mean: number;
  after_mean: number;
  magnitude: number;
  direction: "increase" | "decrease";
}

interface Anomaly {
  date: string;
  value: number;
  z_score: number;
  direction: "high" | "low";
}

interface WeeklyCluster {
  year: number;
  week: number;
  cluster: number;
  label: string | null;
}

const CLUSTER_COLORS = ["#2563eb", "#16a34a", "#ea580c", "#8b5cf6", "#dc2626", "#0891b2"];

export default function PatternsPage() {
  const router = useRouter();
  // Change points state
  const [cpMetric, setCpMetric] = useState("readiness_score");
  const [cpPenalty, setCpPenalty] = useState(10);
  const [cpResults, setCpResults] = useState<ChangePoint[] | null>(null);
  const [cpLoading, setCpLoading] = useState(false);

  // Anomalies state
  const [anomalyMetric, setAnomalyMetric] = useState("hrv_average");
  const [anomalyThreshold, setAnomalyThreshold] = useState(3.0);
  const [anomalyResults, setAnomalyResults] = useState<Anomaly[] | null>(null);
  const [anomalyLoading, setAnomalyLoading] = useState(false);

  // Weekly clusters state
  const [clusterFeatures, setClusterFeatures] = useState<string[]>([
    "readiness_score",
    "sleep_score",
    "activity_score",
    "steps",
  ]);
  const [nClusters, setNClusters] = useState(4);
  const [clusterResults, setClusterResults] = useState<{
    weeks: WeeklyCluster[];
    cluster_profiles: Record<string, Record<string, number>>;
  } | null>(null);
  const [clusterLoading, setClusterLoading] = useState(false);

  const [error, setError] = useState<string | null>(null);

  function getCsrfToken(): string {
    const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  const runChangePoints = async () => {
    setCpLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        metric: cpMetric,
        penalty: cpPenalty.toString(),
      });

      const response = await fetch(`/api/analytics/analyze/patterns/change-points?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to detect change points");
      const data = await response.json();
      setCpResults(data.change_points);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setCpLoading(false);
    }
  };

  const runAnomalies = async () => {
    setAnomalyLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        metric: anomalyMetric,
        threshold: anomalyThreshold.toString(),
      });

      const response = await fetch(`/api/analytics/analyze/patterns/anomalies?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to detect anomalies");
      const data = await response.json();
      setAnomalyResults(data.anomalies);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setAnomalyLoading(false);
    }
  };

  const runClusters = async () => {
    setClusterLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      clusterFeatures.forEach((f) => params.append("features", f));
      params.append("n_clusters", nClusters.toString());

      const response = await fetch(`/api/analytics/analyze/patterns/weekly-clusters?${params}`, {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
      if (!response.ok) throw new Error("Failed to compute clusters");
      const data = await response.json();
      setClusterResults({
        weeks: data.weeks,
        cluster_profiles: data.cluster_profiles,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setClusterLoading(false);
    }
  };

  const toggleClusterFeature = (metric: string) => {
    setClusterFeatures((prev) =>
      prev.includes(metric) ? prev.filter((m) => m !== metric) : [...prev, metric]
    );
  };

  const getMetricLabel = (value: string) =>
    AVAILABLE_METRICS.find((m) => m.value === value)?.label || value;

  return (
    <div className="container mx-auto py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold">Patterns</h1>
        <div className="flex items-center gap-2">
          <Select value="patterns" onValueChange={(value) => router.push(`/${value}`)}>
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

      <Tabs defaultValue="changepoints" className="w-full">
        <TabsList className="grid w-full grid-cols-3 mb-4">
          <TabsTrigger value="changepoints">Change Points</TabsTrigger>
          <TabsTrigger value="anomalies">Anomalies</TabsTrigger>
          <TabsTrigger value="clusters">Weekly Clusters</TabsTrigger>
        </TabsList>

        {/* Change Points Detection */}
        <TabsContent value="changepoints">
          <Card>
            <CardHeader>
              <CardTitle>Change Point Detection</CardTitle>
              <CardDescription>
                Detect significant shifts in your metrics over time using the PELT algorithm
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-2">Metric</label>
                  <select
                    value={cpMetric}
                    onChange={(e) => setCpMetric(e.target.value)}
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
                  <label className="block text-sm font-medium mb-2">
                    Sensitivity (lower = more points)
                  </label>
                  <input
                    type="number"
                    value={cpPenalty}
                    onChange={(e) => setCpPenalty(parseFloat(e.target.value) || 10)}
                    min={1}
                    max={50}
                    step={1}
                    className="w-full border rounded px-3 py-2 bg-background"
                  />
                </div>
              </div>

              <Button onClick={runChangePoints} disabled={cpLoading}>
                {cpLoading ? "Detecting..." : "Detect Change Points"}
              </Button>

              {cpResults && (
                <div className="mt-6">
                  <h4 className="text-sm font-medium mb-4">
                    Found {cpResults.length} change point{cpResults.length !== 1 ? "s" : ""} in{" "}
                    {getMetricLabel(cpMetric)}
                  </h4>

                  {cpResults.length > 0 ? (
                    <div className="space-y-4">
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="border-b">
                              <th className="text-left py-2 px-2">Date</th>
                              <th className="text-left py-2 px-2">Direction</th>
                              <th className="text-right py-2 px-2">Before</th>
                              <th className="text-right py-2 px-2">After</th>
                              <th className="text-right py-2 px-2">Change</th>
                            </tr>
                          </thead>
                          <tbody>
                            {cpResults.map((cp, i) => (
                              <tr key={i} className="border-b">
                                <td className="py-2 px-2">{cp.date}</td>
                                <td className="py-2 px-2">
                                  <Badge
                                    variant={cp.direction === "increase" ? "default" : "destructive"}
                                  >
                                    {cp.direction === "increase" ? "↑" : "↓"} {cp.direction}
                                  </Badge>
                                </td>
                                <td className="text-right py-2 px-2">
                                  {cp.before_mean.toFixed(1)}
                                </td>
                                <td className="text-right py-2 px-2">
                                  {cp.after_mean.toFixed(1)}
                                </td>
                                <td className="text-right py-2 px-2 font-medium">
                                  {cp.magnitude > 0 ? "+" : ""}
                                  {cp.magnitude.toFixed(1)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        Change points indicate dates where your average shifted significantly.
                        Consider what lifestyle changes occurred around these dates.
                      </p>
                    </div>
                  ) : (
                    <p className="text-muted-foreground">
                      No significant change points detected. Try lowering the sensitivity value.
                    </p>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Anomaly Detection */}
        <TabsContent value="anomalies">
          <Card>
            <CardHeader>
              <CardTitle>Anomaly Detection</CardTitle>
              <CardDescription>
                Find unusually high or low values in your metrics using z-score analysis
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-2">Metric</label>
                  <select
                    value={anomalyMetric}
                    onChange={(e) => setAnomalyMetric(e.target.value)}
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
                  <label className="block text-sm font-medium mb-2">
                    Z-score Threshold (lower = more anomalies)
                  </label>
                  <input
                    type="number"
                    value={anomalyThreshold}
                    onChange={(e) => setAnomalyThreshold(parseFloat(e.target.value) || 3)}
                    min={1}
                    max={5}
                    step={0.5}
                    className="w-full border rounded px-3 py-2 bg-background"
                  />
                </div>
              </div>

              <Button onClick={runAnomalies} disabled={anomalyLoading}>
                {anomalyLoading ? "Detecting..." : "Detect Anomalies"}
              </Button>

              {anomalyResults && (
                <div className="mt-6">
                  <h4 className="text-sm font-medium mb-4">
                    Found {anomalyResults.length} anomal{anomalyResults.length !== 1 ? "ies" : "y"}{" "}
                    in {getMetricLabel(anomalyMetric)}
                  </h4>

                  {anomalyResults.length > 0 ? (
                    <div className="space-y-4">
                      <div className="h-[250px]">
                        <ResponsiveContainer width="100%" height="100%">
                          <ScatterChart margin={{ left: 10, right: 10 }}>
                            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                            <XAxis
                              dataKey="date"
                              tick={{ fontSize: 10 }}
                              tickFormatter={(v) => v.slice(8) + '/' + v.slice(5, 7)}
                            />
                            <YAxis dataKey="value" />
                            <Tooltip
                              content={({ active, payload }) => {
                                if (active && payload && payload.length) {
                                  const data = payload[0].payload;
                                  return (
                                    <div className="rounded-lg border bg-background p-3 shadow-lg">
                                      <div className="grid gap-2">
                                        <p className="text-sm font-medium text-muted-foreground">
                                          {data.date}
                                        </p>
                                        <div className="space-y-1">
                                          <p className="text-sm">
                                            <span className="font-medium">Value:</span> {data.value.toFixed(1)}
                                          </p>
                                          <p className="text-sm">
                                            <span className="font-medium">Z-score:</span> {data.z_score.toFixed(2)}
                                          </p>
                                          <p className="text-sm">
                                            <span className="font-medium">Direction:</span>{" "}
                                            <span
                                              className={
                                                data.direction === "high"
                                                  ? "text-green-600 dark:text-green-400"
                                                  : "text-red-600 dark:text-red-400"
                                              }
                                            >
                                              {data.direction}
                                            </span>
                                          </p>
                                        </div>
                                      </div>
                                    </div>
                                  );
                                }
                                return null;
                              }}
                            />
                            <Scatter data={anomalyResults} name="Anomalies">
                              {anomalyResults.map((entry, index) => (
                                <Cell
                                  key={index}
                                  fill={entry.direction === "high" ? "#16a34a" : "#dc2626"}
                                />
                              ))}
                            </Scatter>
                          </ScatterChart>
                        </ResponsiveContainer>
                      </div>

                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead>
                            <tr className="border-b">
                              <th className="text-left py-2 px-2">Date</th>
                              <th className="text-left py-2 px-2">Direction</th>
                              <th className="text-right py-2 px-2">Value</th>
                              <th className="text-right py-2 px-2">Z-score</th>
                            </tr>
                          </thead>
                          <tbody>
                            {anomalyResults.map((a, i) => (
                              <tr key={i} className="border-b">
                                <td className="py-2 px-2">{a.date}</td>
                                <td className="py-2 px-2">
                                  <Badge
                                    variant={a.direction === "high" ? "default" : "destructive"}
                                  >
                                    {a.direction === "high" ? "↑ High" : "↓ Low"}
                                  </Badge>
                                </td>
                                <td className="text-right py-2 px-2">{a.value.toFixed(1)}</td>
                                <td className="text-right py-2 px-2">{a.z_score.toFixed(2)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>

                      <p className="text-xs text-muted-foreground">
                        Anomalies are values that deviate significantly from your normal range.
                        Green = unusually high, Red = unusually low.
                      </p>
                    </div>
                  ) : (
                    <p className="text-muted-foreground">
                      No anomalies detected. Try lowering the z-score threshold.
                    </p>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Weekly Clusters */}
        <TabsContent value="clusters">
          <Card>
            <CardHeader>
              <CardTitle>Weekly Clustering</CardTitle>
              <CardDescription>
                Group your weeks into patterns based on selected features using K-means clustering
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-2">Features for Clustering</label>
                <div className="flex flex-wrap gap-2">
                  {AVAILABLE_METRICS.map((m) => (
                    <Badge
                      key={m.value}
                      variant={clusterFeatures.includes(m.value) ? "default" : "outline"}
                      className="cursor-pointer"
                      onClick={() => toggleClusterFeature(m.value)}
                    >
                      {m.label}
                    </Badge>
                  ))}
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium mb-2">Number of Clusters</label>
                <input
                  type="number"
                  value={nClusters}
                  onChange={(e) => setNClusters(parseInt(e.target.value) || 4)}
                  min={2}
                  max={8}
                  className="w-24 border rounded px-3 py-2 bg-background"
                />
              </div>

              <Button
                onClick={runClusters}
                disabled={clusterLoading || clusterFeatures.length < 2}
              >
                {clusterLoading ? "Clustering..." : "Compute Clusters"}
              </Button>

              {clusterResults && (
                <div className="mt-6">
                  <h4 className="text-sm font-medium mb-4">
                    Clustered {clusterResults.weeks.length} weeks into {nClusters} patterns
                  </h4>

                  {/* Cluster Profiles */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                    {Object.entries(clusterResults.cluster_profiles).map(([cluster, profile]) => (
                      <div
                        key={cluster}
                        className="border rounded-lg p-3"
                        style={{ borderColor: CLUSTER_COLORS[parseInt(cluster)] }}
                      >
                        <div className="flex items-center gap-2 mb-2">
                          <div
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: CLUSTER_COLORS[parseInt(cluster)] }}
                          />
                          <span className="font-medium">Cluster {parseInt(cluster) + 1}</span>
                        </div>
                        <div className="text-xs space-y-1">
                          {Object.entries(profile).map(([metric, value]) => (
                            <div key={metric} className="flex justify-between">
                              <span className="text-muted-foreground">
                                {getMetricLabel(metric).slice(0, 12)}
                              </span>
                              <span>{value.toFixed(0)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>

                  {/* Weekly Timeline */}
                  <div>
                    <h5 className="text-sm font-medium mb-2">Weekly Timeline</h5>
                    <div className="flex flex-wrap gap-1">
                      {clusterResults.weeks.map((w, i) => (
                        <div
                          key={i}
                          className="w-6 h-6 rounded flex items-center justify-center text-xs text-white"
                          style={{ backgroundColor: CLUSTER_COLORS[w.cluster] }}
                          title={`${w.year}-W${w.week}: Cluster ${w.cluster + 1}`}
                        >
                          {w.week}
                        </div>
                      ))}
                    </div>
                    <p className="text-xs text-muted-foreground mt-2">
                      Each box represents a week. Hover to see details. Same color = similar
                      pattern.
                    </p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
