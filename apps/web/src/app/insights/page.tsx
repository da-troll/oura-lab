"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import CalendarHeatmap from "react-calendar-heatmap";
import "react-calendar-heatmap/dist/styles.css";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
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
  { value: "sleep_total_seconds", label: "Sleep Duration (hrs)" },
  { value: "sleep_efficiency", label: "Sleep Efficiency" },
  { value: "cal_total", label: "Total Calories" },
];

interface HeatmapData {
  metric: string;
  data: Array<{ date: string; value: number | null }>;
  min_value: number | null;
  max_value: number | null;
}

interface SleepArchitectureDay {
  date: string;
  deep_pct: number | null;
  rem_pct: number | null;
  light_pct: number | null;
  total_hours: number | null;
}

interface SleepArchitectureData {
  data: SleepArchitectureDay[];
  avg_deep_pct: number | null;
  avg_rem_pct: number | null;
  avg_light_pct: number | null;
}

interface ChronotypeData {
  chronotype: string;
  chronotype_label: string;
  weekend_midpoint: string | null;
  weekday_midpoint: string | null;
  social_jetlag_minutes: number | null;
  social_jetlag_label: string;
  recommendation: string | null;
}

export default function InsightsPage() {
  const router = useRouter();
  // Heatmap state
  const [heatmapMetric, setHeatmapMetric] = useState("readiness_score");
  const [heatmapData, setHeatmapData] = useState<HeatmapData | null>(null);
  const [heatmapLoading, setHeatmapLoading] = useState(false);

  // Sleep architecture state
  const [sleepArchData, setSleepArchData] = useState<SleepArchitectureData | null>(null);
  const [sleepArchLoading, setSleepArchLoading] = useState(false);
  const [sleepArchDays, setSleepArchDays] = useState(30);

  // Chronotype state
  const [chronotypeData, setChronotypeData] = useState<ChronotypeData | null>(null);
  const [chronotypeLoading, setChronotypeLoading] = useState(false);

  const [error, setError] = useState<string | null>(null);

  const apiBase = "/api/analytics";

  // Fetch heatmap data
  const fetchHeatmap = async (metric: string) => {
    setHeatmapLoading(true);
    setError(null);
    try {
      const response = await fetch(`${apiBase}/insights/heatmap?metric=${metric}&days=365`);
      if (!response.ok) throw new Error("Failed to fetch heatmap data");
      const data = await response.json();
      setHeatmapData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setHeatmapLoading(false);
    }
  };

  // Fetch sleep architecture data
  const fetchSleepArchitecture = async (days: number) => {
    setSleepArchLoading(true);
    setError(null);
    try {
      const response = await fetch(`${apiBase}/insights/sleep-architecture?days=${days}`);
      if (!response.ok) throw new Error("Failed to fetch sleep architecture data");
      const data = await response.json();
      setSleepArchData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setSleepArchLoading(false);
    }
  };

  // Fetch chronotype data
  const fetchChronotype = async () => {
    setChronotypeLoading(true);
    setError(null);
    try {
      const response = await fetch(`${apiBase}/insights/chronotype`);
      if (!response.ok) throw new Error("Failed to fetch chronotype data");
      const data = await response.json();
      setChronotypeData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setChronotypeLoading(false);
    }
  };

  // Load initial data
  useEffect(() => {
    fetchHeatmap(heatmapMetric);
    fetchSleepArchitecture(sleepArchDays);
    fetchChronotype();
  }, []);

  // Handle metric change
  const handleMetricChange = (metric: string) => {
    setHeatmapMetric(metric);
    fetchHeatmap(metric);
  };

  // Handle sleep arch days change
  const handleSleepDaysChange = (days: number) => {
    setSleepArchDays(days);
    fetchSleepArchitecture(days);
  };

  const getMetricLabel = (value: string) =>
    AVAILABLE_METRICS.find((m) => m.value === value)?.label || value;

  // Calculate color intensity for heatmap
  const getHeatmapColor = (value: number | null, min: number | null, max: number | null) => {
    if (value === null || min === null || max === null) return "color-empty";
    const range = max - min;
    if (range === 0) return "color-scale-4";
    const normalized = (value - min) / range;
    if (normalized < 0.25) return "color-scale-1";
    if (normalized < 0.5) return "color-scale-2";
    if (normalized < 0.75) return "color-scale-3";
    return "color-scale-4";
  };

  // Get start date for heatmap (1 year ago)
  const getStartDate = () => {
    const date = new Date();
    date.setFullYear(date.getFullYear() - 1);
    return date;
  };

  return (
    <div className="container mx-auto py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold">Insights</h1>
        <div className="flex items-center gap-2">
          <Select value="insights" onValueChange={(value) => router.push(`/${value}`)}>
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

      <Tabs defaultValue="heatmap" className="w-full">
        <TabsList className="grid w-full grid-cols-3 mb-4">
          <TabsTrigger value="heatmap">Annual Heatmap</TabsTrigger>
          <TabsTrigger value="sleep-arch">Sleep Architecture</TabsTrigger>
          <TabsTrigger value="chronotype">Chronotype</TabsTrigger>
        </TabsList>

        {/* Annual Metric Heatmap */}
        <TabsContent value="heatmap">
          <Card>
            <CardHeader>
              <CardTitle>Annual Metric Heatmap</CardTitle>
              <CardDescription>
                Visualize any metric over the past year. Darker colors indicate higher values.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-2">Metric</label>
                <select
                  value={heatmapMetric}
                  onChange={(e) => handleMetricChange(e.target.value)}
                  className="w-full max-w-xs border rounded px-3 py-2 bg-background"
                >
                  {AVAILABLE_METRICS.map((m) => (
                    <option key={m.value} value={m.value}>
                      {m.label}
                    </option>
                  ))}
                </select>
              </div>

              {heatmapLoading ? (
                <div className="animate-pulse">Loading heatmap...</div>
              ) : heatmapData ? (
                <div className="mt-4">
                  <style jsx global>{`
                    .react-calendar-heatmap .color-empty {
                      fill: hsl(var(--muted));
                    }
                    .react-calendar-heatmap .color-scale-1 {
                      fill: #c6e48b;
                    }
                    .react-calendar-heatmap .color-scale-2 {
                      fill: #7bc96f;
                    }
                    .react-calendar-heatmap .color-scale-3 {
                      fill: #239a3b;
                    }
                    .react-calendar-heatmap .color-scale-4 {
                      fill: #196127;
                    }
                    .react-calendar-heatmap text {
                      fill: hsl(var(--foreground));
                      font-size: 8px;
                    }
                  `}</style>
                  <CalendarHeatmap
                    startDate={getStartDate()}
                    endDate={new Date()}
                    values={heatmapData.data.map((d) => ({
                      date: d.date,
                      count: d.value,
                    }))}
                    classForValue={(value) => {
                      if (!value || value.count === null) return "color-empty";
                      return getHeatmapColor(
                        value.count,
                        heatmapData.min_value,
                        heatmapData.max_value
                      );
                    }}
                    titleForValue={(value) => {
                      if (!value || !value.date) return "";
                      const count = (value as { date: string; count: number | null }).count;
                      return `${value.date}: ${count !== null ? count.toFixed(1) : "No data"}`;
                    }}
                    showWeekdayLabels
                    gutterSize={2}
                  />
                  <div className="flex items-center justify-between mt-4 text-sm text-muted-foreground">
                    <span>
                      Range: {heatmapData.min_value?.toFixed(1)} - {heatmapData.max_value?.toFixed(1)}
                    </span>
                    <div className="flex items-center gap-1">
                      <span>Less</span>
                      <div className="w-3 h-3 rounded-sm" style={{ background: "#c6e48b" }} />
                      <div className="w-3 h-3 rounded-sm" style={{ background: "#7bc96f" }} />
                      <div className="w-3 h-3 rounded-sm" style={{ background: "#239a3b" }} />
                      <div className="w-3 h-3 rounded-sm" style={{ background: "#196127" }} />
                      <span>More</span>
                    </div>
                  </div>
                </div>
              ) : null}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Sleep Architecture */}
        <TabsContent value="sleep-arch">
          <Card>
            <CardHeader>
              <CardTitle>Sleep Architecture</CardTitle>
              <CardDescription>
                View your sleep stage distribution (Deep, REM, Light) over time
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center gap-4">
                <label className="text-sm font-medium">Show last</label>
                <div className="flex gap-2">
                  {[14, 30, 60, 90].map((d) => (
                    <Badge
                      key={d}
                      variant={sleepArchDays === d ? "default" : "outline"}
                      className="cursor-pointer"
                      onClick={() => handleSleepDaysChange(d)}
                    >
                      {d} days
                    </Badge>
                  ))}
                </div>
              </div>

              {sleepArchLoading ? (
                <div className="animate-pulse">Loading sleep data...</div>
              ) : sleepArchData && sleepArchData.data.length > 0 ? (
                <>
                  {/* Summary stats */}
                  <div className="grid grid-cols-3 gap-4 mb-4">
                    <div className="text-center p-3 border rounded-lg">
                      <div className="text-2xl font-bold text-indigo-600">
                        {sleepArchData.avg_deep_pct?.toFixed(1)}%
                      </div>
                      <div className="text-sm text-muted-foreground">Avg Deep Sleep</div>
                    </div>
                    <div className="text-center p-3 border rounded-lg">
                      <div className="text-2xl font-bold text-purple-600">
                        {sleepArchData.avg_rem_pct?.toFixed(1)}%
                      </div>
                      <div className="text-sm text-muted-foreground">Avg REM Sleep</div>
                    </div>
                    <div className="text-center p-3 border rounded-lg">
                      <div className="text-2xl font-bold text-blue-400">
                        {sleepArchData.avg_light_pct?.toFixed(1)}%
                      </div>
                      <div className="text-sm text-muted-foreground">Avg Light Sleep</div>
                    </div>
                  </div>

                  {/* Stacked bar chart */}
                  <div className="h-[300px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={sleepArchData.data}
                        margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                        <XAxis
                          dataKey="date"
                          tick={{ fontSize: 10 }}
                          tickFormatter={(v) => v.slice(8) + '/' + v.slice(5, 7)}
                          interval="preserveStartEnd"
                        />
                        <YAxis domain={[0, 100]} unit="%" />
                        <Tooltip
                          content={({ active, payload, label }) => {
                            if (active && payload && payload.length) {
                              const data = payload[0].payload;
                              return (
                                <div className="rounded-lg border bg-background p-3 shadow-lg">
                                  <div className="grid gap-2">
                                    <p className="text-sm font-medium text-muted-foreground">
                                      {label}
                                    </p>
                                    <p className="text-sm font-bold">
                                      Total: {data.total_hours?.toFixed(1)}h
                                    </p>
                                    <div className="space-y-1">
                                      <div className="flex items-center gap-2">
                                        <div className="h-2 w-2 rounded-full bg-[#4f46e5]" />
                                        <span className="text-sm">Deep: {data.deep_pct?.toFixed(1)}%</span>
                                      </div>
                                      <div className="flex items-center gap-2">
                                        <div className="h-2 w-2 rounded-full bg-[#9333ea]" />
                                        <span className="text-sm">REM: {data.rem_pct?.toFixed(1)}%</span>
                                      </div>
                                      <div className="flex items-center gap-2">
                                        <div className="h-2 w-2 rounded-full bg-[#60a5fa]" />
                                        <span className="text-sm">Light: {data.light_pct?.toFixed(1)}%</span>
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              );
                            }
                            return null;
                          }}
                        />
                        <Legend />
                        <Bar dataKey="deep_pct" name="Deep" stackId="a" fill="#4f46e5" />
                        <Bar dataKey="rem_pct" name="REM" stackId="a" fill="#9333ea" />
                        <Bar dataKey="light_pct" name="Light" stackId="a" fill="#60a5fa" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>

                  <p className="text-xs text-muted-foreground">
                    Optimal sleep includes 15-20% deep sleep and 20-25% REM sleep. Light sleep
                    typically makes up the remainder.
                  </p>
                </>
              ) : (
                <p className="text-muted-foreground">No sleep data available.</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Chronotype */}
        <TabsContent value="chronotype">
          <Card>
            <CardHeader>
              <CardTitle>Chronotype & Social Jetlag</CardTitle>
              <CardDescription>
                Your natural sleep pattern and how well your schedule aligns with it
              </CardDescription>
            </CardHeader>
            <CardContent>
              {chronotypeLoading ? (
                <div className="animate-pulse">Analyzing sleep patterns...</div>
              ) : chronotypeData ? (
                <div className="space-y-6">
                  {/* Chronotype Card */}
                  <div className="flex items-center gap-6 p-6 border rounded-lg bg-gradient-to-r from-indigo-50 to-purple-50 dark:from-indigo-950 dark:to-purple-950">
                    <div className="text-6xl">
                      {chronotypeData.chronotype === "morning_lark"
                        ? "🌅"
                        : chronotypeData.chronotype === "night_owl"
                        ? "🦉"
                        : "⚖️"}
                    </div>
                    <div>
                      <h3 className="text-2xl font-bold">{chronotypeData.chronotype_label}</h3>
                      <p className="text-muted-foreground">Your natural chronotype</p>
                    </div>
                  </div>

                  {/* Sleep Midpoints */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="p-4 border rounded-lg">
                      <div className="text-sm text-muted-foreground mb-1">
                        Weekend Sleep Midpoint
                      </div>
                      <div className="text-3xl font-bold">
                        {chronotypeData.weekend_midpoint || "N/A"}
                      </div>
                      <p className="text-xs text-muted-foreground mt-1">
                        Your natural sleep timing
                      </p>
                    </div>
                    <div className="p-4 border rounded-lg">
                      <div className="text-sm text-muted-foreground mb-1">
                        Weekday Sleep Midpoint
                      </div>
                      <div className="text-3xl font-bold">
                        {chronotypeData.weekday_midpoint || "N/A"}
                      </div>
                      <p className="text-xs text-muted-foreground mt-1">
                        Your work-adjusted timing
                      </p>
                    </div>
                  </div>

                  {/* Social Jetlag */}
                  <div className="p-4 border rounded-lg">
                    <div className="flex items-center justify-between mb-2">
                      <div>
                        <div className="text-sm text-muted-foreground">Social Jetlag</div>
                        <div className="text-4xl font-bold">
                          {chronotypeData.social_jetlag_label}
                        </div>
                      </div>
                      <Badge
                        variant={
                          (chronotypeData.social_jetlag_minutes || 0) > 90
                            ? "destructive"
                            : (chronotypeData.social_jetlag_minutes || 0) > 60
                            ? "secondary"
                            : "default"
                        }
                        className="text-lg px-3 py-1"
                      >
                        {(chronotypeData.social_jetlag_minutes || 0) > 90
                          ? "High"
                          : (chronotypeData.social_jetlag_minutes || 0) > 60
                          ? "Moderate"
                          : "Low"}
                      </Badge>
                    </div>

                    {/* Visual jetlag bar */}
                    <div className="mt-4">
                      <div className="flex justify-between text-xs text-muted-foreground mb-1">
                        <span>0m</span>
                        <span>30m</span>
                        <span>60m</span>
                        <span>90m</span>
                        <span>120m+</span>
                      </div>
                      <div className="h-3 bg-muted rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full ${
                            (chronotypeData.social_jetlag_minutes || 0) > 90
                              ? "bg-red-500"
                              : (chronotypeData.social_jetlag_minutes || 0) > 60
                              ? "bg-yellow-500"
                              : "bg-green-500"
                          }`}
                          style={{
                            width: `${Math.min(
                              100,
                              ((chronotypeData.social_jetlag_minutes || 0) / 120) * 100
                            )}%`,
                          }}
                        />
                      </div>
                    </div>
                  </div>

                  {/* Recommendation */}
                  {chronotypeData.recommendation && (
                    <div className="p-4 bg-muted rounded-lg">
                      <h4 className="font-medium mb-2">Recommendation</h4>
                      <p className="text-muted-foreground">{chronotypeData.recommendation}</p>
                    </div>
                  )}

                  <p className="text-xs text-muted-foreground">
                    Social jetlag is the difference between your natural sleep timing (weekends)
                    and your socially-imposed timing (weekdays). High social jetlag is associated
                    with fatigue, mood issues, and metabolic problems.
                  </p>
                </div>
              ) : (
                <p className="text-muted-foreground">Unable to determine chronotype.</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
