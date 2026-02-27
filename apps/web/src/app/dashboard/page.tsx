"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ChartTooltip } from "@/components/ui/chart-tooltip";
import { Settings } from "lucide-react";
import { useRouter } from "next/navigation";

interface DashboardSummary {
  readiness_avg: number | null;
  sleep_score_avg: number | null;
  activity_avg: number | null;
  steps_avg: number | null;
  hrv_avg: number | null;
  rhr_avg: number | null;
  sleep_hours_avg: number | null;
  calories_avg: number | null;
  stress_avg: number | null;
  recovery_avg: number | null;
  spo2_avg: number | null;
  workout_minutes_avg: number | null;
  days_with_data: number;
}

interface TrendPoint {
  date: string;
  value: number | null;
}

interface TrendSeries {
  name: string;
  data: TrendPoint[];
}

interface DashboardData {
  connected: boolean;
  summary: DashboardSummary;
  trends: TrendSeries[];
}

const CHART_COLORS = {
  readiness: "#2563eb",
  sleep: "#ec4899",
  activity: "#16a34a",
  steps: "#ea580c",
  hrv: "#0891b2",
  rhr: "#dc2626",
  sleep_hours: "#8b5cf6",
  stress: "#f59e0b",
  recovery: "#10b981",
  spo2: "#6366f1",
  workout_minutes: "#f97316",
};

export default function DashboardPage() {
  const router = useRouter();
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [days, setDays] = useState(7);
  const [activeTab, setActiveTab] = useState("scores");

  useEffect(() => {
    async function fetchDashboard() {
      setLoading(true);
      try {
        const response = await fetch(
          `/api/analytics/dashboard?days=${days}`
        );
        if (!response.ok) {
          throw new Error("Failed to fetch dashboard data");
        }
        const result = await response.json();
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        setLoading(false);
      }
    }
    fetchDashboard();
  }, [days]);

  const formatValue = (value: number | null | undefined, suffix = "") => {
    if (value === null || value === undefined) return "--";
    return `${Math.round(value)}${suffix}`;
  };

  const getTrendData = (name: string) => {
    const series = data?.trends.find((t) => t.name === name);
    return (
      series?.data.map((point) => ({
        date: point.date.slice(8) + '/' + point.date.slice(5, 7), // DD/MM format
        value: point.value,
      })) || []
    );
  };

  const hasData = data?.connected && data.summary.days_with_data > 0;

  const getTooltipConfig = (name: string) => {
    switch (name) {
      case "steps":
        return { unit: "", precision: 0 };
      case "sleep_hours":
        return { unit: "h", precision: 1 };
      case "hrv":
        return { unit: " ms", precision: 0 };
      case "rhr":
        return { unit: " bpm", precision: 0 };
      case "stress":
      case "recovery":
        return { unit: " min", precision: 0 };
      case "spo2":
        return { unit: "%", precision: 1 };
      case "workout_minutes":
        return { unit: " min", precision: 0 };
      default:
        return { unit: "", precision: 0 };
    }
  };

  const renderChart = (
    name: string,
    title: string,
    color: string,
    domain?: [number, number],
    isBar = false
  ) => {
    const chartData = getTrendData(name);
    if (!hasData || chartData.length === 0) {
      return (
        <div className="h-full flex items-center justify-center text-muted-foreground">
          <p>No data available</p>
        </div>
      );
    }

    const tooltipConfig = getTooltipConfig(name);

    if (isBar) {
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
            <XAxis
              dataKey="date"
              tick={{ fontSize: 10 }}
              interval="preserveStartEnd"
              className="text-muted-foreground"
            />
            <YAxis tick={{ fontSize: 10 }} domain={domain} className="text-muted-foreground" />
            <Tooltip
              content={<ChartTooltip unit={tooltipConfig.unit} precision={tooltipConfig.precision} />}
            />
            <Bar dataKey="value" fill={color} name={title} radius={[2, 2, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      );
    }

    return (
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 10 }}
            interval="preserveStartEnd"
            className="text-muted-foreground"
          />
          <YAxis tick={{ fontSize: 10 }} domain={domain} className="text-muted-foreground" />
          <Tooltip
            content={<ChartTooltip unit={tooltipConfig.unit} precision={tooltipConfig.precision} />}
          />
          <Line
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={2}
            dot={{ fill: color, r: 2 }}
            name={title}
            connectNulls={true}
          />
        </LineChart>
      </ResponsiveContainer>
    );
  };

  return (
    <div className="container mx-auto py-8">
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold">Dashboard</h1>
        <div className="flex items-center gap-2">
          <Select value={String(days)} onValueChange={(v) => setDays(Number(v))}>
            <SelectTrigger className="w-[120px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="7">7 days</SelectItem>
              <SelectItem value="10">10 days</SelectItem>
              <SelectItem value="30">30 days</SelectItem>
              <SelectItem value="60">60 days</SelectItem>
              <SelectItem value="100">100 days</SelectItem>
            </SelectContent>
          </Select>
          <Select value="dashboard" onValueChange={(value) => router.push(`/${value}`)}>
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

      {loading && (
        <div className="text-center text-muted-foreground py-12">Loading...</div>
      )}

      {error && (
        <div className="text-center text-red-500 py-12">Error: {error}</div>
      )}

      {!loading && !error && !hasData && (
        <Card>
          <CardContent className="h-[200px] flex items-center justify-center text-muted-foreground">
            <div className="text-center">
              <p>Connect your Oura Ring and sync data to see your dashboard</p>
              <Link href="/settings" className="mt-2 inline-block">
                <Button variant="link">Go to Settings</Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      )}

      {!loading && !error && hasData && (
        <>
          {/* Summary Cards - compact horizontal layout */}
          <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4 mb-6">
            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Readiness</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.readiness_avg)}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Sleep Score</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.sleep_score_avg)}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Activity</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.activity_avg)}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Steps</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.steps_avg)}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">HRV</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.hrv_avg, " ms")}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Resting HR</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.rhr_avg, " bpm")}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Sleep</span>
                <div className="text-right">
                  <div className="text-xl font-bold">
                    {data?.summary.sleep_hours_avg ? `${data.summary.sleep_hours_avg.toFixed(1)}h` : "--"}
                  </div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Calories</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.calories_avg)}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Stress</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.stress_avg, " min")}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Recovery</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.recovery_avg, " min")}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">SpO2</span>
                <div className="text-right">
                  <div className="text-xl font-bold">
                    {data?.summary.spo2_avg ? `${data.summary.spo2_avg.toFixed(1)}%` : "--"}
                  </div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>

            <Card className="py-3">
              <CardContent className="flex items-center justify-between p-0 px-4">
                <span className="text-sm font-medium">Workouts</span>
                <div className="text-right">
                  <div className="text-xl font-bold">{formatValue(data?.summary.workout_minutes_avg, " min")}</div>
                  <p className="text-xs text-muted-foreground">{days}-day avg</p>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Trend Charts with Tabs */}
          <Card>
            <CardHeader>
              <CardTitle>Trends</CardTitle>
              <CardDescription>Your metrics over the past {days} days</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
                <TabsList className="grid w-full grid-cols-5 mb-4">
                  <TabsTrigger value="scores">Scores</TabsTrigger>
                  <TabsTrigger value="activity">Activity</TabsTrigger>
                  <TabsTrigger value="heart">Heart</TabsTrigger>
                  <TabsTrigger value="sleep">Sleep</TabsTrigger>
                  <TabsTrigger value="body">Body</TabsTrigger>
                </TabsList>

                <TabsContent value="scores" className="space-y-4">
                  <div className="grid gap-4 md:grid-cols-2">
                    <div>
                      <h4 className="text-sm font-medium mb-2">Readiness Score</h4>
                      <div className="h-[200px]">
                        {renderChart("readiness", "Readiness", CHART_COLORS.readiness, [40, 100])}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Sleep Score</h4>
                      <div className="h-[200px]">
                        {renderChart("sleep", "Sleep", CHART_COLORS.sleep, [40, 100])}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Activity Score</h4>
                      <div className="h-[200px]">
                        {renderChart("activity", "Activity", CHART_COLORS.activity, [40, 100])}
                      </div>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="activity" className="space-y-4">
                  <div className="grid gap-4 md:grid-cols-2">
                    <div>
                      <h4 className="text-sm font-medium mb-2">Daily Steps</h4>
                      <div className="h-[200px]">
                        {renderChart("steps", "Steps", CHART_COLORS.steps, undefined, true)}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Workout Duration</h4>
                      <div className="h-[200px]">
                        {renderChart("workout_minutes", "Workout", CHART_COLORS.workout_minutes, undefined, true)}
                      </div>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="heart" className="space-y-4">
                  <div className="grid gap-4 md:grid-cols-2">
                    <div>
                      <h4 className="text-sm font-medium mb-2">HRV (Heart Rate Variability)</h4>
                      <div className="h-[200px]">
                        {renderChart("hrv", "HRV", CHART_COLORS.hrv)}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Resting Heart Rate</h4>
                      <div className="h-[200px]">
                        {renderChart("rhr", "RHR", CHART_COLORS.rhr, [40, 100])}
                      </div>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="sleep" className="space-y-4">
                  <div className="grid gap-4 md:grid-cols-2">
                    <div>
                      <h4 className="text-sm font-medium mb-2">Sleep Score</h4>
                      <div className="h-[200px]">
                        {renderChart("sleep", "Sleep Score", CHART_COLORS.sleep, [40, 100])}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Sleep Duration (hours)</h4>
                      <div className="h-[200px]">
                        {renderChart("sleep_hours", "Hours", CHART_COLORS.sleep_hours, [0, 12])}
                      </div>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="body" className="space-y-4">
                  <div className="grid gap-4 md:grid-cols-2">
                    <div>
                      <h4 className="text-sm font-medium mb-2">Stress (high minutes)</h4>
                      <div className="h-[200px]">
                        {renderChart("stress", "Stress", CHART_COLORS.stress, undefined, true)}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Recovery (high minutes)</h4>
                      <div className="h-[200px]">
                        {renderChart("recovery", "Recovery", CHART_COLORS.recovery, undefined, true)}
                      </div>
                    </div>
                    <div>
                      <h4 className="text-sm font-medium mb-2">Blood Oxygen (SpO2)</h4>
                      <div className="h-[200px]">
                        {renderChart("spo2", "SpO2", CHART_COLORS.spo2, [90, 100])}
                      </div>
                    </div>
                  </div>
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
