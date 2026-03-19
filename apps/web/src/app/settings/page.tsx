"use client";

import { Suspense, useEffect, useRef, useState } from "react";
import { useSearchParams, useRouter } from "next/navigation";

import { useAuth } from "@/lib/auth-context";
import { clientApi } from "@/lib/api-client";
import { splitNdjsonBuffer } from "@/lib/ndjson";
import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { ArrowLeft, CircleHelp } from "lucide-react";

const SCOPE_LABELS: Record<string, string> = {
  "extapi:daily": "Daily Summaries",
  "extapi:heartrate": "Heart Rate",
  "extapi:tag": "Tags",
  "extapi:session": "Sessions",
  "extapi:workout": "Workouts",
  "extapi:personal": "Personal Info",
  "extapi:spo2": "Blood Oxygen",
  "extapi:heart_health": "Heart Health",
};

function formatScope(scope: string): string {
  return SCOPE_LABELS[scope] || scope.replace("extapi:", "").replace(/_/g, " ");
}

interface AuthStatus {
  connected: boolean;
  expires_at?: string;
  scopes?: string[];
  oura_email?: string;
}

interface SyncResult {
  status: string;
  daysProcessed?: number;
  message?: string;
  syncMode?: string;
  startDate?: string;
  endDate?: string;
}

interface SyncProgressEvent {
  type: "progress";
  percent: number;
  phase?: string;
  message?: string;
}

interface SyncDoneEvent {
  type: "done";
  status: string;
  days_processed: number;
  message?: string;
  sync_mode?: string;
  start_date?: string;
  end_date?: string;
}

interface SyncErrorEvent {
  type: "error";
  message?: string;
}

type SyncStreamEvent = SyncProgressEvent | SyncDoneEvent | SyncErrorEvent;
type ToastKind = "success" | "error";

interface ToastItem {
  id: number;
  message: string;
  kind: ToastKind;
  visible: boolean;
}

function getCsrfToken(): string {
  if (typeof document === "undefined") return "";
  const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : "";
}

const PHASE_LABELS: Record<string, string> = {
  starting: "Preparing sync",
  resolving_window: "Preparing date range",
  fetch_raw: "Downloading Oura data",
  normalize_daily: "Processing daily metrics",
  ingest_tags: "Applying tags",
  features: "Calculating insights",
  complete: "Sync complete",
};

const SOURCE_LABELS: Record<string, string> = {
  daily_sleep: "sleep score",
  sleep: "sleep session",
  daily_readiness: "readiness",
  daily_activity: "activity",
  daily_stress: "stress",
  daily_spo2: "SpO2",
  daily_cardiovascular_age: "cardiovascular age",
  tag: "tag",
  workout: "workout",
  session: "session",
};

function toTitleCase(value: string): string {
  return value
    .split(" ")
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() + part.slice(1))
    .join(" ");
}

function formatPhaseLabel(phase?: string): string {
  if (!phase) return "Syncing your data";
  const mapped = PHASE_LABELS[phase];
  if (mapped) return mapped;
  return toTitleCase(phase.replace(/_/g, " "));
}

function formatSyncMessage(message?: string): string {
  if (!message) return "";

  let match = message.match(/^Fetched\s+([a-z_]+)\s+\((\d+)\/(\d+)\)$/i);
  if (match) {
    const source = match[1];
    const current = match[2];
    const total = match[3];
    const sourceLabel = SOURCE_LABELS[source] || source.replace(/_/g, " ");
    return `Downloaded ${sourceLabel} data (${current} of ${total}).`;
  }

  match = message.match(/^Normalized day\s+(\d+)\/(\d+)$/i);
  if (match) {
    return `Processed daily records (${match[1]} of ${match[2]} days).`;
  }

  match = message.match(/^Processed tags\s+(\d+)\/(\d+)$/i);
  if (match) {
    return `Updated tags (${match[1]} of ${match[2]}).`;
  }

  match = message.match(/^Computed features\s+(\d+)\/(\d+)$/i);
  if (match) {
    return `Calculated trend features (${match[1]} of ${match[2]}).`;
  }

  match = message.match(/^Sync window\s+(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})$/i);
  if (match) {
    return `Syncing data from ${match[1]} to ${match[2]}.`;
  }

  if (message === "Resolving sync window") return "Finding the correct date range.";
  if (message === "Computing derived features") return "Calculating trend features.";
  if (message === "No new days for feature recompute") return "No new days needed feature updates.";

  return message;
}

function SettingsContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { user, loading: authLoading, logout } = useAuth();
  const [authStatus, setAuthStatus] = useState<AuthStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [syncProgress, setSyncProgress] = useState(0);
  const [syncPhase, setSyncPhase] = useState("");
  const [syncMessage, setSyncMessage] = useState("");
  const [syncResult, setSyncResult] = useState<SyncResult | null>(null);
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const [showDisconnectDialog, setShowDisconnectDialog] = useState(false);
  const toastTimersRef = useRef<number[]>([]);
  const phaseLabel = formatPhaseLabel(syncPhase);
  const syncMessageFriendly = formatSyncMessage(syncMessage);

  const showToast = (message: string, kind: ToastKind = "success") => {
    const id = Date.now() + Math.floor(Math.random() * 1000);
    setToasts((prev) => [...prev, { id, message, kind, visible: true }]);

    const hideTimer = window.setTimeout(() => {
      setToasts((prev) => prev.map((toast) => (
        toast.id === id ? { ...toast, visible: false } : toast
      )));
    }, 3800);

    const removeTimer = window.setTimeout(() => {
      setToasts((prev) => prev.filter((toast) => toast.id !== id));
    }, 4300);

    toastTimersRef.current.push(hideTimer, removeTimer);
  };

  useEffect(() => {
    return () => {
      toastTimersRef.current.forEach((timer) => window.clearTimeout(timer));
      toastTimersRef.current = [];
    };
  }, []);

  useEffect(() => {
    const success = searchParams.get("success");
    const errorParam = searchParams.get("error");

    if (success === "connected") {
      showToast("Connected to Oura.", "success");
    } else if (errorParam) {
      showToast(`OAuth error: ${errorParam}`, "error");
    }
  }, [searchParams]);

  useEffect(() => {
    async function fetchAuthStatus() {
      try {
        const data = await clientApi<AuthStatus>("/auth/oura/status");
        setAuthStatus(data);
      } catch (err) {
        console.error("Failed to fetch auth status:", err);
        showToast("Failed to connect to analytics service.", "error");
      } finally {
        setLoading(false);
      }
    }

    fetchAuthStatus();
  }, []);

  const handleConnect = () => {
    window.location.href = "/api/oura/auth";
  };

  const handleDisconnect = async () => {
    try {
      await clientApi("/auth/oura/revoke", { method: "POST" });
      setAuthStatus({ connected: false });
      showToast("Disconnected from Oura.", "success");
    } catch {
      showToast("Failed to disconnect.", "error");
    }
  };

  const handleSync = async () => {
    setSyncing(true);
    setSyncProgress(0);
    setSyncPhase("starting");
    setSyncMessage("Starting sync...");
    setSyncResult(null);

    try {
      const response = await fetch("/api/analytics/admin/ingest/stream", {
        method: "POST",
        headers: {
          "X-CSRF-Token": getCsrfToken(),
        },
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Sync failed (${response.status})`);
      }

      if (!response.body) {
        throw new Error("Sync stream unavailable");
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let doneEvent: SyncDoneEvent | null = null;

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        const split = splitNdjsonBuffer(
          buffer,
          decoder.decode(value, { stream: true })
        );
        const lines = split.lines;
        buffer = split.buffer;

        for (const line of lines) {
          if (!line.trim()) continue;
          const event = JSON.parse(line) as SyncStreamEvent;

          if (event.type === "progress") {
            setSyncProgress(Math.max(0, Math.min(100, event.percent || 0)));
            setSyncPhase(event.phase || "");
            setSyncMessage(event.message || "");
            continue;
          }

          if (event.type === "done") {
            doneEvent = event;
            setSyncProgress(100);
            setSyncPhase("complete");
            setSyncMessage(event.message || "Sync complete");
            continue;
          }

          if (event.type === "error") {
            throw new Error(event.message || "Sync failed");
          }
        }
      }

      if (buffer.trim()) {
        const event = JSON.parse(buffer) as SyncStreamEvent;
        if (event.type === "progress") {
          setSyncProgress(Math.max(0, Math.min(100, event.percent || 0)));
          setSyncPhase(event.phase || "");
          setSyncMessage(event.message || "");
        } else if (event.type === "done") {
          doneEvent = event;
          setSyncProgress(100);
          setSyncPhase("complete");
          setSyncMessage(event.message || "Sync complete");
        } else if (event.type === "error") {
          throw new Error(event.message || "Sync failed");
        }
      }

      if (!doneEvent) {
        throw new Error("Sync ended without completion event");
      }

      setSyncResult({
        status: doneEvent.status,
        daysProcessed: doneEvent.days_processed,
        message: doneEvent.message,
        syncMode: doneEvent.sync_mode,
        startDate: doneEvent.start_date,
        endDate: doneEvent.end_date,
      });
      if (doneEvent.message) {
        showToast(doneEvent.message, "success");
      } else {
        showToast("Sync completed.", "success");
      }
    } catch (err) {
      showToast(err instanceof Error ? err.message : "Sync failed", "error");
    } finally {
      setSyncing(false);
    }
  };

  if (loading || authLoading) {
    return (
      <div className="container mx-auto py-8">
        <div className="animate-pulse">Loading...</div>
      </div>
    );
  }

  return (
    <div className="container mx-auto py-8 max-w-2xl">
      <div className="pointer-events-none fixed bottom-4 left-1/2 z-50 flex -translate-x-1/2 flex-col items-center gap-2">
        {toasts.map((toast) => (
          <div
            key={toast.id}
            role="status"
            aria-live="polite"
            className={cn(
              "pointer-events-auto rounded-lg border px-4 py-3 text-sm shadow-lg transition-all duration-300",
              toast.kind === "success"
                ? "border-emerald-500/40 bg-emerald-500/15 text-emerald-100"
                : "border-red-500/40 bg-red-500/15 text-red-100",
              toast.visible ? "translate-y-0 opacity-100" : "translate-y-4 opacity-0",
            )}
          >
            {toast.message}
          </div>
        ))}
      </div>

      <div className="flex justify-between items-center mb-8">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="icon" onClick={() => router.back()}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <h1 className="text-3xl font-bold">Settings</h1>
        </div>
        <ThemeToggle />
      </div>

      {/* Data Sync */}
      {authStatus?.connected && (
        <Card className="mb-6">
          <CardHeader className="flex flex-row items-start justify-between space-y-0">
            <div>
              <CardTitle className="flex items-center gap-1.5">
                Data Sync
                <Tooltip>
                  <TooltipTrigger asChild>
                    <button
                      type="button"
                      aria-label="How sync works"
                      className="text-muted-foreground hover:text-foreground transition-colors"
                    >
                      <CircleHelp className="h-4 w-4" />
                    </button>
                  </TooltipTrigger>
                  <TooltipContent side="top" className="max-w-none whitespace-nowrap">
                    First sync backfills your full available Oura history. Future syncs fetch only days not already stored.
                  </TooltipContent>
                </Tooltip>
              </CardTitle>
              <CardDescription className="mt-1.5">
                Sync all missing Oura data automatically
              </CardDescription>
            </div>
            <Button size="sm" onClick={handleSync} disabled={syncing}>
              {syncing ? "Syncing..." : "Sync"}
            </Button>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            {syncing && (
              <div className="space-y-2">
                <div className="flex items-center justify-between text-xs font-medium">
                  <span>{phaseLabel}</span>
                  <span>{syncProgress}%</span>
                </div>
                <div className="h-2 w-full rounded bg-muted">
                  <div
                    className="h-2 rounded bg-primary transition-all duration-300"
                    style={{ width: `${syncProgress}%` }}
                  />
                </div>
                {syncMessageFriendly && <p className="text-xs">{syncMessageFriendly}</p>}
              </div>
            )}
            {syncResult?.startDate && syncResult?.endDate && (
              <p>
                Last sync window: {syncResult.startDate} to {syncResult.endDate}
                {syncResult.syncMode ? ` (${syncResult.syncMode})` : ""}
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {/* Connection Status */}
      <Card className="mb-6">
        <CardHeader className="flex flex-row items-start justify-between space-y-0">
          <div>
            <CardTitle className="flex items-center gap-2">
              Oura Connection
              {authStatus?.connected ? (
                <Badge variant="default">Connected</Badge>
              ) : (
                <Badge variant="secondary">Not Connected</Badge>
              )}
            </CardTitle>
            <CardDescription className="mt-1.5">
              Connect your Oura Ring to sync your health data
            </CardDescription>
          </div>
          {authStatus?.connected ? (
            <Button variant="destructive" size="sm" onClick={() => setShowDisconnectDialog(true)}>
              Disconnect
            </Button>
          ) : (
            <Button size="sm" onClick={handleConnect}>Connect Oura</Button>
          )}
        </CardHeader>
        {authStatus?.connected && (
          <CardContent>
            <div className="space-y-4">
              {authStatus.expires_at && (
                <p className="text-sm text-muted-foreground">
                  Token expires: {new Date(authStatus.expires_at).toLocaleString()}
                </p>
              )}
              {authStatus.scopes && authStatus.scopes.length > 0 && (
                <div>
                  <p className="text-sm text-muted-foreground mb-1">Permissions:</p>
                  <div className="flex flex-wrap gap-1">
                    {authStatus.scopes.map((scope) => (
                      <Badge key={scope} variant="outline" className="text-xs">
                        {formatScope(scope)}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </CardContent>
        )}
      </Card>

      {/* User Account */}
      <Card className="mb-6">
        <CardHeader className="flex flex-row items-start justify-between space-y-0">
          <div>
            <CardTitle>Account</CardTitle>
            <CardDescription className="mt-1.5">
              {authLoading ? "Loading..." : (authStatus?.oura_email || user?.email || "Unknown")}
            </CardDescription>
          </div>
          <Button variant="outline" size="sm" onClick={logout}>
            Sign out
          </Button>
        </CardHeader>
      </Card>

      <Dialog open={showDisconnectDialog} onOpenChange={setShowDisconnectDialog}>
        <DialogContent showCloseButton={false}>
          <DialogHeader>
            <DialogTitle>Disconnect Oura?</DialogTitle>
            <DialogDescription>
              This will revoke access to your Oura Ring data. You can reconnect at any time.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDisconnectDialog(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={() => { setShowDisconnectDialog(false); handleDisconnect(); }}>
              Disconnect
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

export default function SettingsPage() {
  return (
    <Suspense fallback={<div className="container mx-auto py-8"><div className="animate-pulse">Loading...</div></div>}>
      <SettingsContent />
    </Suspense>
  );
}
