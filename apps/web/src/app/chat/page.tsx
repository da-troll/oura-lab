"use client";

import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import ReactMarkdown from "react-markdown";
import type { Components } from "react-markdown";

import { clientApi } from "@/lib/api-client";
import { ChatChart, type ChatChartArtifact } from "@/components/chat-chart";
import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
} from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Settings, Send, Plus, Trash2, Loader2 } from "lucide-react";

interface ChatMessage {
  role: "user" | "assistant" | "system";
  content: string;
  artifacts?: ChatChartArtifact[];
  model?: string | null;
  tokens_in?: number | null;
  tokens_out?: number | null;
  latency_ms?: number | null;
  created_at?: string;
}

interface Conversation {
  id: string;
  title: string | null;
  created_at: string;
  updated_at: string;
}

const FOLLOW_UP_TEXT = "Would you like a different time period, chart type, or another edit?";
const CONVERSATION_PAGE_SIZE = 120;

function parseTableRow(line: string): string[] {
  return line
    .split("|")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function toRenderableMarkdown(content: string): string {
  // Some model outputs collapse table rows into a single line using "||".
  const expanded = content.replace(
    /\|\|\s*(?=(?:\d{4}-\d{2}-\d{2}|:?-{3,}))/g,
    "|\n|",
  );
  const lines = expanded.split("\n");
  const out: string[] = [];
  const dividerRe = /^\|?\s*:?-{3,}\s*(\|\s*:?-{3,}\s*)+\|?$/;

  let i = 0;
  while (i < lines.length) {
    const headerLine = lines[i].trim();
    const dividerLine = i + 1 < lines.length ? lines[i + 1].trim() : "";
    const isTableHeader = headerLine.startsWith("|") && headerLine.endsWith("|");

    if (isTableHeader && dividerRe.test(dividerLine)) {
      const headers = parseTableRow(headerLine);
      i += 2;
      let rowCount = 0;

      while (i < lines.length) {
        const rowLine = lines[i].trim();
        if (!(rowLine.startsWith("|") && rowLine.endsWith("|"))) break;
        const cells = parseTableRow(rowLine);
        const row = headers.map((header, idx) => `${header}: ${cells[idx] ?? "—"}`);
        out.push(`- ${row.join(" · ")}`);
        rowCount += 1;
        i += 1;
      }

      if (rowCount === 0 && headers.length > 0) {
        out.push(`- ${headers.join(" · ")}`);
      }
      out.push("");
      continue;
    }

    out.push(lines[i]);
    i += 1;
  }

  return out.join("\n");
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function splitFollowUp(content: string): { body: string; hasFollowUp: boolean } {
  if (!content) return { body: "", hasFollowUp: false };
  const trimmed = content.trimEnd();

  const italicPattern = new RegExp(`\\*${escapeRegex(FOLLOW_UP_TEXT)}\\*\\s*$`, "i");
  if (italicPattern.test(trimmed)) {
    return {
      body: trimmed.replace(italicPattern, "").trimEnd(),
      hasFollowUp: true,
    };
  }

  const plainPattern = new RegExp(`${escapeRegex(FOLLOW_UP_TEXT)}\\s*$`, "i");
  if (plainPattern.test(trimmed)) {
    return {
      body: trimmed.replace(plainPattern, "").trimEnd(),
      hasFollowUp: true,
    };
  }

  return { body: trimmed, hasFollowUp: false };
}

function formatTokenCount(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return value.toLocaleString();
}

function normalizeChatMessages(data: ChatMessage[]): ChatMessage[] {
  return data.map((msg) => ({
    ...msg,
    artifacts: Array.isArray(msg.artifacts) ? msg.artifacts : undefined,
  }));
}

const markdownComponents: Components = {
  img: ({ alt }) => (
    <span className="text-xs text-muted-foreground">
      {alt ? `[Image omitted: ${alt}]` : "[Image omitted]"}
    </span>
  ),
};

function getCsrfToken(): string {
  const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : "";
}

const ChatComposer = memo(function ChatComposer({
  loading,
  onSend,
}: {
  loading: boolean;
  onSend: (text: string) => Promise<void>;
}) {
  const [input, setInput] = useState("");

  async function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const text = input.trim();
    if (!text || loading) return;
    setInput("");
    await onSend(text);
  }

  return (
    <form onSubmit={handleSubmit} className="flex gap-2">
      <input
        type="text"
        value={input}
        onChange={(e) => setInput(e.target.value)}
        placeholder="Ask about your health data..."
        className="flex-1 border rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary bg-background"
        disabled={loading}
      />
      <Button type="submit" disabled={loading || !input.trim()}>
        {loading ? (
          <Loader2 className="h-4 w-4 animate-spin" />
        ) : (
          <Send className="h-4 w-4" />
        )}
      </Button>
    </form>
  );
});

export default function ChatPage() {
  const router = useRouter();
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [chatEnabled, setChatEnabled] = useState<boolean | null>(null);
  const [toolStatus, setToolStatus] = useState<string | null>(null);
  const [composerEpoch, setComposerEpoch] = useState(0);
  const [hasOlderMessages, setHasOlderMessages] = useState(false);
  const [loadingOlderMessages, setLoadingOlderMessages] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const suppressAutoScrollRef = useRef(false);

  const introTriggered = useRef(false);

  const fetchConversationPage = useCallback(async (id: string, before?: string) => {
    const params = new URLSearchParams({
      limit: String(CONVERSATION_PAGE_SIZE),
    });
    if (before) {
      params.set("before", before);
    }
    return clientApi<ChatMessage[]>(`/chat/conversations/${id}?${params.toString()}`);
  }, []);

  async function loadConversation(id: string) {
    try {
      const data = await fetchConversationPage(id);
      setMessages(normalizeChatMessages(data));
      setHasOlderMessages(data.length === CONVERSATION_PAGE_SIZE);
      setConversationId(id);
    } catch {
      setHasOlderMessages(false);
      // Ignore
    }
  }

  const loadOlderMessages = useCallback(async () => {
    if (!conversationId || loadingOlderMessages || !hasOlderMessages) return;
    const oldestCreatedAt = messages[0]?.created_at;
    if (!oldestCreatedAt) {
      setHasOlderMessages(false);
      return;
    }

    setLoadingOlderMessages(true);
    try {
      const data = await fetchConversationPage(conversationId, oldestCreatedAt);
      if (data.length === 0) {
        setHasOlderMessages(false);
        return;
      }
      suppressAutoScrollRef.current = true;
      const olderMessages = normalizeChatMessages(data);
      setMessages((prev) => [...olderMessages, ...prev]);
      setHasOlderMessages(data.length === CONVERSATION_PAGE_SIZE);
    } catch {
      // Ignore
    } finally {
      setLoadingOlderMessages(false);
    }
  }, [conversationId, fetchConversationPage, hasOlderMessages, loadingOlderMessages, messages]);

  async function deleteConversation(id: string) {
    try {
      await clientApi(`/chat/conversations/${id}`, { method: "DELETE" });
      setConversations((prev) => prev.filter((c) => c.id !== id));
      if (conversationId === id) {
        setConversationId(null);
        setMessages([]);
        setHasOlderMessages(false);
      }
    } catch {
      // Ignore
    }
  }

  function startNewConversation() {
    setConversationId(null);
    setMessages([]);
    setHasOlderMessages(false);
    setComposerEpoch((prev) => prev + 1);
    triggerIntro();
  }

  const sendRaw = useCallback(async (
    text: string,
    opts: { showUserMessage: boolean; convId: string | null },
  ) => {
    setLoading(true);
    setToolStatus(null);

    if (opts.showUserMessage) {
      const userMessage: ChatMessage = { role: "user", content: text };
      setMessages((prev) => [...prev, userMessage]);
    }

    try {
      const baseUrl = "/api/analytics/chat";
      const response = await fetch(baseUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": getCsrfToken(),
        },
        body: JSON.stringify({
          message: text,
          conversation_id: opts.convId,
        }),
      });

      if (!response.ok) {
        throw new Error(`Chat request failed: ${response.status}`);
      }

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";
      let assistantContent = "";
      let assistantArtifacts: ChatChartArtifact[] = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.trim()) continue;

          try {
            const chunk = JSON.parse(line);

            switch (chunk.type) {
              case "conversation_id":
                setConversationId(chunk.id);
                break;

              case "tool_call":
                setToolStatus(`Calling ${chunk.name}...`);
                break;

              case "tool_result":
                setToolStatus(null);
                break;

              case "token":
                assistantContent += chunk.content;
                setMessages((prev) => {
                  const last = prev[prev.length - 1];
                  if (last?.role === "assistant") {
                    return [
                      ...prev.slice(0, -1),
                      { ...last, content: assistantContent },
                    ];
                  }
                  return [
                    ...prev,
                    { role: "assistant", content: assistantContent },
                  ];
                });
                break;

              case "chart":
                if (chunk.chart) {
                  const chart = chunk.chart as ChatChartArtifact;
                  assistantArtifacts = [...assistantArtifacts, chart];
                }
                break;

              case "usage":
                setMessages((prev) => {
                  const last = prev[prev.length - 1];
                  if (last?.role !== "assistant") return prev;
                  return [
                    ...prev.slice(0, -1),
                    {
                      ...last,
                      tokens_in:
                        typeof chunk.tokens_in === "number" ? chunk.tokens_in : last.tokens_in,
                      tokens_out:
                        typeof chunk.tokens_out === "number" ? chunk.tokens_out : last.tokens_out,
                    },
                  ];
                });
                break;

              case "error":
                setMessages((prev) => [
                  ...prev,
                  {
                    role: "assistant",
                    content: `Error: ${chunk.message}`,
                  },
                ]);
                break;

              case "done":
                if (assistantArtifacts.length) {
                  setMessages((prev) => {
                    const last = prev[prev.length - 1];
                    if (last?.role === "assistant") {
                      return [
                        ...prev.slice(0, -1),
                        {
                          ...last,
                          artifacts: [...(last.artifacts ?? []), ...assistantArtifacts],
                        },
                      ];
                    }
                    return [
                      ...prev,
                      {
                        role: "assistant",
                        content: assistantContent,
                        artifacts: assistantArtifacts,
                      },
                    ];
                  });
                }
                break;
            }
          } catch {
            // Skip malformed lines
          }
        }
      }

      // Refresh conversation list
      try {
        const convos = await clientApi<Conversation[]>("/chat/conversations");
        setConversations(convos);
      } catch {
        // Ignore
      }
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Error: ${err instanceof Error ? err.message : "Request failed"}`,
        },
      ]);
    } finally {
      setLoading(false);
      setToolStatus(null);
    }
  }, []);

  const triggerIntro = useCallback(async () => {
    await sendRaw("__OURALIE_INTRO__", { showUserMessage: false, convId: null });
  }, [sendRaw]);

  const renderedMessages = useMemo(
    () =>
      messages.map((msg) => {
        const { body, hasFollowUp } =
          msg.role === "assistant"
            ? splitFollowUp(msg.content)
            : { body: msg.content, hasFollowUp: false };

        return {
          ...msg,
          body,
          hasFollowUp,
          renderedMarkdown: msg.role === "assistant" ? toRenderableMarkdown(body) : undefined,
          hasTokenUsage:
            msg.role === "assistant" &&
            (typeof msg.tokens_in === "number" || typeof msg.tokens_out === "number"),
        };
      }),
    [messages],
  );

  const sendUserMessage = useCallback(
    async (text: string) => {
      await sendRaw(text, { showUserMessage: true, convId: conversationId });
    },
    [conversationId, sendRaw],
  );

  // Check if chat is enabled
  useEffect(() => {
    async function checkStatus() {
      try {
        const data = await clientApi<{ enabled: boolean }>("/chat/status");
        setChatEnabled(data.enabled);
        if (data.enabled) {
          const convos = await clientApi<Conversation[]>("/chat/conversations");
          setConversations(convos);
          // Auto-trigger intro if no existing conversations
          if (convos.length === 0 && !introTriggered.current) {
            introTriggered.current = true;
            triggerIntro();
          }
        }
      } catch {
        setChatEnabled(false);
      }
    }
    checkStatus();
  }, [triggerIntro]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (suppressAutoScrollRef.current) {
      suppressAutoScrollRef.current = false;
      return;
    }
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, toolStatus]);

  if (chatEnabled === null) {
    return (
      <div className="container mx-auto py-12 text-center text-muted-foreground">
        Loading...
      </div>
    );
  }

  if (!chatEnabled) {
    return (
      <div className="container mx-auto py-12 max-w-2xl">
        <div className="flex justify-between items-center mb-8">
          <h1 className="text-3xl font-bold">Chat</h1>
          <div className="flex items-center gap-2">
            <Select
              value="chat"
              onValueChange={(value) => router.push(`/${value}`)}
            >
              <SelectTrigger className="w-[160px]">
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
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            Chat feature is not enabled. Set CHAT_ENABLED=true and configure
            OPENAI_API_KEY to use the AI assistant.
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <div className="w-64 border-r bg-muted/30 flex flex-col">
        <div className="p-3 border-b">
          <Button
            variant="outline"
            className="w-full justify-start gap-2"
            onClick={startNewConversation}
          >
            <Plus className="h-4 w-4" />
            New Chat
          </Button>
        </div>
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {conversations.map((conv) => (
            <div
              key={conv.id}
              className={`group flex items-center gap-1 rounded-md px-2 py-1.5 text-sm cursor-pointer hover:bg-muted ${
                conversationId === conv.id ? "bg-muted" : ""
              }`}
              onClick={() => loadConversation(conv.id)}
            >
              <span className="flex-1 truncate">
                {conv.title || "Untitled"}
              </span>
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 opacity-0 group-hover:opacity-100"
                onClick={(e) => {
                  e.stopPropagation();
                  deleteConversation(conv.id);
                }}
              >
                <Trash2 className="h-3 w-3" />
              </Button>
            </div>
          ))}
        </div>
      </div>

      {/* Main chat area */}
      <div className="flex-1 flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center p-3 border-b">
          <h1 className="text-xl font-semibold">Ouralie</h1>
          <div className="flex items-center gap-2">
            <Select
              value="chat"
              onValueChange={(value) => router.push(`/${value}`)}
            >
              <SelectTrigger className="w-[160px]">
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

        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {messages.length === 0 && !loading && (
            <div className="text-center text-muted-foreground py-20">
              <p className="text-lg font-medium mb-2">
                Ask Ouralie about your health data
              </p>
              <p className="text-sm">
                Try: &quot;How was my sleep this week?&quot; or &quot;What
                affects my readiness score?&quot;
              </p>
            </div>
          )}

          {conversationId && hasOlderMessages && (
            <div className="flex justify-center">
              <Button
                variant="outline"
                size="sm"
                onClick={loadOlderMessages}
                disabled={loadingOlderMessages}
              >
                {loadingOlderMessages ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Loading older messages...
                  </>
                ) : (
                  "Load older messages"
                )}
              </Button>
            </div>
          )}

          {renderedMessages.map((msg, i) => {
            return (
            <div
              key={i}
              className={`flex flex-col ${
                msg.role === "user" ? "items-end" : "items-start"
              }`}
            >
              <div
                className={`max-w-[75%] rounded-lg px-4 py-2 ${
                  msg.role === "user"
                    ? "bg-primary text-primary-foreground dark:bg-zinc-600 dark:text-zinc-50 dark:border dark:border-zinc-500/50"
                    : "bg-muted"
                }`}
              >
                {msg.role === "assistant" ? (
                  <div className="prose prose-sm dark:prose-invert max-w-none [&>*:first-child]:mt-0 [&>*:last-child]:mb-0">
                    {msg.artifacts?.length ? <div className="h-4" aria-hidden /> : null}
                    {msg.artifacts?.map((chart, idx) => (
                      <ChatChart key={`${i}-chart-${idx}`} chart={chart} />
                    ))}
                    {msg.body ? (
                      <ReactMarkdown components={markdownComponents}>
                        {msg.renderedMarkdown || ""}
                      </ReactMarkdown>
                    ) : null}
                    {msg.hasFollowUp ? (
                      <p className="mt-6 italic text-muted-foreground">{FOLLOW_UP_TEXT}</p>
                    ) : null}
                  </div>
                ) : (
                  <p className="whitespace-pre-wrap text-sm">{msg.content}</p>
                )}
              </div>
              {msg.hasTokenUsage ? (
                <p className="mt-1 px-1 text-[11px] text-muted-foreground">
                  Tokens: {formatTokenCount(msg.tokens_in)} in · {formatTokenCount(msg.tokens_out)} out ·{" "}
                  {formatTokenCount((msg.tokens_in ?? 0) + (msg.tokens_out ?? 0))} total
                </p>
              ) : null}
            </div>
            );
          })}

          {toolStatus && (
            <div className="flex justify-start">
              <div className="bg-muted/50 rounded-lg px-4 py-2 flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-3 w-3 animate-spin" />
                {toolStatus}
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>

        {/* Input */}
        <div className="border-t p-4">
          <ChatComposer key={composerEpoch} loading={loading} onSend={sendUserMessage} />
        </div>
      </div>
    </div>
  );
}
