"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

import { clientApi } from "@/lib/api-client";
import { ThemeToggle } from "@/components/theme-toggle";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
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
  toolCalls?: { name: string; args: Record<string, unknown> }[];
}

interface Conversation {
  id: string;
  title: string | null;
  created_at: string;
  updated_at: string;
}

function getCsrfToken(): string {
  const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : "";
}

export default function ChatPage() {
  const router = useRouter();
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [chatEnabled, setChatEnabled] = useState<boolean | null>(null);
  const [toolStatus, setToolStatus] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Check if chat is enabled
  useEffect(() => {
    async function checkStatus() {
      try {
        const data = await clientApi<{ enabled: boolean }>("/chat/status");
        setChatEnabled(data.enabled);
        if (data.enabled) {
          loadConversations();
        }
      } catch {
        setChatEnabled(false);
      }
    }
    checkStatus();
  }, []);

  async function loadConversations() {
    try {
      const data = await clientApi<Conversation[]>("/chat/conversations");
      setConversations(data);
    } catch {
      // Ignore
    }
  }

  async function loadConversation(id: string) {
    try {
      const data = await clientApi<ChatMessage[]>(
        `/chat/conversations/${id}`
      );
      setMessages(data);
      setConversationId(id);
    } catch {
      // Ignore
    }
  }

  async function deleteConversation(id: string) {
    try {
      await clientApi(`/chat/conversations/${id}`, { method: "DELETE" });
      setConversations((prev) => prev.filter((c) => c.id !== id));
      if (conversationId === id) {
        setConversationId(null);
        setMessages([]);
      }
    } catch {
      // Ignore
    }
  }

  function startNewConversation() {
    setConversationId(null);
    setMessages([]);
    setInput("");
  }

  async function sendMessage() {
    const text = input.trim();
    if (!text || loading) return;

    setInput("");
    setLoading(true);
    setToolStatus(null);

    // Add user message immediately
    const userMessage: ChatMessage = { role: "user", content: text };
    setMessages((prev) => [...prev, userMessage]);

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
          conversation_id: conversationId,
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
                break;
            }
          } catch {
            // Skip malformed lines
          }
        }
      }

      // Refresh conversation list
      loadConversations();
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
  }

  // Auto-scroll to bottom
  useEffect(() => {
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
        <div className="flex justify-between items-center p-4 border-b">
          <h1 className="text-xl font-semibold">Health Assistant</h1>
          <div className="flex items-center gap-2">
            <Select
              value="chat"
              onValueChange={(value) => router.push(`/${value}`)}
            >
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

        {/* Messages */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {messages.length === 0 && (
            <div className="text-center text-muted-foreground py-20">
              <p className="text-lg font-medium mb-2">
                Ask me about your health data
              </p>
              <p className="text-sm">
                Try: &quot;How was my sleep this week?&quot; or &quot;What
                affects my readiness score?&quot;
              </p>
            </div>
          )}

          {messages.map((msg, i) => (
            <div
              key={i}
              className={`flex ${
                msg.role === "user" ? "justify-end" : "justify-start"
              }`}
            >
              <div
                className={`max-w-[75%] rounded-lg px-4 py-2 ${
                  msg.role === "user"
                    ? "bg-primary text-primary-foreground"
                    : "bg-muted"
                }`}
              >
                <p className="whitespace-pre-wrap text-sm">{msg.content}</p>
              </div>
            </div>
          ))}

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
          <form
            onSubmit={(e) => {
              e.preventDefault();
              sendMessage();
            }}
            className="flex gap-2"
          >
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
        </div>
      </div>
    </div>
  );
}
