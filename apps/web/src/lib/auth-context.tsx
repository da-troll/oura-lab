"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { useRouter } from "next/navigation";

interface User {
  user_id: string;
  email: string;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType | null>(null);

function getCsrfToken(): string {
  if (typeof document === "undefined") return "";
  const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : "";
}

async function parseJsonSafely(response: Response): Promise<Record<string, unknown> | null> {
  const raw = await response.text();
  if (!raw) return null;
  try {
    return JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return null;
  }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  // Bootstrap CSRF token + check auth on mount
  useEffect(() => {
    async function init() {
      try {
        // Bootstrap CSRF cookie
        await fetch("/api/auth/csrf");

        // Check if logged in
        const res = await fetch("/api/auth/me");
        if (res.ok) {
          const data = await parseJsonSafely(res);
          if (data && typeof data.user_id === "string" && typeof data.email === "string") {
            setUser({ user_id: data.user_id, email: data.email });
          }
        }
      } catch {
        // Not logged in
      } finally {
        setLoading(false);
      }
    }
    init();
  }, []);

  const login = useCallback(
    async (email: string, password: string) => {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": getCsrfToken(),
        },
        body: JSON.stringify({ email, password }),
      });

      if (!res.ok) {
        const data = await parseJsonSafely(res);
        const detail = typeof data?.detail === "string" ? data.detail : null;
        const error = typeof data?.error === "string" ? data.error : null;
        throw new Error(detail || error || `Login failed (${res.status})`);
      }

      const data = await parseJsonSafely(res);
      if (!data || typeof data.user_id !== "string" || typeof data.email !== "string") {
        throw new Error("Login returned an invalid response");
      }
      setUser({ user_id: data.user_id, email: data.email });
      router.push("/dashboard");
    },
    [router]
  );

  const register = useCallback(
    async (email: string, password: string) => {
      const res = await fetch("/api/auth/register", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": getCsrfToken(),
        },
        body: JSON.stringify({ email, password }),
      });

      if (!res.ok) {
        const data = await parseJsonSafely(res);
        const detail = typeof data?.detail === "string" ? data.detail : null;
        const error = typeof data?.error === "string" ? data.error : null;
        throw new Error(detail || error || `Registration failed (${res.status})`);
      }

      const data = await parseJsonSafely(res);
      if (!data || typeof data.user_id !== "string" || typeof data.email !== "string") {
        throw new Error("Registration returned an invalid response");
      }
      setUser({ user_id: data.user_id, email: data.email });
      router.push("/dashboard");
    },
    [router]
  );

  const logout = useCallback(async () => {
    try {
      await fetch("/api/auth/logout", {
        method: "POST",
        headers: { "X-CSRF-Token": getCsrfToken() },
      });
    } catch {
      // Continue with local logout regardless
    }
    setUser(null);
    router.push("/login");
  }, [router]);

  return (
    <AuthContext.Provider
      value={{
        user,
        loading,
        isAuthenticated: !!user,
        login,
        register,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
