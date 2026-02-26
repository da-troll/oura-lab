/**
 * API client for the analytics service.
 */

const ANALYTICS_BASE_URL =
  process.env.ANALYTICS_BASE_URL || "http://localhost:8001";

export interface ApiError {
  error: string;
  message: string;
  details?: Record<string, unknown>;
}

export class ApiClientError extends Error {
  constructor(
    public status: number,
    public data: ApiError
  ) {
    super(data.message);
    this.name = "ApiClientError";
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let errorData: ApiError;
    try {
      errorData = await response.json();
    } catch {
      errorData = {
        error: "unknown_error",
        message: response.statusText || "An unknown error occurred",
      };
    }
    throw new ApiClientError(response.status, errorData);
  }
  return response.json();
}

/**
 * Fetch from the analytics API (server-side only — used in BFF routes).
 */
export async function analyticsApi<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const url = `${ANALYTICS_BASE_URL}${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  return handleResponse<T>(response);
}

// ============================================
// Client-side API (calls BFF proxy)
// ============================================

function getCsrfToken(): string {
  if (typeof document === "undefined") return "";
  const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : "";
}

const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

/**
 * Client-side fetch through the BFF proxy at /api/analytics/...
 * Auto-attaches CSRF token on mutating requests.
 */
export async function clientApi<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const url = `/api/analytics${path}`;
  const method = options?.method?.toUpperCase() || "GET";

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options?.headers as Record<string, string>),
  };

  if (MUTATING_METHODS.has(method)) {
    headers["X-CSRF-Token"] = getCsrfToken();
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });

  return handleResponse<T>(response);
}

// ============================================
// Auth endpoints (server-side, used in OAuth BFF routes)
// ============================================

export interface AuthUrlResponse {
  url: string;
  state: string;
}

export interface AuthStatusResponse {
  connected: boolean;
  expiresAt?: string;
  scopes?: string[];
}

export interface ExchangeCodeResponse {
  success: boolean;
  message?: string;
}

export async function getAuthUrl(token: string): Promise<AuthUrlResponse> {
  return analyticsApi<AuthUrlResponse>("/auth/oura/url", {
    headers: { Authorization: `Bearer ${token}` },
  });
}

export async function exchangeCode(
  code: string,
  token: string
): Promise<ExchangeCodeResponse> {
  return analyticsApi<ExchangeCodeResponse>("/auth/oura/exchange", {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
    body: JSON.stringify({ code }),
  });
}

export async function getAuthStatus(token: string): Promise<AuthStatusResponse> {
  return analyticsApi<AuthStatusResponse>("/auth/oura/status", {
    headers: { Authorization: `Bearer ${token}` },
  });
}

export async function revokeAuth(token: string): Promise<{ success: boolean }> {
  return analyticsApi<{ success: boolean }>("/auth/oura/revoke", {
    method: "POST",
    headers: { Authorization: `Bearer ${token}` },
  });
}

// ============================================
// Health check
// ============================================

export interface HealthResponse {
  ok: boolean;
}

export async function healthCheck(): Promise<HealthResponse> {
  return analyticsApi<HealthResponse>("/health");
}
