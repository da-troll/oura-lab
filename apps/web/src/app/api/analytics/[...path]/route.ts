import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";

const ANALYTICS_URL =
  process.env.ANALYTICS_BASE_URL || "http://localhost:8001";

function getSessionToken(cookieStore: Awaited<ReturnType<typeof cookies>>): string | undefined {
  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";
  return cookieStore.get(cookieName)?.value;
}

function verifyCsrf(request: NextRequest, cookieStore: Awaited<ReturnType<typeof cookies>>): boolean {
  const headerToken = request.headers.get("x-csrf-token");
  const cookieToken = cookieStore.get("csrf_token")?.value;
  return !!headerToken && !!cookieToken && headerToken === cookieToken;
}

const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"]);

async function handler(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  const cookieStore = await cookies();
  const token = getSessionToken(cookieStore);

  if (!token) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  // CSRF check on mutating methods
  if (MUTATING_METHODS.has(request.method) && !verifyCsrf(request, cookieStore)) {
    return NextResponse.json({ error: "CSRF validation failed" }, { status: 403 });
  }

  const { path } = await params;
  const targetPath = `/${path.join("/")}`;
  const url = new URL(`${ANALYTICS_URL}${targetPath}`);

  // Forward query params
  request.nextUrl.searchParams.forEach((value, key) => {
    url.searchParams.append(key, value);
  });

  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
  };

  // Forward content-type if present
  const contentType = request.headers.get("content-type");
  if (contentType) {
    headers["Content-Type"] = contentType;
  }

  const fetchOptions: RequestInit = {
    method: request.method,
    headers,
  };

  if (MUTATING_METHODS.has(request.method)) {
    try {
      fetchOptions.body = await request.text();
    } catch {
      // No body
    }
  }

  const response = await fetch(url.toString(), fetchOptions);

  // Handle NDJSON streaming (for chat endpoint)
  const respContentType = response.headers.get("content-type") || "";
  if (respContentType.includes("application/x-ndjson") && response.body) {
    return new NextResponse(response.body as ReadableStream, {
      status: response.status,
      headers: {
        "Content-Type": "application/x-ndjson",
        "Transfer-Encoding": "chunked",
        "Cache-Control": "no-cache",
      },
    });
  }

  // Standard JSON response
  const data = await response.text();
  return new NextResponse(data, {
    status: response.status,
    headers: {
      "Content-Type": respContentType || "application/json",
    },
  });
}

export const GET = handler;
export const POST = handler;
export const PUT = handler;
export const PATCH = handler;
export const DELETE = handler;
