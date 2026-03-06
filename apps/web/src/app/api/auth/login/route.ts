import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";

const ANALYTICS_URL =
  process.env.ANALYTICS_BASE_URL || "http://localhost:8001";

function verifyCsrf(request: NextRequest, cookieStore: Awaited<ReturnType<typeof cookies>>): boolean {
  const headerToken = request.headers.get("x-csrf-token");
  const cookieToken = cookieStore.get("csrf_token")?.value;
  return !!headerToken && !!cookieToken && headerToken === cookieToken;
}

async function parseJsonSafely(response: Response): Promise<Record<string, unknown>> {
  const raw = await response.text();
  if (!raw) return {};
  try {
    return JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return { error: raw };
  }
}

export async function POST(request: NextRequest) {
  const cookieStore = await cookies();

  if (!verifyCsrf(request, cookieStore)) {
    return NextResponse.json({ error: "CSRF validation failed" }, { status: 403 });
  }

  const body = await request.json();

  // Resolve client IP for backend rate limiting.
  // Prefer proxy headers and fall back to unknown.
  const forwardedFor = request.headers.get("x-forwarded-for");
  const forwardedIp = forwardedFor
    ?.split(",")
    .map((ip) => ip.trim())
    .filter(Boolean)
    .at(0);
  const realIp = request.headers.get("x-real-ip")?.trim();
  const clientIp = forwardedIp || realIp || "unknown";

  let response: Response;
  try {
    response = await fetch(`${ANALYTICS_URL}/auth/login`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Forwarded-For": clientIp,
      },
      body: JSON.stringify(body),
    });
  } catch (error) {
    console.error("Auth login proxy failed", { error });
    return NextResponse.json({ error: "Auth service unavailable" }, { status: 503 });
  }

  const data = await parseJsonSafely(response);

  if (!response.ok) {
    return NextResponse.json(
      Object.keys(data).length ? data : { error: "Login failed" },
      { status: response.status }
    );
  }

  const userId = typeof data.user_id === "string" ? data.user_id : null;
  const email = typeof data.email === "string" ? data.email : null;
  const sessionToken = typeof data.session_token === "string" ? data.session_token : null;

  if (!userId || !email || !sessionToken) {
    return NextResponse.json({ error: "Invalid auth service response" }, { status: 502 });
  }

  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";

  const res = NextResponse.json({
    user_id: userId,
    email,
  });

  res.cookies.set(cookieName, sessionToken, {
    httpOnly: true,
    secure: isProd,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 30,
  });

  return res;
}
