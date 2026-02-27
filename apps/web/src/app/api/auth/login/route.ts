import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";

const ANALYTICS_URL =
  process.env.ANALYTICS_BASE_URL || "http://localhost:8001";

function verifyCsrf(request: NextRequest, cookieStore: Awaited<ReturnType<typeof cookies>>): boolean {
  const headerToken = request.headers.get("x-csrf-token");
  const cookieToken = cookieStore.get("csrf_token")?.value;
  return !!headerToken && !!cookieToken && headerToken === cookieToken;
}

export async function POST(request: NextRequest) {
  const cookieStore = await cookies();

  if (!verifyCsrf(request, cookieStore)) {
    return NextResponse.json({ error: "CSRF validation failed" }, { status: 403 });
  }

  const body = await request.json();

  // Resolve client IP for backend rate limiting.
  // Use request.ip when available; otherwise use the last X-Forwarded-For hop.
  const forwardedFor = request.headers.get("x-forwarded-for");
  const forwardedIp = forwardedFor
    ?.split(",")
    .map((ip) => ip.trim())
    .filter(Boolean)
    .at(-1);
  const clientIp = request.ip || forwardedIp || "unknown";

  const response = await fetch(`${ANALYTICS_URL}/auth/login`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Forwarded-For": clientIp,
    },
    body: JSON.stringify(body),
  });

  const data = await response.json();

  if (!response.ok) {
    return NextResponse.json(data, { status: response.status });
  }

  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";

  const res = NextResponse.json({
    user_id: data.user_id,
    email: data.email,
  });

  res.cookies.set(cookieName, data.session_token, {
    httpOnly: true,
    secure: isProd,
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24 * 30,
  });

  return res;
}
