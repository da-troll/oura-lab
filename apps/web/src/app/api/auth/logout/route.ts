import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";

const ANALYTICS_URL =
  process.env.ANALYTICS_BASE_URL || "http://localhost:8001";

function verifyCsrf(request: NextRequest, cookieStore: Awaited<ReturnType<typeof cookies>>): boolean {
  const headerToken = request.headers.get("x-csrf-token");
  const cookieToken = cookieStore.get("csrf_token")?.value;
  return !!headerToken && !!cookieToken && headerToken === cookieToken;
}

function getSessionToken(cookieStore: Awaited<ReturnType<typeof cookies>>): string | undefined {
  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";
  return cookieStore.get(cookieName)?.value;
}

export async function POST(request: NextRequest) {
  const cookieStore = await cookies();

  if (!verifyCsrf(request, cookieStore)) {
    return NextResponse.json({ error: "CSRF validation failed" }, { status: 403 });
  }

  const token = getSessionToken(cookieStore);

  if (token) {
    // Best-effort logout on backend
    try {
      await fetch(`${ANALYTICS_URL}/auth/logout`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      });
    } catch {
      // Ignore errors — we clear the cookie regardless
    }
  }

  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";

  const res = NextResponse.json({ success: true });
  res.cookies.delete(cookieName);
  return res;
}
