import { cookies } from "next/headers";
import { NextResponse } from "next/server";

const ANALYTICS_URL =
  process.env.ANALYTICS_BASE_URL || "http://localhost:8001";

function getSessionToken(cookieStore: Awaited<ReturnType<typeof cookies>>): string | undefined {
  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";
  return cookieStore.get(cookieName)?.value;
}

export async function GET() {
  const cookieStore = await cookies();
  const token = getSessionToken(cookieStore);

  if (!token) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  const response = await fetch(`${ANALYTICS_URL}/auth/me`, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!response.ok) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  const data = await response.json();
  return NextResponse.json(data);
}
