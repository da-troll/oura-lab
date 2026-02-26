import { cookies } from "next/headers";
import { NextResponse } from "next/server";
import crypto from "crypto";

/**
 * GET /api/auth/csrf
 * Bootstrap CSRF token cookie. Called on mount before any POST.
 */
export async function GET() {
  const cookieStore = await cookies();
  let token = cookieStore.get("csrf_token")?.value;

  if (!token) {
    token = crypto.randomBytes(32).toString("hex");
  }

  const response = NextResponse.json({ ok: true });

  response.cookies.set("csrf_token", token, {
    httpOnly: false, // Must be readable by JS
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: 60 * 60 * 24, // 24 hours
  });

  return response;
}
