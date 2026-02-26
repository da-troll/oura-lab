import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import { getAuthUrl } from "@/lib/api-client";

function getSessionToken(cookieStore: Awaited<ReturnType<typeof cookies>>): string | undefined {
  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";
  return cookieStore.get(cookieName)?.value;
}

/**
 * GET /api/oura/auth
 * Initiates the OAuth flow. Requires an active session.
 */
export async function GET() {
  try {
    const cookieStore = await cookies();
    const sessionToken = getSessionToken(cookieStore);

    if (!sessionToken) {
      return NextResponse.redirect(
        new URL("/login", process.env.NEXT_PUBLIC_BASE_URL || "http://localhost:3000")
      );
    }

    // Get auth URL from analytics service (state is stored in DB, bound to user)
    const { url } = await getAuthUrl(sessionToken);

    // Redirect to Oura authorization URL
    return NextResponse.redirect(url);
  } catch (error) {
    console.error("Failed to initiate OAuth:", error);
    return NextResponse.redirect(
      new URL("/settings?error=auth_init_failed", process.env.NEXT_PUBLIC_BASE_URL || "http://localhost:3000")
    );
  }
}
