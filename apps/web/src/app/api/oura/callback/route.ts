import { cookies } from "next/headers";
import { type NextRequest, NextResponse } from "next/server";

import { exchangeCode } from "@/lib/api-client";

function getSessionToken(cookieStore: Awaited<ReturnType<typeof cookies>>): string | undefined {
  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";
  return cookieStore.get(cookieName)?.value;
}

/**
 * GET /api/oura/callback
 * Handles the OAuth callback from Oura.
 * Uses the user's session to forward the code exchange request.
 */
export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const code = searchParams.get("code");
  const state = searchParams.get("state");
  const error = searchParams.get("error");

  const baseUrl = process.env.NEXT_PUBLIC_BASE_URL || "http://localhost:3000";

  if (error) {
    console.error("OAuth error from Oura:", error);
    return NextResponse.redirect(
      new URL(`/settings?error=${encodeURIComponent(error)}`, baseUrl)
    );
  }

  if (!code) {
    console.error("No code in OAuth callback");
    return NextResponse.redirect(
      new URL("/settings?error=missing_code", baseUrl)
    );
  }

  if (!state) {
    console.error("No state in OAuth callback — possible CSRF attack");
    return NextResponse.redirect(
      new URL("/settings?error=missing_state", baseUrl)
    );
  }

  const cookieStore = await cookies();
  const sessionToken = getSessionToken(cookieStore);

  if (!sessionToken) {
    return NextResponse.redirect(new URL("/login", baseUrl));
  }

  try {
    // Forward code to analytics service for token exchange
    // The backend validates the OAuth state (stored in DB, bound to user)
    const response = await exchangeCode(code, sessionToken, state);

    if (!response.success) {
      console.error("Token exchange failed:", response.message);
      return NextResponse.redirect(
        new URL(`/settings?error=${encodeURIComponent(response.message || "exchange_failed")}`, baseUrl)
      );
    }

    return NextResponse.redirect(
      new URL("/settings?success=connected", baseUrl)
    );
  } catch (error) {
    console.error("Failed to exchange OAuth code:", error);
    return NextResponse.redirect(
      new URL("/settings?error=exchange_failed", baseUrl)
    );
  }
}
