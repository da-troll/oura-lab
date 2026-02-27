import { NextRequest, NextResponse } from "next/server";

const PROTECTED_PATHS = [
  "/dashboard",
  "/correlations",
  "/patterns",
  "/insights",
  "/settings",
  "/chat",
];

const PUBLIC_PATHS = ["/login", "/register"];

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow API routes and static assets
  if (pathname.startsWith("/api/") || pathname.startsWith("/_next/") || pathname.startsWith("/favicon")) {
    return NextResponse.next();
  }

  const isProd = process.env.NODE_ENV === "production";
  const cookieName = isProd ? "__Host-session_token" : "session_token";
  const sessionToken = request.cookies.get(cookieName)?.value;

  // Protected routes: redirect to login if no session
  const isProtected = PROTECTED_PATHS.some(
    (p) => pathname === p || pathname.startsWith(p + "/")
  );

  if (isProtected && !sessionToken) {
    const loginUrl = new URL("/login", request.url);
    loginUrl.searchParams.set("from", pathname);
    return NextResponse.redirect(loginUrl);
  }

  // Public auth pages: redirect to dashboard if already logged in
  const isPublicAuth = PUBLIC_PATHS.some((p) => pathname === p);
  if (isPublicAuth && sessionToken) {
    return NextResponse.redirect(new URL("/dashboard", request.url));
  }

  // Root redirect
  if (pathname === "/") {
    if (sessionToken) {
      return NextResponse.redirect(new URL("/dashboard", request.url));
    }
    return NextResponse.redirect(new URL("/login", request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    /*
     * Match all paths except:
     * - _next/static (static files)
     * - _next/image (image optimization)
     * - favicon.ico
     */
    "/((?!_next/static|_next/image|favicon.ico).*)",
  ],
};
