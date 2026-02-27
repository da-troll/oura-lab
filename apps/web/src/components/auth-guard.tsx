"use client";

import { useAuth } from "@/lib/auth-context";

/**
 * Client-side auth guard (UX layer only — real security is in proxy.ts).
 * Shows a loading state while auth is being checked.
 */
export function AuthGuard({ children }: { children: React.ReactNode }) {
  const { loading, isAuthenticated } = useAuth();

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-pulse text-muted-foreground">Loading...</div>
      </div>
    );
  }

  if (!isAuthenticated) {
    // Middleware handles redirect — this is just a fallback
    return null;
  }

  return <>{children}</>;
}
