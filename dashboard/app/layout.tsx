import type { Metadata } from "next";
import { Suspense } from "react";
import Nav from "@/components/Nav";
import "./globals.css";

export const metadata: Metadata = {
  title: "agentflow",
  description: "Workflow graph, flow metrics, run traces and log queries for agentflow",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <Suspense fallback={<nav className="nav"><span className="brand">agentflow</span></nav>}>
          <Nav />
        </Suspense>
        <main><Suspense fallback={<p className="muted">Loading…</p>}>{children}</Suspense></main>
      </body>
    </html>
  );
}
