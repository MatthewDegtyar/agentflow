"use client";
import { useSearchParams } from "next/navigation";
import { useEffect, useRef, useState } from "react";

export type StepMetrics = {
  capacity: number; queue: number; inflight: number;
  arrival_rate: number; throughput: number;
  wait_ms: number | null; service_ms: number | null; wait_p95_ms: number | null; service_p95_ms: number | null;
  utilization: number | null; L_observed: number | null; L_littles: number | null; queue_trend: number;
  success_rate: number | null; retries: number; dead_letters: number;
  tokens_in: number; tokens_out: number; cost_usd: number;
  slots_needed_at_80pct: number | null; status: "bottleneck" | "busy" | "ok" | "idle"; hint: string | null;
};
export type Metrics = {
  window_s: number; bottleneck: string | null;
  steps: Record<string, StepMetrics>;
  edges: { from: string; to: string; rate: number }[];
  workflow: { runs_started_per_s: number; runs_completed_per_s: number; run_duration_s: number | null;
              run_duration_p95_s: number | null; success_rate: number | null };
  load: { rate: number; duration_s: number; sent: number; running: boolean } | null;
};
export type Edge = { from: string; to: string; when: string | null; with: Record<string, unknown> };
export type Workflow = {
  name: string; session_key: string | null; yaml: string; examples: Record<string, unknown>[];
  flow: Edge[];
  steps: Record<string, { repo: string | null; ref: string; path: string | null; origin: string | null;
    commit: string | null; replicas: number; concurrency: number; timeout_s: number; retries: number;
    max_queue: number; processes: { index: number; port: number; healthy: boolean; restarts: number; inflight: number }[] }>;
};
export type QueryResult = { columns: string[]; rows: unknown[][]; truncated: boolean; elapsed_ms?: number };

// Monitor mode (`agentflow monitor ROOT...`): one API serves many workflows; the selected one travels as ?w=<id>
// in the page URL and is appended to every API call.
export type Health = { ok: boolean; mode?: "monitor"; workflow: string | null; roots?: string[]; workflows?: number };
export type MonitoredWorkflow = {
  id: string; name: string; dir: string; live: boolean; status: "running" | "stopped" | "lost" | "unknown";
  pid: number | null; started: number | null; stopped: number | null; agentflow_version: string | null;
  runs: number; runs_completed: number; runs_failed: number; runs_running: number; dead_letters: number;
  cost_usd: number; last_event: string | null;
};

/** Append ?w= (the monitored workflow) to an API path or page href. */
export function withW(path: string, w: string | null | undefined): string {
  if (!w) return path;
  return `${path}${path.includes("?") ? "&" : "?"}w=${encodeURIComponent(w)}`;
}

/** The workflow selected in monitor mode (?w=), or null. */
export function useW(): string | null {
  return useSearchParams().get("w");
}

function currentW(): string | null {
  return typeof window === "undefined" ? null : new URLSearchParams(window.location.search).get("w");
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  return rawApi<T>(withW(path, currentW()), init);
}

async function rawApi<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, { ...init, headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) } });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(typeof body?.detail === "string" ? body.detail : `HTTP ${res.status}`);
  return body as T;
}

/** Poll an endpoint; keeps the last good value and exposes the last error. */
export function usePoll<T>(rawPath: string | null, intervalMs = 2000) {
  const w = useW();
  const path = rawPath === null ? null : withW(rawPath, rawPath.startsWith("/workflows") || rawPath.startsWith("/health") ? null : w);
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    if (!path) return;
    let timer: ReturnType<typeof setTimeout>;
    const tick = async () => {
      try {
        const d = await rawApi<T>(path);
        if (alive.current) { setData(d); setError(null); }
      } catch (e) {
        if (alive.current) setError((e as Error).message);
      }
      if (alive.current) timer = setTimeout(tick, intervalMs);
    };
    tick();
    return () => { alive.current = false; clearTimeout(timer); };
  }, [path, intervalMs]);
  return { data, error };
}

export const fmtMs = (v: number | null | undefined) =>
  v == null ? "–" : v >= 1000 ? `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}s` : `${Math.round(v)}ms`;
export const fmtPct = (v: number | null | undefined) => (v == null ? "–" : `${Math.round(v * 100)}%`);
export const fmtRate = (v: number | null | undefined) => (v == null ? "–" : `${v.toFixed(v < 1 ? 2 : 1)}/s`);
