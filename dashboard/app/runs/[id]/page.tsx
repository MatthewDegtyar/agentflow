"use client";
import { useParams } from "next/navigation";
import { fmtMs, usePoll, type QueryResult } from "@/lib/api";

type Ev = {
  ts: number; event: string; item_id: string | null; parent_id: string | null; step: string | null;
  replica: number | null; attempt: number | null; wait_ms: number | null; service_ms: number | null;
  status: string | null; reason: string | null; error: string | null; message: string | null; data: string | null;
};

type Span = { item: string; step: string; attempt: number; enq: number; start?: number; end?: number; outcome: string; reason?: string | null };

function toEvents(q: QueryResult): Ev[] {
  return q.rows.map((r) => {
    const o = Object.fromEntries(q.columns.map((c, i) => [c, r[i]])) as Record<string, unknown>;
    return { ...(o as unknown as Ev), ts: new Date(o.ts as string).getTime() };
  });
}

/** Build one bar per (item, attempt): queued from `enqueued` to `started`, in service until the outcome. */
function spans(evs: Ev[]): Span[] {
  const out: Span[] = [];
  const open: Record<string, Span> = {};
  for (const e of evs) {
    if (!e.item_id || !e.step) continue;
    const key = `${e.item_id}:${e.attempt ?? 1}`;
    if (e.event === "enqueued") open[key] = { item: e.item_id, step: e.step, attempt: e.attempt ?? 1, enq: e.ts, outcome: "queued" };
    else if (e.event === "started" && open[key]) open[key].start = e.ts;
    else if (["completed", "retry", "dead_letter"].includes(e.event) && open[key]) {
      const s = open[key];
      s.end = e.ts;
      s.outcome = e.event;
      s.reason = e.reason;
      out.push(s);
      delete open[key];
    }
  }
  return [...out, ...Object.values(open)].sort((a, b) => a.enq - b.enq);
}

export default function RunDetail() {
  const { id } = useParams<{ id: string }>();
  const { data, error } = usePoll<QueryResult>(`/runs/${id}`, 2000);
  if (error && !data) return <p className="error">{error}</p>;
  if (!data) return <p className="muted">Loading…</p>;
  const evs = toEvents(data);
  const t0 = evs[0]?.ts ?? 0;
  const done = evs.find((e) => e.event === "run_completed");
  const tEnd = Math.max(...evs.map((e) => e.ts), t0 + 1);
  const total = tEnd - t0;
  const sp = spans(evs);
  const pct = (t: number) => `${(100 * (t - t0)) / total}%`;

  return (
    <>
      <h1 className="mono" style={{ fontSize: 17 }}>{id}</h1>
      <p className="sub">
        {done ? <span className={`pill ${done.status}`}>{done.status}</span> : <span className="pill running">running</span>}
        {"  "}{fmtMs(total)} total · {sp.length} step executions · {evs.filter((e) => e.event === "log").length} log lines
      </p>

      <div className="panel">
        <h2 style={{ marginTop: 0 }}>Timeline</h2>
        <p className="muted" style={{ marginTop: -6, fontSize: 12.5 }}>
          <span style={{ display: "inline-block", width: 10, height: 10, background: "var(--busy)", opacity: 0.5, borderRadius: 2 }} /> waiting in queue
          {"   "}<span style={{ display: "inline-block", width: 10, height: 10, background: "var(--accent)", borderRadius: 2 }} /> in the agent
          {"   "}<span style={{ display: "inline-block", width: 10, height: 10, background: "var(--bad)", borderRadius: 2 }} /> failed attempt
        </p>
        {sp.map((s) => {
          const start = s.start ?? s.end ?? tEnd;
          const end = s.end ?? tEnd;
          const failed = s.outcome !== "completed" && s.outcome !== "queued";
          return (
            <div key={`${s.item}:${s.attempt}`} style={{ display: "grid", gridTemplateColumns: "150px 1fr 130px", gap: 10, alignItems: "center", marginBottom: 6 }}>
              <span><b>{s.step}</b>{s.attempt > 1 ? <span className="muted"> · try {s.attempt}</span> : null}</span>
              <div style={{ position: "relative", height: 16, background: "var(--panel-2)", borderRadius: 4 }}>
                <div title={`queued ${fmtMs(start - s.enq)}`} style={{ position: "absolute", left: pct(s.enq), width: `calc(${pct(start)} - ${pct(s.enq)})`, height: "100%", background: "var(--busy)", opacity: 0.5, borderRadius: 4 }} />
                <div title={`in service ${fmtMs(end - start)}`} style={{ position: "absolute", left: pct(start), width: `max(2px, calc(${pct(end)} - ${pct(start)}))`, height: "100%", background: failed ? "var(--bad)" : "var(--accent)", borderRadius: 4 }} />
              </div>
              <span className="muted" style={{ fontSize: 12 }}>
                {fmtMs(start - s.enq)} wait · {fmtMs(end - start)}{failed ? ` · ${s.reason ?? s.outcome}` : ""}
              </span>
            </div>
          );
        })}
      </div>

      <h2>Events</h2>
      <div className="panel table-wrap">
        <table>
          <thead><tr><th>+ms</th><th>event</th><th>step</th><th>item</th><th>try</th><th>detail</th></tr></thead>
          <tbody>
            {evs.map((e, i) => (
              <tr key={i} style={e.event === "dead_letter" || e.event === "retry" ? { color: "var(--bad)" } : undefined}>
                <td className="num">{Math.round(e.ts - t0)}</td>
                <td>{e.event}</td>
                <td>{e.step ?? ""}</td>
                <td className="mono" style={{ fontSize: 11.5 }}>{e.item_id?.slice(-8) ?? ""}</td>
                <td className="num">{e.attempt ?? ""}</td>
                <td className="mono" style={{ fontSize: 11.5, maxWidth: 620, overflowWrap: "anywhere" }}>
                  {[e.status && e.event === "log" ? `[${e.status}]` : null, e.message, e.reason, e.error,
                    e.wait_ms != null ? `wait ${fmtMs(e.wait_ms)}` : null, e.service_ms != null ? `service ${fmtMs(e.service_ms)}` : null,
                    e.data && e.data.length < 400 ? e.data : e.data ? e.data.slice(0, 400) + "…" : null].filter(Boolean).join("  ·  ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
