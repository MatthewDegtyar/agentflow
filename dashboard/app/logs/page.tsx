"use client";
import { Suspense, useCallback, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { api, type QueryResult } from "@/lib/api";

type Preset = { name: string; sql: string };

const DEFAULT_SQL = `select ts, event, step, status, message, reason, error, run_id
from events
order by ts desc
limit 100`;

function LogsInner() {
  const params = useSearchParams();
  const step = params.get("step");
  const [presets, setPresets] = useState<Preset[]>([]);
  const [sql, setSql] = useState(
    step ? `select ts, event, status as level, message, reason, error, run_id, data\nfrom events\nwhere step = '${step.replace(/'/g, "''")}'\norder by ts desc\nlimit 200` : DEFAULT_SQL);
  const [res, setRes] = useState<QueryResult | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const run = useCallback(async (q: string) => {
    setBusy(true);
    setErr(null);
    try {
      setRes(await api<QueryResult>("/query", { method: "POST", body: JSON.stringify({ sql: q }) }));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, []);

  useEffect(() => { api<Preset[]>("/queries").then(setPresets).catch(() => {}); }, []);
  useEffect(() => {
    // initial query: state is only set from the promise, never synchronously in the effect body
    let alive = true;
    api<QueryResult>("/query", { method: "POST", body: JSON.stringify({ sql }) })
      .then((r) => { if (alive) setRes(r); })
      .catch((e: Error) => { if (alive) setErr(e.message); });
    return () => { alive = false; };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <>
      <h1>Logs</h1>
      <p className="sub">
        Read-only SQL (DuckDB) over the <code>events</code> table: <code>ts, event, run_id, item_id, parent_id, step,
        replica, attempt, session_id, wait_ms, service_ms, status, reason, error, message, data</code>. Use <code>data-&gt;&gt;&apos;key&apos;</code> for JSON fields.
      </p>
      <div className="panel">
        <div className="row" style={{ marginBottom: 8, gap: 6 }}>
          {presets.map((p) => (
            <button key={p.name} style={{ fontSize: 12.5 }} onClick={() => { setSql(p.sql); run(p.sql); }}>{p.name}</button>
          ))}
        </div>
        <textarea value={sql} onChange={(e) => setSql(e.target.value)} spellCheck={false}
                  onKeyDown={(e) => { if ((e.metaKey || e.ctrlKey) && e.key === "Enter") run(sql); }} />
        <div className="row" style={{ alignItems: "center", marginTop: 8 }}>
          <button className="primary" onClick={() => run(sql)} disabled={busy}>{busy ? "Running…" : "Run query"}</button>
          <span className="muted">⌘/Ctrl + Enter</span>
          {res && !err && <span className="muted">{res.rows.length} rows{res.truncated ? " (truncated)" : ""} · {res.elapsed_ms}ms</span>}
        </div>
        {err && <p className="error">{err}</p>}
      </div>
      {res && !err && (
        <div className="panel table-wrap" style={{ marginTop: 14 }}>
          <table>
            <thead><tr>{res.columns.map((c) => <th key={c}>{c}</th>)}</tr></thead>
            <tbody>
              {res.rows.map((r, i) => (
                <tr key={i}>
                  {r.map((v, j) => (
                    <td key={j} className={typeof v === "number" ? "num" : "mono"} style={{ fontSize: 12, maxWidth: 520, overflowWrap: "anywhere" }}>
                      {v === null ? <span className="muted">null</span> : typeof v === "object" ? JSON.stringify(v) : String(v)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

export default function LogsPage() {
  return (
    <Suspense fallback={<p className="muted">Loading…</p>}>
      <LogsInner />
    </Suspense>
  );
}
