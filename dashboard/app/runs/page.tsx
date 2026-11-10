"use client";
import Link from "next/link";
import { fmtMs, usePoll, type QueryResult } from "@/lib/api";

export default function RunsPage() {
  const { data, error } = usePoll<QueryResult>("/runs?limit=100", 2000);
  if (error && !data) return <p className="error">{error}</p>;
  const rows = data?.rows ?? [];
  return (
    <>
      <h1>Runs</h1>
      <p className="sub">Most recent 100. Click a run to trace every hop, wait and failure.</p>
      <div className="panel table-wrap">
        <table>
          <thead><tr><th>run</th><th>started</th><th>session</th><th>status</th><th>duration</th><th>failed items</th></tr></thead>
          <tbody>
            {rows.map((r) => {
              const [id, started, session, status, dur, failed] = r as [string, string, string | null, string | null, number | null, number | null];
              return (
                <tr key={id}>
                  <td className="mono"><Link href={`/runs/${id}`}>{id}</Link></td>
                  <td>{new Date(started).toLocaleTimeString()}</td>
                  <td>{session ?? "–"}</td>
                  <td><span className={`pill ${status ?? "running"}`}>{status ?? "running"}</span></td>
                  <td className="num">{fmtMs(dur)}</td>
                  <td className="num">{failed ?? "–"}</td>
                </tr>
              );
            })}
            {!rows.length && <tr><td colSpan={6} className="muted">No runs yet. Start one from the Workflow page.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );
}
