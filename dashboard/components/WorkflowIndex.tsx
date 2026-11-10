"use client";
import Link from "next/link";
import { usePoll, withW, type Health, type MonitoredWorkflow } from "@/lib/api";

const when = (s: string | null) => (s ? new Date(s).toLocaleString() : "–");

/** Monitor mode home: every engine work dir under the monitored roots, live ones first. */
export default function WorkflowIndex({ health }: { health: Health }) {
  const { data, error } = usePoll<MonitoredWorkflow[]>("/workflows", 3000);
  if (error && !data) return <p className="error">{error}</p>;
  const rows = data ?? [];
  return (
    <>
      <h1>Workflows</h1>
      <p className="sub">
        Monitoring {health.roots?.join(", ")}. Every engine that ran there, live ones first. Pick one to see its flow graph, runs and logs.
      </p>
      <div className="panel table-wrap">
        <table>
          <thead><tr>
            <th>workflow</th><th>status</th><th>runs</th><th>running</th><th>completed</th><th>failed</th><th>dead letters</th>
            <th>cost</th><th>last event</th><th>agentflow</th>
          </tr></thead>
          <tbody>
            {rows.map((x) => (
              <tr key={x.id}>
                <td><Link href={withW("/", x.id)}><b>{x.id}</b></Link>{x.name !== x.id.split("/").pop() && <span className="muted"> · {x.name}</span>}</td>
                <td><span className={`pill ${x.live ? "ok" : x.status === "lost" ? "failed" : "idle"}`}>{x.live ? "running" : x.status}</span></td>
                <td className="num">{x.runs}</td>
                <td className="num">{x.runs_running}</td>
                <td className="num">{x.runs_completed}</td>
                <td className="num">{x.runs_failed}</td>
                <td className="num">{x.dead_letters}</td>
                <td className="num">${x.cost_usd.toFixed(4)}</td>
                <td>{when(x.last_event)}</td>
                <td className="mono">{x.agentflow_version ?? "–"}</td>
              </tr>
            ))}
            {!rows.length && <tr><td colSpan={10} className="muted">No work dirs found yet. They appear when an engine starts under a monitored root.</td></tr>}
          </tbody>
        </table>
      </div>
    </>
  );
}
