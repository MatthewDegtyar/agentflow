"use client";
import { useState } from "react";
import FlowGraph from "@/components/FlowGraph";
import WorkflowIndex from "@/components/WorkflowIndex";
import { api, fmtMs, fmtPct, fmtRate, usePoll, useW, withW, type Health, type Metrics, type Workflow } from "@/lib/api";

const WINDOWS = [30, 60, 300];

export default function Home() {
  const w = useW();
  const { data: health } = usePoll<Health>("/health", 10000);
  if (!health) return <p className="muted">Connecting…</p>;
  if (health.mode === "monitor" && !w) return <WorkflowIndex health={health} />;
  return <WorkflowPage monitor={health.mode === "monitor"} />;
}

function WorkflowPage({ monitor }: { monitor: boolean }) {
  const wid = useW();
  const [win, setWin] = useState(60);
  const { data: wf, error: wfErr } = usePoll<Workflow & { live?: boolean; status?: string }>("/workflow", monitor ? 3000 : 10000);
  const { data: m } = usePoll<Metrics & { live?: boolean }>(`/metrics?window=${win}`, 1500);
  const [selected, setSelected] = useState<string | null>(null);
  const [rate, setRate] = useState(2);
  const [duration, setDuration] = useState(60);
  const [msg, setMsg] = useState<string | null>(null);
  const [showYaml, setShowYaml] = useState(false);

  if (wfErr && !wf) return <p className="error">{monitor ? wfErr : <>Can’t reach the runtime: {wfErr}. Start it with `python -m agentflow up workflows/&lt;file&gt;.yaml`.</>}</p>;
  if (!wf) return <p className="muted">Loading workflow…</p>;

  const runOne = async () => {
    const ex = wf.examples[Math.floor(Math.random() * wf.examples.length)] ?? {};
    try {
      const r = await api<{ run_id: string }>("/runs", { method: "POST", body: JSON.stringify({ input: ex }) });
      setMsg(`started ${r.run_id}`);
    } catch (e) { setMsg((e as Error).message); }
  };
  const load = async (stop = false) => {
    try {
      if (stop) await api("/load", { method: "DELETE" });
      else await api("/load", { method: "POST", body: JSON.stringify({ rate, duration_s: duration }) });
      setMsg(stop ? "load stopped" : `load: ${rate} runs/s for ${duration}s`);
    } catch (e) { setMsg((e as Error).message); }
  };

  const sel = selected ? m?.steps[selected] : null;
  const selSpec = selected ? wf.steps[selected] : null;
  const w = m?.workflow;

  return (
    <>
      <div className="row" style={{ alignItems: "baseline", justifyContent: "space-between" }}>
        <div>
          <h1>{wf.name}</h1>
          <p className="sub">
            {Object.keys(wf.steps).length} steps · {wf.flow.length} edges
            {wf.session_key ? <> · sessions keyed by <code>{wf.session_key}</code></> : null}
          </p>
        </div>
        {monitor ? (
          <div className="row" style={{ alignItems: "center", gap: 8 }}>
            <span className={`pill ${wf.live ? "ok" : wf.status === "lost" ? "failed" : "idle"}`}>{wf.live ? "running" : wf.status}</span>
            <span className="muted">{wf.live ? "live metrics (last 60s)" : "not running: flow metrics are empty; runs and logs are kept"}</span>
          </div>
        ) : (
        <div className="row" style={{ alignItems: "center", gap: 8 }}>
          <span className="muted">window</span>
          <select value={win} onChange={(e) => setWin(Number(e.target.value))}>
            {WINDOWS.map((s) => <option key={s} value={s}>{s}s</option>)}
          </select>
          <button onClick={runOne} disabled={!wf.examples.length}>Run example</button>
          <input type="number" min={0.1} step={0.5} value={rate} onChange={(e) => setRate(Number(e.target.value))} style={{ width: 70 }} title="runs per second" />
          <span className="muted">/s for</span>
          <input type="number" min={5} step={5} value={duration} onChange={(e) => setDuration(Number(e.target.value))} style={{ width: 70 }} title="seconds" />
          <span className="muted">s</span>
          {m?.load?.running
            ? <button onClick={() => load(true)}>Stop load ({m.load.sent} sent)</button>
            : <button className="primary" onClick={() => load()} disabled={!wf.examples.length}>Generate load</button>}
        </div>
        )}
      </div>
      {msg && <p className="muted" style={{ marginTop: -6 }}>{msg}</p>}

      <div className="kpis">
        <div className="kpi"><div className="label">Runs started</div><div className="value">{fmtRate(w?.runs_started_per_s)}</div></div>
        <div className="kpi"><div className="label">Runs completed</div><div className="value">{fmtRate(w?.runs_completed_per_s)}</div></div>
        <div className="kpi"><div className="label">Run duration (avg / p95)</div><div className="value">{w?.run_duration_s ?? "–"}s / {w?.run_duration_p95_s ?? "–"}s</div></div>
        <div className="kpi"><div className="label">Run success</div><div className="value">{fmtPct(w?.success_rate)}</div></div>
        <div className="kpi">
          <div className="label">Bottleneck</div>
          <div className="value" style={{ color: m?.bottleneck ? "var(--bad)" : undefined }}>{m?.bottleneck ?? "none"}</div>
        </div>
      </div>

      <div className="grid">
        <div className="panel">
          <FlowGraph wf={wf} m={m} selected={selected} onSelect={setSelected} />
          <h2>Stations</h2>
          <div className="table-wrap">
            <table>
              <thead><tr>
                <th>step</th><th>status</th><th>slots</th><th>queue</th><th>λ in</th><th>X out</th><th>wait avg/p95</th>
                <th>service avg/p95</th><th>ρ</th><th>L obs / λ·W</th><th>retries</th><th>dead</th><th>tokens in/out</th><th>cost</th>
              </tr></thead>
              <tbody>
                {Object.entries(m?.steps ?? {}).map(([name, s]) => (
                  <tr key={name} onClick={() => setSelected(name)} style={{ cursor: "pointer" }}>
                    <td><b>{name}</b></td>
                    <td><span className={`pill ${s.status}`}>{s.status}</span></td>
                    <td className="num">{s.capacity}</td>
                    <td className="num">{s.queue}</td>
                    <td className="num">{fmtRate(s.arrival_rate)}</td>
                    <td className="num">{fmtRate(s.throughput)}</td>
                    <td className="num">{fmtMs(s.wait_ms)} / {fmtMs(s.wait_p95_ms)}</td>
                    <td className="num">{fmtMs(s.service_ms)} / {fmtMs(s.service_p95_ms)}</td>
                    <td className="num">{fmtPct(s.utilization)}</td>
                    <td className="num">{s.L_observed ?? "–"} / {s.L_littles ?? "–"}</td>
                    <td className="num">{s.retries}</td>
                    <td className="num">{s.dead_letters}</td>
                    <td className="num">{s.tokens_in.toLocaleString()} / {s.tokens_out.toLocaleString()}</td>
                    <td className="num">${s.cost_usd.toFixed(4)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="muted" style={{ fontSize: 12.5 }}>
            Little’s law: items in the system (L) = arrival rate (λ) × time in system (W). When the observed L runs well above λ·W,
            the queue is still growing, so the station can’t keep up.
          </p>
        </div>

        <div className="panel">
          {sel && selSpec && selected ? (
            <>
              <h2 style={{ marginTop: 0 }}>{selected} <span className={`pill ${sel.status}`}>{sel.status}</span></h2>
              {sel.hint && <p style={{ marginTop: 0 }}>{sel.hint}</p>}
              <div className="kv">
                <span className="k">source</span><span className="mono">{selSpec.repo ?? selSpec.path} @ {selSpec.ref}</span>
                <span className="k">resolved</span><span className="mono">{selSpec.origin} · {selSpec.commit?.slice(0, 12) ?? "no git"}</span>
                <span className="k">replicas</span><span>{selSpec.replicas} × concurrency {selSpec.concurrency}</span>
                <span className="k">timeout / retries</span><span>{selSpec.timeout_s}s / {selSpec.retries}</span>
                <span className="k">slots needed @80%</span><span>{sel.slots_needed_at_80pct ?? "–"} (has {sel.capacity})</span>
                <span className="k">success</span><span>{fmtPct(sel.success_rate)}</span>
              </div>
              <h2>Processes</h2>
              <table><tbody>
                {selSpec.processes.map((p) => (
                  <tr key={p.index}><td>#{p.index}</td><td className="mono">:{p.port}</td>
                    <td><span className={`pill ${p.healthy ? "ok" : "failed"}`}>{p.healthy ? "healthy" : "down"}</span></td>
                    <td className="num">{p.inflight} in flight</td><td className="num">{p.restarts} restarts</td></tr>
                ))}
              </tbody></table>
              <p><a href={withW(`/logs?step=${encodeURIComponent(selected)}`, wid)}>Query this step’s logs →</a></p>
            </>
          ) : (
            <p className="muted" style={{ marginTop: 0 }}>Click a station to see its source, processes and capacity advice.</p>
          )}
          <h2>
            Workflow YAML{" "}
            <button style={{ fontSize: 12, padding: "2px 8px" }} onClick={() => setShowYaml((v) => !v)}>{showYaml ? "hide" : "show"}</button>
          </h2>
          {showYaml && <pre className="yaml">{wf.yaml}</pre>}
        </div>
      </div>
    </>
  );
}
