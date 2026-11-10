"use client";
import type { Metrics, Workflow } from "@/lib/api";
import { fmtMs, fmtPct, fmtRate } from "@/lib/api";

const NODE_W = 210;
const NODE_H = 118;
const COL_GAP = 90;
const ROW_GAP = 34;
const PAD = 24;

type Node = { id: string; kind: "input" | "step" | "sink"; col: number; row: number; x: number; y: number };

/** Layer nodes by longest path from `input` (cycle-safe), then stack each layer vertically. */
function layout(wf: Workflow): { nodes: Record<string, Node>; width: number; height: number } {
  const ids = ["input", ...Object.keys(wf.steps), ...Array.from(new Set(wf.flow.filter((e) => e.to.startsWith("sink.")).map((e) => e.to)))];
  const level: Record<string, number> = { input: 0 };
  for (let pass = 0; pass < ids.length; pass++) {
    let changed = false;
    for (const e of wf.flow) {
      if (level[e.from] === undefined) continue;
      const next = level[e.from] + 1;
      if ((level[e.to] ?? -1) < next && next < ids.length) { level[e.to] = next; changed = true; }
    }
    if (!changed) break;
  }
  const cols: Record<number, string[]> = {};
  for (const id of ids) (cols[level[id] ?? 0] ??= []).push(id);
  const maxRows = Math.max(...Object.values(cols).map((c) => c.length));
  const height = PAD * 2 + maxRows * NODE_H + (maxRows - 1) * ROW_GAP;
  const nodes: Record<string, Node> = {};
  for (const [c, list] of Object.entries(cols)) {
    const colH = list.length * NODE_H + (list.length - 1) * ROW_GAP;
    list.forEach((id, r) => {
      const col = Number(c);
      nodes[id] = {
        id, col, row: r,
        kind: id === "input" ? "input" : id.startsWith("sink.") ? "sink" : "step",
        x: PAD + col * (NODE_W + COL_GAP),
        y: PAD + (height - PAD * 2 - colH) / 2 + r * (NODE_H + ROW_GAP),
      };
    });
  }
  const width = PAD * 2 + (Math.max(...Object.values(nodes).map((n) => n.col)) + 1) * (NODE_W + COL_GAP) - COL_GAP;
  return { nodes, width, height };
}

const statusColor = (s?: string) =>
  s === "bottleneck" ? "var(--bad)" : s === "busy" ? "var(--busy)" : s === "idle" ? "var(--idle)" : "var(--ok)";

export default function FlowGraph({ wf, m, selected, onSelect }: {
  wf: Workflow; m: Metrics | null; selected: string | null; onSelect: (id: string) => void;
}) {
  const { nodes, width, height } = layout(wf);
  const rate = (a: string, b: string) => m?.edges.find((e) => e.from === a && e.to === b)?.rate ?? 0;
  const maxRate = Math.max(0.001, ...(m?.edges.map((e) => e.rate) ?? [0]));

  return (
    <div>
      <svg viewBox={`0 0 ${width} ${height + 40}`} width="100%" style={{ maxHeight: 520, display: "block" }}
           preserveAspectRatio="xMidYMid meet" role="img" aria-label={`workflow ${wf.name}`}>
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="9" markerHeight="9" markerUnits="userSpaceOnUse" orient="auto-start-reverse">
            <path d="M0,0 L10,5 L0,10 z" fill="var(--muted)" />
          </marker>
        </defs>
        {wf.flow.map((e, i) => {
          const a = nodes[e.from], b = nodes[e.to];
          if (!a || !b) return null;
          const back = b.col <= a.col;
          const x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2;
          const dx = Math.max(40, (x2 - x1) / 2);
          const d = back
            ? `M${a.x + NODE_W / 2},${a.y + NODE_H} C${a.x + NODE_W / 2},${a.y + NODE_H + 60} ${b.x + NODE_W / 2},${b.y + NODE_H + 60} ${b.x + NODE_W / 2},${b.y + NODE_H}`
            : `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`;
          const r = rate(e.from, e.to);
          const w = 1.2 + 4 * (r / maxRate);
          const mx = back ? (a.x + b.x) / 2 + NODE_W / 2 : (x1 + x2) / 2;
          const my = back ? Math.max(a.y, b.y) + NODE_H + 48 : (y1 + y2) / 2;
          return (
            <g key={i}>
              <path d={d} fill="none" stroke="var(--muted)" strokeOpacity={r > 0 ? 0.9 : 0.35} strokeWidth={w} markerEnd="url(#arrow)" />
              <text x={mx} y={my - 6} textAnchor="middle" fontSize="11" fill="var(--muted)">
                {r > 0 ? fmtRate(r) : ""}{e.when ? `  ⟨${e.when.length > 26 ? e.when.slice(0, 25) + "…" : e.when}⟩` : ""}
              </text>
            </g>
          );
        })}
        {Object.values(nodes).map((n) => {
          const s = n.kind === "step" ? m?.steps[n.id] : undefined;
          const st = wf.steps[n.id];
          const sel = selected === n.id;
          const border = n.kind === "step" ? statusColor(s?.status) : "var(--border)";
          const util = s?.utilization ?? 0;
          return (
            <g key={n.id} transform={`translate(${n.x},${n.y})`} style={{ cursor: n.kind === "step" ? "pointer" : "default" }}
               onClick={() => n.kind === "step" && onSelect(n.id)}>
              <rect width={NODE_W} height={n.kind === "step" ? NODE_H : 54} y={n.kind === "step" ? 0 : (NODE_H - 54) / 2}
                    rx="10" fill="var(--panel)" stroke={sel ? "var(--accent)" : border} strokeWidth={sel ? 2.5 : n.kind === "step" ? 2 : 1} />
              {n.kind !== "step" ? (
                <text x={NODE_W / 2} y={NODE_H / 2 + 5} textAnchor="middle" fontSize="13" fill="var(--muted)">
                  {n.kind === "input" ? "▶ input" : n.id}
                </text>
              ) : (
                <>
                  <text x="12" y="22" fontSize="14" fontWeight="650" fill="var(--text)">{n.id}</text>
                  <text x={NODE_W - 12} y="22" fontSize="11" textAnchor="end" fill="var(--muted)">
                    {st.replicas}×{st.concurrency} slots
                  </text>
                  <text x="12" y="44" fontSize="12" fill="var(--muted)">
                    queue <tspan fill="var(--text)" fontWeight="600">{s?.queue ?? 0}</tspan>
                    {"   "}in service <tspan fill="var(--text)" fontWeight="600">{s?.inflight ?? 0}/{s?.capacity ?? st.replicas * st.concurrency}</tspan>
                  </text>
                  <text x="12" y="63" fontSize="12" fill="var(--muted)">
                    λ <tspan fill="var(--text)">{fmtRate(s?.arrival_rate)}</tspan>
                    {"   "}X <tspan fill="var(--text)">{fmtRate(s?.throughput)}</tspan>
                  </text>
                  <text x="12" y="82" fontSize="12" fill="var(--muted)">
                    wait <tspan fill="var(--text)">{fmtMs(s?.wait_ms)}</tspan>
                    {"   "}service <tspan fill="var(--text)">{fmtMs(s?.service_ms)}</tspan>
                  </text>
                  <rect x="12" y="94" width={NODE_W - 24} height="8" rx="4" fill="var(--panel-2)" />
                  <rect x="12" y="94" width={(NODE_W - 24) * Math.min(1, util)} height="8" rx="4" fill={statusColor(s?.status)} />
                  <text x={NODE_W - 12} y="113" fontSize="11" textAnchor="end" fill="var(--muted)">
                    ρ {fmtPct(s?.utilization)}{s?.dead_letters ? `  ·  ${s.dead_letters} dead` : ""}{s?.retries ? `  ·  ${s.retries} retries` : ""}
                  </text>
                </>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
