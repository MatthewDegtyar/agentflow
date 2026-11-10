"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { usePoll, useW, withW, type Health, type MonitoredWorkflow } from "@/lib/api";

const LINKS = [
  { href: "/", label: "Workflow" },
  { href: "/runs", label: "Runs" },
  { href: "/logs", label: "Logs" },
];

export default function Nav() {
  const path = usePathname();
  const router = useRouter();
  const w = useW();
  const { data, error } = usePoll<Health>("/health", 5000);
  const monitor = data?.mode === "monitor";
  const { data: wfs } = usePoll<MonitoredWorkflow[]>(monitor ? "/workflows" : null, 5000);
  const current = wfs?.find((x) => x.id === w);
  return (
    <nav className="nav">
      <Link href="/" className="brand" style={{ textDecoration: "none" }}>agentflow</Link>
      {monitor && (
        <select value={w ?? ""} onChange={(e) => router.push(e.target.value ? withW(path, e.target.value) : "/")} title="monitored workflow">
          <option value="">All workflows ({wfs?.length ?? data?.workflows ?? 0})</option>
          {(wfs ?? []).map((x) => <option key={x.id} value={x.id}>{x.live ? "● " : ""}{x.id}</option>)}
        </select>
      )}
      <span className="links">
        {(!monitor || w) && LINKS.map((l) => {
          const active = l.href === "/" ? path === "/" : path.startsWith(l.href);
          return (
            <Link key={l.href} href={withW(l.href, w)} className={active ? "active" : ""}>
              {l.label}
            </Link>
          );
        })}
      </span>
      <span className="status">
        {error ? <span className="error">runtime unreachable</span>
          : monitor ? (current ? <>{current.live ? "● running" : `○ ${current.status}`}</> : <>monitor · {data?.workflows ?? 0} work dirs</>)
          : data?.ok ? <>● {data.workflow}</> : "connecting…"}
      </span>
    </nav>
  );
}
