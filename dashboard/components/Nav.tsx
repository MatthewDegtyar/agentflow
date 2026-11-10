"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { usePoll } from "@/lib/api";

const LINKS = [
  { href: "/", label: "Workflow" },
  { href: "/runs", label: "Runs" },
  { href: "/logs", label: "Logs" },
];

export default function Nav() {
  const path = usePathname();
  const { data, error } = usePoll<{ ok: boolean; workflow: string | null }>("/health", 5000);
  return (
    <nav className="nav">
      <span className="brand">agentflow</span>
      <span className="links">
        {LINKS.map((l) => {
          const active = l.href === "/" ? path === "/" : path.startsWith(l.href);
          return (
            <Link key={l.href} href={l.href} className={active ? "active" : ""}>
              {l.label}
            </Link>
          );
        })}
      </span>
      <span className="status">
        {error ? <span className="error">runtime unreachable</span>
          : data?.ok ? <>● {data.workflow}</> : "connecting…"}
      </span>
    </nav>
  );
}
