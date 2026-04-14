"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Activity, BrainCircuit, GitBranchPlus, House, MessageSquareWarning, Radar, SlidersHorizontal } from "lucide-react";

import { cn } from "@/lib/utils";

const navItems = [
  { href: "/", label: "Komuta Paneli", icon: House, note: "Operasyon özeti" },
  { href: "/accounts", label: "Hesaplar", icon: Radar, note: "Öncelik indeksi" },
  { href: "/graph", label: "İlişki Ağı", icon: GitBranchPlus, note: "Ağ inceleme" },
  { href: "/review-queue", label: "İnceleme Kuyruğu", icon: MessageSquareWarning, note: "Bayraklı akış" },
  { href: "/prompts", label: "Prompt Yönetimi", icon: SlidersHorizontal, note: "İç araçlar" },
  { href: "/operations", label: "Operasyon / Toplama", icon: Activity, note: "Çalıştırma ve izleme" },
];

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="min-h-screen overflow-x-hidden bg-transparent text-[var(--text-primary)]">
      <div className="mx-auto grid min-h-screen max-w-[1920px] grid-cols-[248px_minmax(0,1fr)]">
        <aside className="border-r border-[var(--border-default)] bg-[rgba(8,12,18,0.92)] px-4 py-4">
          <div className="flex h-[72px] items-center gap-3 border-b border-[var(--border-subtle)] px-2 pb-4">
            <div className="flex h-11 w-11 items-center justify-center rounded-[12px] border border-[var(--accent-border)] bg-[rgba(36,209,195,0.08)] text-[var(--accent-primary)]">
              <BrainCircuit className="h-5 w-5" />
            </div>
            <div>
              <div className="text-[20px] font-semibold tracking-[-0.04em] text-[var(--text-primary)]">RedKid</div>
              <div className="mt-1 text-[11px] font-bold uppercase tracking-[0.22em] text-[var(--text-muted)]">Komuta Arayüzü</div>
            </div>
          </div>

          <div className="mt-5">
            <div className="mb-3 px-2 text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-dim)]">Analist Menüsü</div>
            <nav className="space-y-2">
              {navItems.map((item) => {
                const Icon = item.icon;
                const active = pathname === item.href || (item.href !== "/" && pathname.startsWith(item.href));
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={cn(
                      "flex h-11 items-center gap-3 rounded-[12px] border px-3 transition-colors",
                      active
                        ? "border-[var(--accent-border)] bg-[var(--accent-soft)] text-[var(--text-primary)] shadow-[var(--glow-selection)]"
                        : "border-transparent text-[var(--text-secondary)] hover:border-[var(--border-subtle)] hover:bg-[rgba(255,255,255,0.03)] hover:text-[var(--text-primary)]",
                    )}
                  >
                    <Icon className="h-[18px] w-[18px]" />
                    <div className="min-w-0">
                      <div className="truncate text-[14px] font-semibold">{item.label}</div>
                      <div className="truncate text-[11px] text-[var(--text-dim)]">{item.note}</div>
                    </div>
                  </Link>
                );
              })}
            </nav>
          </div>
        </aside>

        <main className="min-w-0 overflow-x-hidden px-6 py-5 xl:px-6">{children}</main>
      </div>
    </div>
  );
}
