"use client";

import { useSyncExternalStore } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip, Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts";

import { ChartDatum } from "@/lib/types";
import { AppPanel, SectionTitle } from "@/components/ui";

const COLORS = [
  "var(--chart-accent)",
  "var(--chart-warning)",
  "var(--chart-danger)",
  "var(--chart-info)",
  "var(--chart-neutral)",
  "var(--chart-success)",
];

function useIsClient() {
  return useSyncExternalStore(
    () => () => undefined,
    () => true,
    () => false,
  );
}

function ChartTooltip() {
  return (
    <Tooltip
      contentStyle={{
        background: "#0e151d",
        border: "1px solid rgba(148, 163, 184, 0.14)",
        borderRadius: 16,
        color: "#e6edf5",
      }}
      labelStyle={{ color: "#a7b3c2" }}
      itemStyle={{ color: "#e6edf5" }}
    />
  );
}

export function PiePanel({ title, data }: { title: string; data: ChartDatum[] }) {
  const mounted = useIsClient();
  const total = data.reduce((sum, item) => sum + item.value, 0);

  return (
    <AppPanel className="min-h-[400px] overflow-hidden">
      <SectionTitle eyebrow="Distribution" title={title} />
      <div className="mt-5 grid gap-5 xl:grid-cols-[220px_minmax(0,1fr)] xl:items-center">
        <div className="relative h-[220px] xl:h-[228px]">
          {mounted ? (
            <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={220}>
              <PieChart>
                <Pie data={data} dataKey="value" nameKey="name" innerRadius={58} outerRadius={88} paddingAngle={2} stroke="rgba(7,16,25,0.9)" strokeWidth={3}>
                  {data.map((entry, index) => (
                    <Cell key={entry.name} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <ChartTooltip />
              </PieChart>
            </ResponsiveContainer>
          ) : null}
          <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
            <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">Toplam</div>
            <div data-mono="true" className="mt-2 text-[28px] font-bold tracking-[-0.04em] text-[var(--text-primary)]">
              {total}
            </div>
          </div>
        </div>
        <div className="space-y-2 xl:max-h-[228px] xl:overflow-auto xl:pr-1">
          {data.map((item, index) => (
            <div key={item.name} className="flex items-center justify-between rounded-[12px] border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] px-3 py-2">
              <div className="flex min-w-0 items-center gap-3">
                <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: COLORS[index % COLORS.length] }} />
                <span className="truncate text-[14px] text-[var(--text-secondary)]">{item.name}</span>
              </div>
              <span data-mono="true" className="ml-3 shrink-0 text-[13px] font-semibold text-[var(--text-primary)]">
                {item.value}
              </span>
            </div>
          ))}
        </div>
      </div>
    </AppPanel>
  );
}

export function BarPanel({ title, data }: { title: string; data: ChartDatum[] }) {
  const mounted = useIsClient();

  return (
    <AppPanel className="min-h-[320px] overflow-hidden">
      <SectionTitle eyebrow="Threat Signal" title={title} />
      <div className="mt-5 h-[228px]">
        {mounted ? (
          <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={228}>
            <BarChart data={data} margin={{ left: 0, right: 8, top: 10, bottom: 0 }}>
              <CartesianGrid stroke="rgba(148,163,184,0.08)" vertical={false} />
              <XAxis dataKey="name" stroke="#7c8998" fontSize={11} tickLine={false} axisLine={false} />
              <YAxis stroke="#7c8998" fontSize={11} tickLine={false} axisLine={false} />
              <ChartTooltip />
              <Bar dataKey="value" radius={[6, 6, 0, 0]} fill="var(--chart-accent)" />
            </BarChart>
          </ResponsiveContainer>
        ) : null}
      </div>
    </AppPanel>
  );
}
