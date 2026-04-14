import Link from "next/link";
import { ReactNode } from "react";
import { Search } from "lucide-react";

import { cn } from "@/lib/utils";

export type Tone = "neutral" | "accent" | "success" | "warning" | "danger" | "info" | "persona";

const toneStyles: Record<Tone, string> = {
  neutral: "border-[rgba(148,163,184,0.18)] bg-[rgba(148,163,184,0.08)] text-[var(--text-secondary)]",
  accent: "border-[var(--accent-border)] bg-[var(--accent-soft)] text-[var(--text-primary)]",
  success: "border-[rgba(70,211,154,0.26)] bg-[rgba(70,211,154,0.12)] text-[#b5f0d4]",
  warning: "border-[rgba(233,196,106,0.28)] bg-[rgba(233,196,106,0.12)] text-[#f5deb0]",
  danger: "border-[rgba(255,100,124,0.3)] bg-[rgba(255,100,124,0.12)] text-[#ffc1cb]",
  info: "border-[rgba(90,174,250,0.28)] bg-[rgba(90,174,250,0.12)] text-[#b8dcff]",
  persona: "border-[rgba(155,140,255,0.28)] bg-[rgba(155,140,255,0.12)] text-[#d8d0ff]",
};

export function getThreatTone(value?: string | null): Tone {
  if (!value) return "neutral";
  if (value === "kritik" || value === "yuksek" || value === "open") return "danger";
  if (value === "orta" || value === "dusuk" || value === "review") return "warning";
  if (value === "healthy" || value === "validated" || value === "closed") return "success";
  return "accent";
}

export function PageHeader({
  eyebrow,
  title,
  description,
  action,
}: {
  eyebrow?: string;
  title: string;
  description?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
      <div className="max-w-4xl">
        {eyebrow ? (
          <div className="text-[11px] font-bold uppercase tracking-[0.22em] text-[var(--text-muted)]">{eyebrow}</div>
        ) : null}
        <h1 className="mt-3 text-[40px] font-bold leading-[1.08] tracking-[-0.045em] text-[var(--text-primary)] xl:text-[44px]">
          {title}
        </h1>
        {description ? (
          <p className="mt-4 max-w-3xl text-[17px] leading-[1.58] text-[var(--text-secondary)]">{description}</p>
        ) : null}
      </div>
      {action ? <div className="flex items-center gap-3">{action}</div> : null}
    </div>
  );
}

export const SectionHeader = PageHeader;

export function AppPanel({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <section
      data-surface="panel"
      className={cn("rounded-[var(--radius-panel)] p-[var(--card-padding)]", className)}
    >
      {children}
    </section>
  );
}

export const Panel = AppPanel;
export const DataCard = AppPanel;

export function SectionTitle({
  eyebrow,
  title,
  description,
  action,
}: {
  eyebrow?: string;
  title: string;
  description?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-start justify-between gap-3">
      <div>
        {eyebrow ? (
          <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">{eyebrow}</div>
        ) : null}
        <h2 className="mt-2 text-[22px] font-semibold tracking-[-0.03em] text-[var(--text-primary)]">{title}</h2>
        {description ? <p className="mt-2 text-[14px] leading-6 text-[var(--text-secondary)]">{description}</p> : null}
      </div>
      {action}
    </div>
  );
}

export function MetricCard({
  label,
  value,
  hint,
  tone = "neutral",
}: {
  label: string;
  value: string | number;
  hint?: string;
  tone?: Tone;
}) {
  return (
    <AppPanel className="flex min-h-[var(--kpi-min-height)] flex-col justify-between">
      <div className="flex items-center justify-between gap-3">
        <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">{label}</div>
        <span className={cn("h-2.5 w-2.5 rounded-full", tone === "danger" ? "bg-[var(--danger)]" : tone === "warning" ? "bg-[var(--warning)]" : tone === "success" ? "bg-[var(--success)]" : "bg-[var(--accent-primary)]")} />
      </div>
      <div className="mt-5" data-mono="true">
        <div className="text-[36px] font-bold leading-none tracking-[-0.05em] text-[var(--text-primary)]">{value}</div>
      </div>
      {hint ? <div className="mt-4 text-[13px] leading-6 text-[var(--text-secondary)]">{hint}</div> : null}
    </AppPanel>
  );
}

export const StatCard = MetricCard;

export function StatusBadge({
  children,
  tone = "neutral",
  mono = false,
}: {
  children: ReactNode;
  tone?: Tone;
  mono?: boolean;
}) {
  return (
    <span
      data-mono={mono ? "true" : undefined}
      className={cn(
        "inline-flex min-h-6 items-center rounded-[12px] border px-2.5 py-1 text-[12px] font-semibold tracking-[0.04em]",
        toneStyles[tone],
      )}
    >
      {children}
    </span>
  );
}

export const Badge = StatusBadge;

export function DataRow({
  label,
  value,
  mono = false,
}: {
  label: string;
  value: ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex items-start justify-between gap-4 border-b border-[var(--border-subtle)] py-3 last:border-b-0">
      <div className="text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">{label}</div>
      <div data-mono={mono ? "true" : undefined} className="max-w-[62%] text-right text-[14px] font-medium text-[var(--text-primary)]">
        {value}
      </div>
    </div>
  );
}

export function Button({
  children,
  tone = "secondary",
  href,
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  children: ReactNode;
  tone?: "primary" | "secondary" | "quiet";
  href?: string;
}) {
  const classes = cn(
    "inline-flex h-[var(--control-height)] items-center justify-center rounded-[var(--radius-control)] px-4 text-[14px] font-semibold transition-colors",
    tone === "primary"
      ? "border border-[var(--accent-border)] bg-[var(--accent-primary)] text-[#031013] hover:bg-[#39ddd0]"
      : tone === "quiet"
        ? "text-[var(--text-secondary)] hover:bg-[rgba(255,255,255,0.04)] hover:text-[var(--text-primary)]"
        : "border border-[var(--border-default)] bg-[rgba(255,255,255,0.03)] text-[var(--text-primary)] hover:bg-[var(--bg-hover)]",
    className,
  );

  if (href) {
    return (
      <Link href={href} className={classes}>
        {children}
      </Link>
    );
  }

  return (
    <button {...props} className={classes}>
      {children}
    </button>
  );
}

export function FieldLabel({ children }: { children: ReactNode }) {
  return <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">{children}</div>;
}

export function SearchInput(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <div className="relative">
      <Search className="pointer-events-none absolute left-4 top-1/2 h-4 w-4 -translate-y-1/2 text-[var(--text-dim)]" />
      <input
        {...props}
        className={cn(
          "h-[var(--control-height)] w-full rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)] pl-11 pr-4 text-[14px] text-[var(--text-primary)] outline-none placeholder:text-[var(--text-dim)] focus:border-[var(--accent-border)] focus:ring-2 focus:ring-[rgba(36,209,195,0.12)]",
          props.className,
        )}
      />
    </div>
  );
}

export function InputControl(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      {...props}
      className={cn(
        "h-[var(--control-height)] w-full rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)] px-4 text-[14px] text-[var(--text-primary)] outline-none placeholder:text-[var(--text-dim)] focus:border-[var(--accent-border)] focus:ring-2 focus:ring-[rgba(36,209,195,0.12)]",
        props.className,
      )}
    />
  );
}

export function SelectControl(props: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select
      {...props}
      className={cn(
        "h-[var(--control-height)] w-full rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)] px-4 text-[14px] text-[var(--text-primary)] outline-none focus:border-[var(--accent-border)] focus:ring-2 focus:ring-[rgba(36,209,195,0.12)]",
        props.className,
      )}
    />
  );
}

export function TextAreaControl(props: React.TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      {...props}
      className={cn(
        "w-full rounded-[var(--radius-card)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)] px-4 py-3 text-[14px] leading-6 text-[var(--text-primary)] outline-none focus:border-[var(--accent-border)] focus:ring-2 focus:ring-[rgba(36,209,195,0.12)]",
        props.className,
      )}
    />
  );
}

export function AnalystListItem({
  title,
  subtitle,
  summary,
  meta,
  status,
  href,
  selected = false,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  summary?: ReactNode;
  meta?: ReactNode;
  status?: ReactNode;
  href?: string;
  selected?: boolean;
}) {
  const content = (
    <div
      className={cn(
        "relative grid min-h-[var(--list-row-height)] gap-4 rounded-[var(--radius-card)] border px-4 py-4 transition-colors",
        selected
          ? "border-[var(--accent-border)] bg-[rgba(36,209,195,0.08)] shadow-[var(--glow-selection)]"
          : "border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] hover:bg-[rgba(255,255,255,0.035)]",
      )}
    >
      {selected ? <span className="absolute inset-y-3 left-0 w-0.5 rounded-full bg-[var(--accent-primary)]" /> : null}
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="text-[15px] font-semibold text-[var(--text-primary)]">{title}</div>
          {subtitle ? <div className="mt-1 text-[12px] text-[var(--text-muted)]">{subtitle}</div> : null}
          {summary ? <div className="mt-3 text-[14px] leading-6 text-[var(--text-secondary)]">{summary}</div> : null}
        </div>
        {(meta || status) ? (
          <div className="flex shrink-0 flex-col items-end gap-2">
            {status}
            {meta ? <div className="text-right text-[12px] text-[var(--text-muted)]">{meta}</div> : null}
          </div>
        ) : null}
      </div>
    </div>
  );

  if (href) {
    return <Link href={href}>{content}</Link>;
  }

  return content;
}

export function EvidencePanel({
  title,
  eyebrow,
  children,
  action,
  className,
}: {
  title: string;
  eyebrow?: string;
  children: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <AppPanel className={className}>
      <SectionTitle eyebrow={eyebrow} title={title} action={action} />
      <div className="mt-5">{children}</div>
    </AppPanel>
  );
}

export function DetailSidebar({
  title,
  eyebrow,
  children,
}: {
  title: string;
  eyebrow?: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-[var(--radius-panel)] border border-[var(--border-default)] bg-[rgba(12,18,27,0.86)] p-[var(--card-padding)]">
      <SectionTitle eyebrow={eyebrow} title={title} />
      <div className="mt-5 space-y-4">{children}</div>
    </div>
  );
}

type DenseTableColumn<T> = {
  key: string;
  header: string;
  className?: string;
  align?: "left" | "right";
  render: (item: T) => ReactNode;
};

export function DenseTable<T>({
  columns,
  items,
  getKey,
  getRowHref,
}: {
  columns: DenseTableColumn<T>[];
  items: T[];
  getKey: (item: T) => string | number;
  getRowHref?: (item: T) => string;
}) {
  return (
    <AppPanel className="overflow-hidden p-0">
      <div className="overflow-x-auto">
        <table className="w-full min-w-[880px] border-collapse">
          <thead>
            <tr className="border-b border-[var(--border-default)]">
              {columns.map((column) => (
                <th
                  key={column.key}
                  className={cn(
                    "h-[var(--table-header-height)] px-4 text-left text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]",
                    column.align === "right" ? "text-right" : "text-left",
                    column.className,
                  )}
                >
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {items.map((item) => {
              const rowContent = columns.map((column) => (
                <td
                  key={column.key}
                  className={cn(
                    "h-[var(--table-row-height)] px-4 align-middle text-[14px] text-[var(--text-secondary)]",
                    column.align === "right" ? "text-right" : "text-left",
                    column.className,
                  )}
                >
                  {column.render(item)}
                </td>
              ));

              const href = getRowHref?.(item);
              return (
                <tr key={getKey(item)} className="border-b border-[var(--border-subtle)] last:border-b-0 hover:bg-[rgba(255,255,255,0.03)]">
                  {href ? (
                    rowContent.map((cell, index) => (
                      <td key={`${String(getKey(item))}-${index}`} className="p-0">
                        <Link href={href} className="block h-full px-4 py-3">
                          {(cell as unknown as { props: { children: ReactNode } }).props.children}
                        </Link>
                      </td>
                    ))
                  ) : (
                    rowContent
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </AppPanel>
  );
}
