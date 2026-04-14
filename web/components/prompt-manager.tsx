"use client";

import { useMemo, useState, useTransition } from "react";

import { updatePromptTemplate } from "@/lib/api";
import { PromptTemplate } from "@/lib/types";
import { AppPanel, Button, SectionTitle, StatusBadge, TextAreaControl } from "@/components/ui";

type DiffRow = {
  left?: string;
  right?: string;
  type: "same" | "added" | "removed" | "changed";
};

function buildDiffRows(baseText: string, currentText: string): DiffRow[] {
  const baseLines = baseText.split("\n");
  const currentLines = currentText.split("\n");
  const max = Math.max(baseLines.length, currentLines.length);
  const rows: DiffRow[] = [];

  for (let index = 0; index < max; index += 1) {
    const left = baseLines[index];
    const right = currentLines[index];
    if (left === right) rows.push({ left, right, type: "same" });
    else if (left === undefined) rows.push({ right, type: "added" });
    else if (right === undefined) rows.push({ left, type: "removed" });
    else rows.push({ left, right, type: "changed" });
  }

  return rows;
}

function diffTone(type: DiffRow["type"]) {
  if (type === "added") return "border-[rgba(70,211,154,0.18)] bg-[rgba(70,211,154,0.08)]";
  if (type === "removed") return "border-[rgba(255,100,124,0.18)] bg-[rgba(255,100,124,0.08)]";
  if (type === "changed") return "border-[rgba(233,196,106,0.18)] bg-[rgba(233,196,106,0.08)]";
  return "border-transparent bg-transparent";
}

export function PromptManager({ initialItems }: { initialItems: PromptTemplate[] }) {
  const [items, setItems] = useState(initialItems);
  const [selectedKey, setSelectedKey] = useState(initialItems[0]?.key || "");
  const [draft, setDraft] = useState(initialItems[0]?.content || "");
  const [isPending, startTransition] = useTransition();
  const [status, setStatus] = useState<string>("");

  const selected = useMemo(
    () => items.find((item) => item.key === selectedKey) || items[0],
    [items, selectedKey],
  );
  const diffRows = useMemo(() => buildDiffRows(selected?.default_content || "", draft), [selected, draft]);

  const persist = (payload: { content: string; reset_to_default?: boolean }) => {
    if (!selected) return;
    setStatus("");
    startTransition(async () => {
      try {
        const updated = await updatePromptTemplate(selected.key, {
          content: payload.content,
          is_enabled: true,
          reset_to_default: payload.reset_to_default,
        });
        setItems((prev) => prev.map((item) => (item.key === updated.key ? updated : item)));
        setDraft(updated.content);
        setStatus("Prompt kaydedildi.");
      } catch (error) {
        setStatus(error instanceof Error ? error.message : "Kaydetme başarısız.");
      }
    });
  };

  if (!selected) {
    return <AppPanel>Prompt bulunamadı.</AppPanel>;
  }

  return (
    <div className="grid gap-6 xl:grid-cols-[300px_minmax(0,1fr)]">
      <AppPanel className="h-fit">
        <SectionTitle eyebrow="İstem Kayıtları" title="İstem Listesi" description="Operasyonel istem anahtarları ve ezme durumu." />
        <div className="mt-5 space-y-2">
          {items.map((item) => (
            <button
              key={item.key}
              type="button"
              className={`w-full rounded-[var(--radius-card)] border px-4 py-3 text-left transition ${
                item.key === selected.key
                  ? "border-[var(--accent-border)] bg-[var(--accent-soft)] text-[var(--text-primary)] shadow-[var(--glow-selection)]"
                  : "border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] text-[var(--text-secondary)] hover:bg-[rgba(255,255,255,0.03)]"
              }`}
              onClick={() => {
                setSelectedKey(item.key);
                setDraft(item.content);
                setStatus("");
              }}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="font-semibold">{item.display_name}</div>
                  <div data-mono="true" className="mt-1 text-[12px] text-[var(--text-dim)]">
                    {item.key}
                  </div>
                </div>
                <StatusBadge tone={item.is_overridden ? "warning" : "neutral"}>{item.is_overridden ? "ezildi" : "varsayılan"}</StatusBadge>
              </div>
              <div className="mt-3 line-clamp-2 text-[13px] leading-6 text-[var(--text-muted)]">{item.description}</div>
            </button>
          ))}
        </div>
      </AppPanel>

      <div className="space-y-6">
        <AppPanel>
          <SectionTitle
            eyebrow="İstem Detayı"
            title={selected.display_name}
            description={selected.description || "Aynı anahtar için sürüm, ezme ve fark durumu burada izlenir."}
            action={
              <div className="flex flex-wrap gap-2">
                <StatusBadge tone={selected.is_overridden ? "warning" : "success"}>{selected.is_overridden ? "ezme aktif" : "varsayılan"}</StatusBadge>
                <StatusBadge tone="neutral" mono>
                  v{selected.version}
                </StatusBadge>
              </div>
            }
          />
        </AppPanel>

        <div className="grid gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
          <AppPanel>
            <SectionTitle eyebrow="Düzenleyici" title="Güncel İstem" />
            <TextAreaControl
              data-mono="true"
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
              className="mt-5 h-[480px] text-[13px] leading-7"
            />
          </AppPanel>

          <AppPanel>
            <SectionTitle
              eyebrow="Farklar"
              title="Varsayılan Karşılaştırma"
              action={
                <div className="flex flex-wrap gap-2">
                  <StatusBadge tone="neutral">aynı</StatusBadge>
                  <StatusBadge tone="warning">değişti</StatusBadge>
                  <StatusBadge tone="success">eklendi</StatusBadge>
                  <StatusBadge tone="danger">silindi</StatusBadge>
                </div>
              }
            />
            <div className="mt-5 h-[480px] overflow-auto rounded-[var(--radius-card)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)]">
              <div className="grid grid-cols-2 border-b border-[var(--border-default)] px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">
                <div>Varsayılan</div>
                <div>Güncel</div>
              </div>
              <div className="space-y-1 p-2">
                {diffRows.map((row, index) => (
                  <div key={`${row.type}-${index}`} className={`grid grid-cols-2 gap-2 rounded-[12px] border ${diffTone(row.type)}`}>
                    <pre data-mono="true" className="overflow-auto border-r border-[var(--border-subtle)] px-3 py-2 whitespace-pre-wrap text-[12px] leading-6 text-[var(--text-muted)]">
                      {row.left ?? ""}
                    </pre>
                    <pre data-mono="true" className="overflow-auto px-3 py-2 whitespace-pre-wrap text-[12px] leading-6 text-[var(--text-primary)]">
                      {row.right ?? ""}
                    </pre>
                  </div>
                ))}
              </div>
            </div>
          </AppPanel>
        </div>

        <AppPanel>
          <div className="flex flex-wrap items-start justify-between gap-4">
            <SectionTitle eyebrow="İş Akışı" title="Kaydet ve Önizle" />
            <div className="flex flex-wrap gap-3">
              <Button tone="primary" onClick={() => persist({ content: draft })} disabled={isPending}>
                {isPending ? "Kaydediliyor..." : "Kaydet"}
              </Button>
              <Button tone="secondary" onClick={() => persist({ content: selected.default_content, reset_to_default: true })} disabled={isPending}>
                Varsayılana Dön
              </Button>
            </div>
          </div>

          <div className="mt-5 grid gap-6 xl:grid-cols-2">
            <div className="rounded-[var(--radius-card)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)] p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">Canlı Önizleme</div>
              <pre data-mono="true" className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap text-[12px] leading-6 text-[var(--text-primary)]">
                {draft}
              </pre>
            </div>
            <div className="rounded-[var(--radius-card)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.94)] p-4">
              <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">Durum</div>
              <div className="mt-3 space-y-3 text-[13px] text-[var(--text-secondary)]">
                <div data-mono="true">Son güncelleme: {selected.updated_at}</div>
                <div>Kaynak: {selected.is_overridden ? "Veritabanı ezmesi" : "Varsayılan istem"}</div>
                {status ? <div className="text-[var(--accent-primary)]">{status}</div> : null}
              </div>
            </div>
          </div>
        </AppPanel>
      </div>
    </div>
  );
}
