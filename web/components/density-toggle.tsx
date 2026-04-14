"use client";

import { useEffect, useState } from "react";

import { cn } from "@/lib/utils";

type DensityMode = "comfortable" | "compact";

const STORAGE_KEY = "redkid-density-mode";

export function DensityToggle() {
  const [mode, setMode] = useState<DensityMode>(() => {
    if (typeof window === "undefined") return "comfortable";
    const stored = window.localStorage.getItem(STORAGE_KEY) as DensityMode | null;
    return stored === "compact" ? "compact" : "comfortable";
  });

  useEffect(() => {
    document.documentElement.dataset.density = mode;
    window.localStorage.setItem(STORAGE_KEY, mode);
  }, [mode]);

  return (
    <div className="inline-flex h-11 items-center gap-1 rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(12,18,27,0.92)] p-1 text-[12px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">
      {(["comfortable", "compact"] as DensityMode[]).map((item) => (
        <button
          key={item}
          type="button"
          onClick={() => setMode(item)}
          className={cn(
            "inline-flex h-full items-center rounded-[10px] px-3 transition-colors",
            mode === item
              ? "border border-[var(--accent-border)] bg-[var(--accent-soft)] text-[var(--text-primary)] shadow-[var(--glow-selection)]"
              : "text-[var(--text-secondary)] hover:bg-[rgba(255,255,255,0.03)]",
          )}
        >
          {item === "comfortable" ? "Rahat" : "Sıkı"}
        </button>
      ))}
    </div>
  );
}
