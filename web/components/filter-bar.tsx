"use client";

import { ReactNode, useEffect, useRef } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { FieldLabel, SearchInput, SelectControl } from "@/components/ui";

export function FilterBar({
  action,
  children,
  autoSubmit = false,
}: {
  action?: string;
  children: ReactNode;
  autoSubmit?: boolean;
}) {
  const formRef = useRef<HTMLFormElement | null>(null);
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (!autoSubmit) return undefined;
    const form = formRef.current;
    if (!form) return undefined;

    let timeoutId: number | undefined;

    const submitForm = (target?: EventTarget | null) => {
      if (!(target instanceof HTMLElement)) return;
      const formData = new FormData(form);
      const next = new URLSearchParams();

      for (const [key, value] of formData.entries()) {
        const normalized = String(value).trim();
        if (normalized) next.set(key, normalized);
      }

      const nextQuery = next.toString();
      const currentQuery = searchParams.toString();
      if (nextQuery === currentQuery) return;
      router.replace(nextQuery ? `${pathname}?${nextQuery}` : pathname);
    };

    const onInput = (event: Event) => {
      if (timeoutId) window.clearTimeout(timeoutId);
      timeoutId = window.setTimeout(() => submitForm(event.target), 220);
    };

    const onChange = (event: Event) => {
      if (timeoutId) window.clearTimeout(timeoutId);
      submitForm(event.target);
    };

    form.addEventListener("input", onInput);
    form.addEventListener("change", onChange);

    return () => {
      if (timeoutId) window.clearTimeout(timeoutId);
      form.removeEventListener("input", onInput);
      form.removeEventListener("change", onChange);
    };
  }, [autoSubmit, pathname, router, searchParams]);

  return (
    <section data-surface="panel" className="rounded-[var(--radius-panel)] p-[var(--card-padding)]">
      <form
        ref={formRef}
        action={action}
        className="grid gap-4 xl:grid-cols-[minmax(240px,1.2fr)_repeat(3,minmax(180px,0.7fr))]"
      >
        {children}
      </form>
    </section>
  );
}

export function FilterField({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <label className="block">
      <FieldLabel>{label}</FieldLabel>
      {children}
    </label>
  );
}

export { SearchInput as FilterInput, SelectControl as FilterSelect };
