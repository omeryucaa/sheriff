"use client";

import { useEffect, useState } from "react";
import { Expand, X } from "lucide-react";

import { AppPanel, SectionTitle, StatusBadge, getThreatTone } from "@/components/ui";
import type { PostItem } from "@/lib/types";

function formatThreatLabel(value?: string | null) {
  return value ? `${value} tehdit` : "tehdit belirsiz";
}

function formatOrgLabel(value?: string | null) {
  return value && value !== "belirsiz" ? value : "örgüt belirsiz";
}

function formatCategoryLabel(value: string) {
  return value.replaceAll("_", " ");
}

function toRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function readStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item || "").trim()).filter(Boolean);
}

function parseDeepHighlights(observation: NonNullable<PostItem["media_observations"]>[number]): string[] {
  const deepPayload = toRecord(observation.deep_payload);
  const highlights: string[] = [];

  const location = toRecord(deepPayload.location_assessment);
  const locationText = String(location.candidate_location_text || "").trim();
  if (locationText && locationText !== "unclear") highlights.push(`Konum adayı: ${locationText}`);

  const vehicle = toRecord(deepPayload.vehicle_plate_assessment);
  const plateTexts = readStringList(vehicle.plate_text_candidates).filter((item) => item !== "unclear");
  if (plateTexts.length) highlights.push(`Plaka adayları: ${plateTexts.join(", ")}`);

  const sensitive = Array.isArray(deepPayload.sensitive_information) ? deepPayload.sensitive_information : [];
  const sensitiveNotes = sensitive
    .map((entry) => {
      const item = toRecord(entry);
      const typ = String(item.type || "").trim();
      const val = String(item.value || "").trim();
      if (!typ && !val) return "";
      if (typ && val) return `${typ}: ${val}`;
      return typ || val;
    })
    .filter(Boolean);
  if (sensitiveNotes.length) highlights.push(`Hassas bilgi: ${sensitiveNotes.join(" | ")}`);

  const followupPriority = String(deepPayload.followup_priority || "").trim();
  if (followupPriority && followupPriority !== "unclear") highlights.push(`Takip önceliği: ${followupPriority}`);

  const note = String(deepPayload.analyst_note_tr || "").trim();
  if (note && note !== "unclear") highlights.push(`Analist notu: ${note}`);

  return highlights;
}

function DeepReviewPanel({ post }: { post: PostItem }) {
  const observations = Array.isArray(post.media_observations) ? post.media_observations : [];
  if (!observations.length) return null;

  const deepReviewed = observations.filter((item) => item.deep_required || item.deep_status === "completed" || item.deep_status === "failed");
  if (!deepReviewed.length) return null;

  return (
    <div className="mt-4 rounded-[12px] border border-[var(--border-subtle)] bg-[rgba(10,17,26,0.72)] px-3 py-3">
      <div className="flex flex-wrap items-center gap-2">
        <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">Detayli Tekrar Inceleme</div>
        <StatusBadge tone="info" mono>
          {deepReviewed.length} medya
        </StatusBadge>
      </div>

      <div className="mt-3 space-y-3">
        {deepReviewed.map((observation, index) => {
          const mediaIndex = Number(observation.media_index || index + 1);
          const deepStatus = String(observation.deep_status || "not_required");
          const statusTone = deepStatus === "completed" ? "success" : deepStatus === "failed" ? "danger" : "neutral";
          const highlights = parseDeepHighlights(observation);
          return (
            <div key={`deep-observation-${post.id}-${mediaIndex}-${index}`} className="rounded-[10px] border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.01)] p-3">
              <div className="flex flex-wrap items-center gap-2">
                <StatusBadge tone="neutral" mono>
                  Medya #{mediaIndex}
                </StatusBadge>
                <StatusBadge tone={statusTone}>{deepStatus}</StatusBadge>
                {observation.location_confidence ? <StatusBadge tone="info">lokasyon {String(observation.location_confidence)}</StatusBadge> : null}
                {observation.contains_vehicle ? <StatusBadge tone="warning">arac var</StatusBadge> : null}
                {observation.contains_plate ? <StatusBadge tone="warning">plaka gorunur</StatusBadge> : null}
              </div>

              {observation.deep_reason ? (
                <div className="mt-2 text-[12px] leading-6 text-[var(--text-secondary)]">
                  Sebep: {String(observation.deep_reason)}
                </div>
              ) : null}

              {highlights.length ? (
                <div className="mt-2 space-y-1">
                  {highlights.map((line) => (
                    <div key={`${mediaIndex}-${line}`} className="text-[12px] leading-6 text-[var(--text-primary)]">
                      • {line}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="mt-2 text-[12px] leading-6 text-[var(--text-muted)]">Ek derin bulgu cikmadi.</div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function PostMedia({ post }: { post: PostItem }) {
  const mediaItems = post.media_items?.length
    ? post.media_items
    : [{ media_type: post.media_type, media_url: post.media_url }];
  const [activeIndex, setActiveIndex] = useState(0);
  const [overlayOpen, setOverlayOpen] = useState(false);
  const activeItem = mediaItems[Math.min(activeIndex, mediaItems.length - 1)];

  useEffect(() => {
    if (!overlayOpen) return undefined;
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") setOverlayOpen(false);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [overlayOpen]);

  return (
    <>
      <div className="space-y-3">
        <button
          type="button"
          onClick={() => setOverlayOpen(true)}
          className="group relative block w-full overflow-hidden rounded-[var(--radius-card)] border border-[var(--border-subtle)] bg-[rgba(10,17,26,0.94)] text-left"
        >
          {activeItem.media_type === "video" ? (
            <video className="h-56 w-full object-cover transition-transform duration-200 group-hover:scale-[1.01]" controls src={activeItem.media_url} />
          ) : (
            // eslint-disable-next-line @next/next/no-img-element
            <img src={activeItem.media_url} alt={post.post_ozet || "post"} className="h-56 w-full object-cover transition-transform duration-200 group-hover:scale-[1.01]" />
          )}
          <span className="absolute right-3 top-3 inline-flex h-9 w-9 items-center justify-center rounded-[12px] border border-[var(--border-default)] bg-[rgba(5,7,10,0.72)] text-[var(--text-primary)]">
            <Expand className="h-4 w-4" />
          </span>
        </button>

        {mediaItems.length > 1 ? (
          <div className="space-y-2">
            <div data-mono="true" className="text-[12px] text-[var(--text-muted)]">
              Medya {activeIndex + 1}/{mediaItems.length}
            </div>
            <div className="grid grid-cols-4 gap-2">
              {mediaItems.map((item, index) => (
                <button
                  key={`${item.media_url}-${index}`}
                  type="button"
                  onClick={() => setActiveIndex(index)}
                  className={`overflow-hidden rounded-[12px] border transition ${
                    index === activeIndex
                      ? "border-[var(--accent-border)] bg-[var(--accent-soft)]"
                      : "border-[var(--border-default)] bg-[rgba(255,255,255,0.02)]"
                  }`}
                >
                  {item.media_type === "video" ? (
                    <video className="h-16 w-full object-cover" muted src={item.media_url} />
                  ) : (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img src={item.media_url} alt={`post media ${index + 1}`} className="h-16 w-full object-cover" />
                  )}
                </button>
              ))}
            </div>
          </div>
        ) : null}
      </div>

      {overlayOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-[rgba(3,6,10,0.88)] p-4" onClick={() => setOverlayOpen(false)}>
          <div
            className="relative max-h-[92vh] w-full max-w-[1100px] overflow-hidden rounded-[var(--radius-overlay)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.98)] p-4 shadow-[var(--shadow-overlay)]"
            onClick={(event) => event.stopPropagation()}
          >
            <button
              type="button"
              onClick={() => setOverlayOpen(false)}
              className="absolute right-4 top-4 z-10 inline-flex h-10 w-10 items-center justify-center rounded-[12px] border border-[var(--border-default)] bg-[rgba(5,7,10,0.72)] text-[var(--text-primary)]"
            >
              <X className="h-4 w-4" />
            </button>
            <div className="overflow-hidden rounded-[16px] bg-[rgba(5,7,10,0.72)]">
              {activeItem.media_type === "video" ? (
                <video className="max-h-[82vh] w-full object-contain" controls autoPlay src={activeItem.media_url} />
              ) : (
                // eslint-disable-next-line @next/next/no-img-element
                <img src={activeItem.media_url} alt={post.post_ozet || "post"} className="max-h-[82vh] w-full object-contain" />
              )}
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}

export function PostTimeline({ posts }: { posts: PostItem[] }) {
  return (
    <AppPanel>
      <SectionTitle eyebrow="Kanıt Zaman Çizgisi" title="Post Zaman Çizgisi" description="Post özeti, tehdit seviyesi ve analitik bağlam birlikte incelenir." />
      <div className="mt-5 space-y-4">
        {posts.map((post) => (
          <div key={post.id} className="rounded-[var(--radius-card)] border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] p-4">
            <div className="grid gap-5 xl:grid-cols-[240px_1fr]">
              <PostMedia post={post} />
              <div>
                <div className="flex flex-wrap items-center gap-2">
                  <StatusBadge tone={getThreatTone(post.tehdit_seviyesi)}>{formatThreatLabel(post.tehdit_seviyesi)}</StatusBadge>
                  <StatusBadge tone={post.tespit_edilen_orgut && post.tespit_edilen_orgut !== "belirsiz" ? "warning" : "neutral"}>
                    {formatOrgLabel(post.tespit_edilen_orgut)}
                  </StatusBadge>
                  {(post.media_observations || []).some((item) => item.deep_required || item.deep_status === "completed" || item.deep_status === "failed") ? (
                    <StatusBadge tone="info">detay inceleme</StatusBadge>
                  ) : null}
                  {post.icerik_kategorisi.map((item) => (
                    <StatusBadge key={item} tone="neutral">
                      {formatCategoryLabel(item)}
                    </StatusBadge>
                  ))}
                </div>

                <p className="mt-4 text-[15px] leading-7 text-[var(--text-primary)]">{post.post_ozet || post.post_analysis}</p>

                <div className="mt-4 grid gap-2 sm:grid-cols-[220px_minmax(0,1fr)]">
                  <div className="rounded-[12px] border border-[var(--border-subtle)] bg-[rgba(10,17,26,0.74)] px-3 py-3">
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">Önem / Medya</div>
                    <div data-mono="true" className="mt-2 text-[13px] text-[var(--text-secondary)]">
                      {post.onem_skoru || 0} / {post.media_items?.length || 1}
                    </div>
                  </div>
                  <div className="rounded-[12px] border border-[var(--border-subtle)] bg-[rgba(10,17,26,0.74)] px-3 py-3">
                    <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">Paylaşım Metni</div>
                    <div className="mt-2 text-[13px] leading-6 text-[var(--text-secondary)]">{post.caption || "-"}</div>
                  </div>
                </div>

                <DeepReviewPanel post={post} />
              </div>
            </div>
          </div>
        ))}
      </div>
    </AppPanel>
  );
}
