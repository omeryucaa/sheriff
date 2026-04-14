"use client";

import { FormEvent, useEffect, useState, useTransition } from "react";
import { Activity, CheckCircle2, Clock3, GitBranch, Images, Layers3, MessageSquareText, Radio } from "lucide-react";

import { apiBaseUrl, createBatchJob, getIngestTrace, getJobsOverview, jobsOverviewWebSocketUrl, runIngestWorkersOnce } from "@/lib/api";
import {
  BatchJob,
  BatchJobCreateResponse,
  BatchJobTarget,
  IngestJobEvent,
  IngestJob,
  IngestWorkersRunOnceResponse,
  JobsOverview,
  JobsOverviewStreamPayload,
} from "@/lib/types";
import {
  AppPanel,
  Button,
  FieldLabel,
  InputControl,
  MetricCard,
  SectionTitle,
  SelectControl,
  StatusBadge,
  TextAreaControl,
} from "@/components/ui";

type RuntimeTask = {
  key: string;
  postLabel: string;
  sourceLabel?: string;
  laneLabel: string;
  actorLabel?: string;
  startedAt: string;
  endedAt?: string | null;
  status: "running" | "completed";
};

type RuntimeSnapshot = {
  activePosts: RuntimeTask[];
  completedPosts: RuntimeTask[];
  activeMedia: RuntimeTask[];
  completedMedia: RuntimeTask[];
  activeComments: RuntimeTask[];
  completedComments: RuntimeTask[];
  activeMerges: RuntimeTask[];
  completedMerges: RuntimeTask[];
};

function statusTone(status?: string | null): "neutral" | "warning" | "danger" | "success" | "info" {
  if (!status) return "neutral";
  if (status === "completed") return "success";
  if (status === "running") return "info";
  if (status === "suggested") return "info";
  if (status === "failed" || status === "missing_archive" || status === "partial") return "danger";
  if (status === "queued" || status === "pending" || status === "enqueued" || status === "retry_wait") return "warning";
  return "neutral";
}

function renderTimestamp(value?: string | null) {
  if (!value) return "-";
  return value.replace("T", " ").replace("Z", "");
}

function statusLabel(status?: string | null) {
  if (!status) return "-";
  const map: Record<string, string> = {
    running: "çalışıyor",
    completed: "tamamlandı",
    suggested: "öneri",
    failed: "hata",
    missing_archive: "arşiv yok",
    partial: "kısmi",
    queued: "kuyrukta",
    pending: "bekliyor",
    enqueued: "kuyruğa alındı",
    retry_wait: "yeniden deneme bekliyor",
    skipped: "atlanmış",
    open: "açık",
    closed: "kapalı",
  };
  return map[status] || status;
}

function sortTargets(items: BatchJobTarget[]) {
  const order = new Map([
    ["running", 0],
    ["enqueued", 1],
    ["pending", 2],
    ["suggested", 3],
    ["failed", 3],
    ["missing_archive", 4],
    ["completed", 5],
    ["skipped", 6],
  ]);
  return [...items].sort((left, right) => {
    const leftOrder = order.get(left.status) ?? 99;
    const rightOrder = order.get(right.status) ?? 99;
    if (leftOrder !== rightOrder) return leftOrder - rightOrder;
    return right.id - left.id;
  });
}

function sortIngestJobs(items: IngestJob[]) {
  const order = new Map([
    ["running", 0],
    ["pending", 1],
    ["retry_wait", 2],
    ["failed", 3],
    ["completed", 4],
    ["skipped", 5],
  ]);
  return [...items].sort((left, right) => {
    const leftOrder = order.get(left.status) ?? 99;
    const rightOrder = order.get(right.status) ?? 99;
    if (leftOrder !== rightOrder) return leftOrder - rightOrder;
    return right.id - left.id;
  });
}

function describeJobProgress(item: IngestJob) {
  const bits: string[] = [];
  if (item.current_post_index && item.total_posts) {
    bits.push(`post ${item.current_post_index}/${item.total_posts}`);
  }
  if (item.current_media_index && item.total_media_items) {
    bits.push(`medya ${item.current_media_index}/${item.total_media_items}`);
  }
  if (item.current_comment_index && item.total_comments) {
    bits.push(`yorum ${item.current_comment_index}/${item.total_comments}`);
  }
  if (item.current_post_id) {
    bits.push(`post_id ${item.current_post_id}`);
  }
  if (item.current_commenter_username) {
    bits.push(`@${item.current_commenter_username}`);
  }
  return bits.join(" • ");
}

function eventTone(eventType: string): "neutral" | "warning" | "danger" | "success" | "info" {
  if (eventType.includes("failed") || eventType.includes("error")) return "danger";
  if (eventType.includes("review") || eventType.includes("followup")) return "warning";
  if (eventType.includes("completed") || eventType.includes("finished")) return "success";
  if (eventType.includes("started") || eventType.includes("claimed")) return "info";
  return "neutral";
}

function buildEventPostParts(item: IngestJobEvent) {
  let postLabel = "gönderi";
  if (item.post_index && item.post_total) {
    postLabel = `gönderi ${item.post_index}/${item.post_total}`;
  } else if (item.post_index) {
    postLabel = `gönderi ${item.post_index}`;
  }
  return {
    postLabel,
    sourceLabel: item.source_post_id || undefined,
  };
}

function truncateMiddle(value: string, start = 12, end = 8) {
  if (value.length <= start + end + 1) return value;
  return `${value.slice(0, start)}...${value.slice(-end)}`;
}

function parseTimestampMs(value?: string | null) {
  if (!value) return Date.now();
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function formatDurationMs(durationMs: number) {
  const totalSeconds = Math.max(0, Math.floor(durationMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes <= 0) return `${seconds}s`;
  if (minutes < 60) return `${minutes}m ${seconds}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function formatTaskDuration(task: RuntimeTask, nowMs: number) {
  const startMs = parseTimestampMs(task.startedAt);
  const endMs = task.endedAt ? parseTimestampMs(task.endedAt) : nowMs;
  return formatDurationMs(Math.max(0, endMs - startMs));
}

function describeRuntimeTask(task: RuntimeTask, nowMs: number) {
  const parts = [task.postLabel, task.laneLabel];
  if (task.sourceLabel) parts.push(truncateMiddle(task.sourceLabel));
  if (task.actorLabel) parts.push(task.actorLabel);
  parts.push(formatTaskDuration(task, nowMs));
  return parts.join(" • ");
}

function buildJobRuntimeSnapshots(events: IngestJobEvent[]) {
  const byJob = new Map<
    number,
    {
      posts: Map<string, RuntimeTask>;
      media: Map<string, RuntimeTask>;
      comments: Map<string, RuntimeTask>;
      merges: Map<string, RuntimeTask>;
      completedPosts: RuntimeTask[];
      completedMedia: RuntimeTask[];
      completedComments: RuntimeTask[];
      completedMerges: RuntimeTask[];
    }
  >();

  const ensure = (jobId: number) => {
    const existing = byJob.get(jobId);
    if (existing) return existing;
    const created = {
      posts: new Map<string, RuntimeTask>(),
      media: new Map<string, RuntimeTask>(),
      comments: new Map<string, RuntimeTask>(),
      merges: new Map<string, RuntimeTask>(),
      completedPosts: [] as RuntimeTask[],
      completedMedia: [] as RuntimeTask[],
      completedComments: [] as RuntimeTask[],
      completedMerges: [] as RuntimeTask[],
    };
    byJob.set(jobId, created);
    return created;
  };

  const pushCompletedTask = (target: RuntimeTask[], task: RuntimeTask) => {
    target.unshift(task);
    if (target.length > 8) {
      target.splice(8);
    }
  };

  const sorted = [...events].sort((left, right) => left.id - right.id);
  for (const event of sorted) {
    const state = ensure(event.ingest_job_id);
    const postKey = event.source_post_id || `post-${event.post_index || "x"}`;
    const { postLabel, sourceLabel } = buildEventPostParts(event);
    const createdAt = event.created_at;

    if (event.event_type === "post_started") {
      state.posts.set(postKey, {
        key: postKey,
        postLabel,
        sourceLabel,
        laneLabel: "gönderi",
        startedAt: createdAt,
        status: "running",
      });
      continue;
    }
    if (event.event_type === "post_pipeline_completed" || event.event_type === "post_failed") {
      const existing = state.posts.get(postKey);
      if (existing) {
        pushCompletedTask(state.completedPosts, {
          ...existing,
          endedAt: createdAt,
          status: "completed",
        });
      }
      state.posts.delete(postKey);
      for (const key of [...state.media.keys()]) {
        if (key.startsWith(`${postKey}|`)) state.media.delete(key);
      }
      for (const key of [...state.comments.keys()]) {
        if (key.startsWith(`${postKey}|`)) state.comments.delete(key);
      }
      state.merges.delete(`${postKey}|merge`);
      continue;
    }
    if (event.event_type === "single_media_started") {
      state.media.set(`${postKey}|media|${event.media_index || "x"}`, {
        key: `${postKey}|media|${event.media_index || "x"}`,
        postLabel,
        sourceLabel,
        laneLabel: `medya ${event.media_index || "?"}/${event.media_total || "?"}`,
        startedAt: createdAt,
        status: "running",
      });
      continue;
    }
    if (event.event_type === "single_media_completed") {
      const taskKey = `${postKey}|media|${event.media_index || "x"}`;
      const existing = state.media.get(taskKey);
      pushCompletedTask(
        state.completedMedia,
        existing || {
          key: taskKey,
          postLabel,
          sourceLabel,
          laneLabel: `medya ${event.media_index || "?"}/${event.media_total || "?"}`,
          startedAt: createdAt,
          status: "completed",
        },
      );
      if (state.completedMedia[0]) {
        state.completedMedia[0].endedAt = createdAt;
        state.completedMedia[0].status = "completed";
      }
      state.media.delete(taskKey);
      continue;
    }
    if (event.event_type === "comment_started") {
      state.comments.set(
        `${postKey}|yorum|${event.comment_index || "x"}|${event.commenter_username || ""}`,
        {
          key: `${postKey}|yorum|${event.comment_index || "x"}|${event.commenter_username || ""}`,
          postLabel,
          sourceLabel,
          laneLabel: `yorum ${event.comment_index || "?"}/${event.comment_total || "?"}`,
          actorLabel: event.commenter_username ? `@${event.commenter_username}` : undefined,
          startedAt: createdAt,
          status: "running",
        },
      );
      continue;
    }
    if (event.event_type === "comment_completed") {
      const taskKey = `${postKey}|yorum|${event.comment_index || "x"}|${event.commenter_username || ""}`;
      const existing = state.comments.get(taskKey);
      pushCompletedTask(
        state.completedComments,
        existing || {
          key: taskKey,
          postLabel,
          sourceLabel,
          laneLabel: `yorum ${event.comment_index || "?"}/${event.comment_total || "?"}`,
          actorLabel: event.commenter_username ? `@${event.commenter_username}` : undefined,
          startedAt: createdAt,
          status: "completed",
        },
      );
      if (state.completedComments[0]) {
        state.completedComments[0].endedAt = createdAt;
        state.completedComments[0].status = "completed";
      }
      state.comments.delete(taskKey);
      continue;
    }
    if (event.event_type === "post_merge_started") {
      state.merges.set(`${postKey}|merge`, {
        key: `${postKey}|merge`,
        postLabel,
        sourceLabel,
        laneLabel: "üst birleştirme",
        startedAt: createdAt,
        status: "running",
      });
      continue;
    }
    if (event.event_type === "post_completed" && event.stage === "post_parent_merge") {
      const taskKey = `${postKey}|birlestirme`;
      const existing = state.merges.get(taskKey);
      pushCompletedTask(
        state.completedMerges,
        existing || {
          key: taskKey,
          postLabel,
          sourceLabel,
          laneLabel: "üst birleştirme",
          startedAt: createdAt,
          status: "completed",
        },
      );
      if (state.completedMerges[0]) {
        state.completedMerges[0].endedAt = createdAt;
        state.completedMerges[0].status = "completed";
      }
      state.merges.delete(taskKey);
    }
  }

  return new Map<number, RuntimeSnapshot>(
    [...byJob.entries()].map(([jobId, value]) => [
      jobId,
      {
        activePosts: [...value.posts.values()],
        completedPosts: value.completedPosts,
        activeMedia: [...value.media.values()],
        completedMedia: value.completedMedia,
        activeComments: [...value.comments.values()],
        completedComments: value.completedComments,
        activeMerges: [...value.merges.values()],
        completedMerges: value.completedMerges,
      },
    ]),
  );
}

function summarizeWorkerTopology(jobs: IngestJob[], snapshots: Map<number, RuntimeSnapshot>) {
  const runningJobs = jobs.filter((item) => item.status === "running");
  const queuedJobs = jobs.filter((item) => ["pending", "enqueued", "retry_wait"].includes(item.status));
  const workers = new Map<
    string,
    {
      leaseOwner: string;
      jobs: IngestJob[];
      activePosts: number;
      activeMedia: number;
      activeComments: number;
      activeMerges: number;
    }
  >();

  for (const job of runningJobs) {
    const leaseOwner = job.lease_owner || `workerless-${job.id}`;
    const existing = workers.get(leaseOwner) || {
      leaseOwner,
      jobs: [],
      activePosts: 0,
      activeMedia: 0,
      activeComments: 0,
      activeMerges: 0,
    };
    existing.jobs.push(job);
    const snapshot = snapshots.get(job.id);
    existing.activePosts += snapshot?.activePosts.length || 0;
    existing.activeMedia += snapshot?.activeMedia.length || 0;
    existing.activeComments += snapshot?.activeComments.length || 0;
    existing.activeMerges += snapshot?.activeMerges.length || 0;
    workers.set(leaseOwner, existing);
  }

  return {
    activeWorkers: workers.size,
    runningJobsCount: runningJobs.length,
    queuedJobsCount: queuedJobs.length,
    activePosts: [...snapshots.values()].reduce((sum, item) => sum + item.activePosts.length, 0),
    activeMedia: [...snapshots.values()].reduce((sum, item) => sum + item.activeMedia.length, 0),
    activeComments: [...snapshots.values()].reduce((sum, item) => sum + item.activeComments.length, 0),
    activeMerges: [...snapshots.values()].reduce((sum, item) => sum + item.activeMerges.length, 0),
    workers: [...workers.values()].sort((left, right) => right.jobs.length - left.jobs.length),
  };
}

function collectTasks(
  snapshots: Map<number, RuntimeSnapshot>,
  jobsById: Map<number, IngestJob>,
  kind: "activeMedia" | "completedMedia" | "activeComments" | "completedComments" | "activeMerges" | "completedMerges",
) {
  const items: Array<{ task: RuntimeTask; job?: IngestJob }> = [];
  for (const [jobId, snapshot] of snapshots.entries()) {
    const tasks = snapshot[kind] as RuntimeTask[];
    for (const task of tasks) {
      items.push({ task, job: jobsById.get(jobId) });
    }
  }
  return items.sort((left, right) => parseTimestampMs(right.task.startedAt) - parseTimestampMs(left.task.startedAt));
}

function resolveStageIndex(item: IngestJob) {
  if (item.status === "completed") return 6;
  if (item.status === "failed") return 5;
  const stage = item.current_stage || "";
  if (stage === "worker") return 0;
  if (stage === "post_read" || stage === "media_prepare" || stage === "comment_prepare" || stage === "post_analysis") return 1;
  if (stage === "single_media_post_analysis") return 2;
  if (stage === "post_parent_merge") return 3;
  if (stage === "comment_analysis") return 4;
  if (stage === "followup" || stage === "review_queue") return 5;
  if (stage === "post_complete" || stage === "done") return 6;
  return 0;
}

function EmptyStateCard({ children }: { children: string }) {
  return (
    <div className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] px-4 py-3 text-[13px] text-[var(--text-muted)]">
      {children}
    </div>
  );
}

function BatchJobSummary({ item }: { item: BatchJob }) {
  return (
    <div className="rounded-[16px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div data-mono="true" className="text-[14px] font-semibold text-[var(--text-primary)]">
            batch #{item.id}
          </div>
      <div className="mt-1 text-[12px] text-[var(--text-muted)]">
            {(item.mode === "all" ? "tümü" : item.mode === "seed_only" ? "yalnız tohum" : item.mode)} • arşiv {item.bucket}
            {item.country ? ` • ${item.country}` : ""}
            {item.focus_entity ? ` • odak ${item.focus_entity}` : ""}
          </div>
        </div>
        <StatusBadge tone={statusTone(item.status)}>{statusLabel(item.status)}</StatusBadge>
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        <StatusBadge tone="info" mono>{item.running_targets} çalışan</StatusBadge>
        <StatusBadge tone="warning" mono>{item.pending_targets} kuyrukta</StatusBadge>
        <StatusBadge tone="success" mono>{item.completed_targets} tamamlandı</StatusBadge>
        <StatusBadge tone="danger" mono>{item.failed_targets} sorun</StatusBadge>
        <StatusBadge tone="neutral" mono>{item.discovered_followups} takip</StatusBadge>
        <StatusBadge tone={item.auto_enqueue_followups ? "warning" : "info"} mono>
          {item.auto_enqueue_followups ? "otomatik kuyruğa alım açık" : "yalnız öneri"}
        </StatusBadge>
      </div>
      <div className="mt-3 text-[12px] leading-6 text-[var(--text-secondary)]">
        Hedefler: {item.requested_targets.join(", ") || "-"}
      </div>
    </div>
  );
}

function TargetFlowCard({ item }: { item: BatchJobTarget }) {
  return (
    <div className="rounded-[16px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div data-mono="true" className="text-[14px] font-semibold text-[var(--text-primary)]">
            @{item.normalized_username}
          </div>
          <div className="mt-1 text-[12px] text-[var(--text-muted)]">
            {item.source_kind}
            {item.parent_username ? ` • parent @${item.parent_username}` : ""}
            {item.ingest_job_id ? ` • job #${item.ingest_job_id}` : ""}
          </div>
        </div>
        <StatusBadge tone={statusTone(item.status)}>{statusLabel(item.status)}</StatusBadge>
      </div>
      {item.note ? <div className="mt-3 text-[12px] leading-6 text-[var(--text-secondary)]">{item.note}</div> : null}
    </div>
  );
}

function IngestJobCard({ item, snapshot, nowMs }: { item: IngestJob; snapshot?: RuntimeSnapshot; nowMs: number }) {
  const progress = describeJobProgress(item);
  return (
    <div className="rounded-[16px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div data-mono="true" className="text-[14px] font-semibold text-[var(--text-primary)]">
            job #{item.id} • @{item.target_username}
          </div>
          <div className="mt-1 text-[12px] text-[var(--text-muted)]">
            {item.source_kind || "ilk hedef"}
            {item.parent_username ? ` • parent @${item.parent_username}` : ""}
            {item.focus_entity ? ` • odak ${item.focus_entity}` : ""}
          </div>
        </div>
        <StatusBadge tone={statusTone(item.status)}>{statusLabel(item.status)}</StatusBadge>
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        <StatusBadge tone="info" mono>{item.processed_posts} post</StatusBadge>
        <StatusBadge tone="info" mono>{item.processed_comments} yorum</StatusBadge>
        <StatusBadge tone="danger" mono>{item.flagged_users} bayraklı</StatusBadge>
        {snapshot?.activeMedia.length ? <StatusBadge tone="warning" mono>{snapshot.activeMedia.length} aktif medya</StatusBadge> : null}
        {snapshot?.activeComments.length ? <StatusBadge tone="warning" mono>{snapshot.activeComments.length} aktif yorum</StatusBadge> : null}
        {snapshot?.activeMerges.length ? <StatusBadge tone="accent" mono>{snapshot.activeMerges.length} aktif merge</StatusBadge> : null}
      </div>
      <div className="mt-3 text-[12px] leading-6 text-[var(--text-secondary)]">
        Çalıştırma: <span data-mono="true">{item.run_id}</span> • işleyici {item.lease_owner || "-"} • başlangıç {renderTimestamp(item.started_at)} • bitiş {renderTimestamp(item.finished_at)}
      </div>
      <StageRail item={item} />
      {item.current_event ? (
        <div className="mt-3 rounded-[12px] border border-[rgba(98,197,255,0.18)] bg-[rgba(98,197,255,0.06)] px-3 py-2 text-[12px] leading-6 text-[var(--text-secondary)]">
          <div className="font-semibold text-[var(--text-primary)]">
            {statusLabel(item.current_stage || "running")}
            {item.last_event_at ? ` • ${renderTimestamp(item.last_event_at)}` : ""}
          </div>
          <div className="mt-1">{item.current_event}</div>
          {progress ? <div className="mt-1 text-[var(--text-muted)]">{progress}</div> : null}
        </div>
      ) : null}
      {snapshot && (snapshot.activePosts.length || snapshot.activeMedia.length || snapshot.activeComments.length || snapshot.activeMerges.length) ? (
        <div className="mt-3 rounded-[12px] border border-[rgba(36,209,195,0.18)] bg-[rgba(36,209,195,0.06)] px-3 py-3 text-[12px] leading-6 text-[var(--text-secondary)]">
          <div className="font-semibold text-[var(--text-primary)]">Canlı alt işler</div>
          {snapshot.activePosts.length ? <div className="mt-1">Aktif postlar: {snapshot.activePosts.map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}</div> : null}
          {snapshot.activeMedia.length ? <div className="mt-1">Aktif medya işleri: {snapshot.activeMedia.map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}</div> : null}
          {snapshot.activeComments.length ? <div className="mt-1">Aktif yorum işleri: {snapshot.activeComments.map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}</div> : null}
          {snapshot.activeMerges.length ? <div className="mt-1">Aktif merge işleri: {snapshot.activeMerges.map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}</div> : null}
          {snapshot.completedMedia.length ? <div className="mt-1 text-[#b5f0d4]">Biten medya işleri: {snapshot.completedMedia.slice(0, 3).map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}</div> : null}
        </div>
      ) : null}
      {item.error_message ? (
        <div className="mt-3 rounded-[12px] border border-[rgba(255,100,124,0.28)] bg-[rgba(255,100,124,0.08)] px-3 py-2 text-[12px] leading-6 text-[#ffc1cb]">
          {item.error_message}
        </div>
      ) : null}
    </div>
  );
}

function WorkerLaneCard({
  leaseOwner,
  jobs,
  snapshots,
  nowMs,
}: {
  leaseOwner: string;
  jobs: IngestJob[];
  snapshots: Map<number, RuntimeSnapshot>;
  nowMs: number;
}) {
  const activeMedia = jobs.reduce((sum, item) => sum + (snapshots.get(item.id)?.activeMedia.length || 0), 0);
  const activeComments = jobs.reduce((sum, item) => sum + (snapshots.get(item.id)?.activeComments.length || 0), 0);
  const activeMerges = jobs.reduce((sum, item) => sum + (snapshots.get(item.id)?.activeMerges.length || 0), 0);

  return (
    <div className="rounded-[16px] border border-[rgba(36,209,195,0.18)] bg-[rgba(36,209,195,0.05)] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div data-mono="true" className="text-[14px] font-semibold text-[var(--text-primary)]">
            {leaseOwner}
          </div>
          <div className="mt-1 text-[12px] text-[var(--text-muted)]">
            Bu işleyici şu an {jobs.length} hesap işi taşıyor.
          </div>
        </div>
        <StatusBadge tone="info" mono>{jobs.length} aktif iş</StatusBadge>
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        <StatusBadge tone="warning" mono>{activeMedia} aktif medya</StatusBadge>
        <StatusBadge tone="warning" mono>{activeComments} aktif yorum</StatusBadge>
        <StatusBadge tone="accent" mono>{activeMerges} aktif birleştirme</StatusBadge>
      </div>
      <div className="mt-4 grid gap-3">
        {jobs.map((job) => {
          const snapshot = snapshots.get(job.id);
          return (
            <div key={job.id} className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.03)] px-4 py-3">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div data-mono="true" className="text-[13px] font-semibold text-[var(--text-primary)]">
                  job #{job.id} • @{job.target_username}
                </div>
                <StatusBadge tone={statusTone(job.status)} mono>{statusLabel(job.current_stage || job.status)}</StatusBadge>
              </div>
              <div className="mt-2 text-[12px] leading-6 text-[var(--text-secondary)]">
                {job.current_event || "İş akışı devam ediyor."}
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <StatusBadge tone="neutral" mono>{snapshot?.activePosts.length || 0} gönderi hattı</StatusBadge>
                <StatusBadge tone="warning" mono>{snapshot?.activeMedia.length || 0} medya hattı</StatusBadge>
                <StatusBadge tone="warning" mono>{snapshot?.activeComments.length || 0} yorum hattı</StatusBadge>
                <StatusBadge tone="accent" mono>{snapshot?.activeMerges.length || 0} birleştirme hattı</StatusBadge>
              </div>
              {snapshot?.activeMedia.length ? (
                <div className="mt-3 text-[12px] leading-6 text-[var(--text-muted)]">
                  {snapshot.activeMedia.map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}
                </div>
              ) : null}
              {snapshot?.completedMedia.length ? (
                <div className="mt-2 text-[12px] leading-6 text-[#b5f0d4]">
                  {snapshot.completedMedia.slice(0, 2).map((task) => describeRuntimeTask(task, nowMs)).join(" | ")}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function StageRail({ item }: { item: IngestJob }) {
  const currentIndex = resolveStageIndex(item);
  const stages = [
    { label: "sahiplen", icon: Radio },
    { label: "gönderi", icon: Layers3 },
    { label: "media", icon: Images },
    { label: "birleştirme", icon: GitBranch },
    { label: "yorum", icon: MessageSquareText },
    { label: "takip", icon: Activity },
    { label: "tamam", icon: CheckCircle2 },
  ];

  return (
    <div className="mt-3 flex flex-wrap gap-2">
      {stages.map((stage, index) => {
        const Icon = stage.icon;
        const tone =
          item.status === "completed" || index < currentIndex
            ? "success"
            : index === currentIndex && item.status === "running"
              ? "info"
              : "neutral";
        return (
          <StatusBadge key={stage.label} tone={tone}>
            <span className="flex items-center gap-1.5">
              <Icon className="h-3.5 w-3.5" />
              {stage.label}
            </span>
          </StatusBadge>
        );
      })}
    </div>
  );
}

function TaskTile({
  task,
  job,
  nowMs,
  tone,
}: {
  task: RuntimeTask;
  job?: IngestJob;
  nowMs: number;
  tone: "info" | "warning" | "success" | "accent";
}) {
  const isCompleted = task.status === "completed";
  return (
    <div className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.03)] px-4 py-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <div className="text-[14px] font-semibold text-[var(--text-primary)]">{task.postLabel}</div>
          <div className="mt-1 flex flex-wrap gap-2">
            <StatusBadge tone={isCompleted ? "success" : tone} mono>
              {task.laneLabel}
            </StatusBadge>
            {task.sourceLabel ? (
              <StatusBadge tone="neutral" mono>
                {truncateMiddle(task.sourceLabel)}
              </StatusBadge>
            ) : null}
          </div>
        </div>
        <StatusBadge tone={isCompleted ? "success" : tone}>
          <span className="flex items-center gap-1.5">
            {isCompleted ? <CheckCircle2 className="h-3.5 w-3.5" /> : <Clock3 className="h-3.5 w-3.5" />}
            {formatTaskDuration(task, nowMs)}
          </span>
        </StatusBadge>
      </div>
      <div className="mt-3 flex flex-wrap gap-2 text-[12px] text-[var(--text-secondary)]">
        <span data-mono="true" className="rounded-full border border-[var(--border-subtle)] px-2.5 py-1 text-[var(--text-muted)]">
          {job ? `job #${job.id}` : "job ?"}
        </span>
        {job ? (
          <span className="rounded-full border border-[var(--border-subtle)] px-2.5 py-1 text-[var(--text-muted)]">
            @{job.target_username}
          </span>
        ) : null}
        {task.actorLabel ? (
          <span className="rounded-full border border-[var(--border-subtle)] px-2.5 py-1 text-[var(--text-muted)]">
            {task.actorLabel}
          </span>
        ) : null}
      </div>
    </div>
  );
}

function EventCard({ item, job }: { item: IngestJobEvent; job?: IngestJob }) {
  const context: string[] = [];
  if (job) context.push(`job #${job.id}`);
  if (job) context.push(`@${job.target_username}`);
  if (item.post_index && item.post_total) context.push(`gönderi ${item.post_index}/${item.post_total}`);
  if (item.media_index && item.media_total) context.push(`medya ${item.media_index}/${item.media_total}`);
  if (item.comment_index && item.comment_total) context.push(`yorum ${item.comment_index}/${item.comment_total}`);
  if (item.source_post_id) context.push(truncateMiddle(item.source_post_id));
  if (item.commenter_username) context.push(`@${item.commenter_username}`);

  return (
    <div className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] px-4 py-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap gap-2">
          {context.length ? (
            context.map((entry) => (
              <span
                key={entry}
                data-mono={entry.startsWith("job #") || entry.startsWith("gönderi ") ? "true" : undefined}
                className="rounded-full border border-[var(--border-subtle)] px-2.5 py-1 text-[11px] text-[var(--text-muted)]"
              >
                {entry}
              </span>
            ))
          ) : (
            <span className="rounded-full border border-[var(--border-subtle)] px-2.5 py-1 text-[11px] text-[var(--text-muted)]">
              {item.stage || "olay"}
            </span>
          )}
        </div>
        <StatusBadge tone={eventTone(item.event_type)} mono>{renderTimestamp(item.created_at)}</StatusBadge>
      </div>
      <div className="mt-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
        {item.stage || item.event_type}
      </div>
      <div className="mt-2 text-[14px] leading-6 text-[var(--text-primary)]">{item.message}</div>
    </div>
  );
}

export function IngestConsole({
  initialTrace,
  initialOverview,
}: {
  initialTrace: string;
  initialOverview: JobsOverview;
}) {
  const [targetsText, setTargetsText] = useState("https://www.instagram.com/cdk.liege/\ncdk.liege");
  const [mode, setMode] = useState<"all" | "seed_only">("all");
  const [country, setCountry] = useState("be");
  const [bucket, setBucket] = useState("instagram-archive");
  const [focusEntity, setFocusEntity] = useState("PKK");
  const [workerMaxJobs, setWorkerMaxJobs] = useState("10");
  const [trace, setTrace] = useState(initialTrace);
  const [overview, setOverview] = useState<JobsOverview>(initialOverview);
  const [lastBatch, setLastBatch] = useState<BatchJobCreateResponse | null>(null);
  const [lastWorkerRun, setLastWorkerRun] = useState<IngestWorkersRunOnceResponse | null>(null);
  const [error, setError] = useState("");
  const [socketStatus, setSocketStatus] = useState<"connecting" | "live" | "retrying" | "offline">("connecting");
  const [socketServerTime, setSocketServerTime] = useState<string | null>(null);
  const [nowMs, setNowMs] = useState<number>(0);
  const [isPending, startTransition] = useTransition();

  const refreshAll = () => {
    startTransition(async () => {
      try {
        const [traceResponse, overviewResponse] = await Promise.all([getIngestTrace(), getJobsOverview({ limit: 30 })]);
        setTrace(traceResponse.content);
        setOverview(overviewResponse);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Operasyon durumu okunamadı.");
      }
    });
  };

  useEffect(() => {
    const timer = window.setInterval(() => {
      setNowMs(Date.now());
    }, 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let closed = false;
    let retryTimer: number | null = null;
    let socket: WebSocket | null = null;

    const connect = () => {
      if (closed) return;
      setSocketStatus((current) => (current === "live" ? "retrying" : "connecting"));
      socket = new WebSocket(`${jobsOverviewWebSocketUrl}?limit=40&interval_ms=1000`);

      socket.onopen = () => {
        setSocketStatus("live");
        setError("");
      };

      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data) as JobsOverviewStreamPayload;
          setOverview(payload.overview);
          setSocketServerTime(payload.server_time);
        } catch {
          setSocketStatus("retrying");
        }
      };

      socket.onerror = () => {
        setSocketStatus("retrying");
      };

      socket.onclose = () => {
        if (closed) return;
        setSocketStatus("retrying");
        retryTimer = window.setTimeout(connect, 1500);
      };
    };

    connect();
    return () => {
      closed = true;
      setSocketStatus("offline");
      if (retryTimer) window.clearTimeout(retryTimer);
      socket?.close();
    };
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => {
      startTransition(async () => {
        try {
          const traceResponse = await getIngestTrace();
          setTrace(traceResponse.content);
        } catch {
          // Keep the last trace content if the periodic refresh fails.
        }
      });
    }, 12000);
    return () => window.clearInterval(timer);
  }, []);

  const onBatchSubmit = (event: FormEvent) => {
    event.preventDefault();
    setError("");
    startTransition(async () => {
      try {
        const targets = targetsText
          .split(/\r?\n/)
          .map((item) => item.trim())
          .filter(Boolean);
        const response = await createBatchJob({
          targets,
          mode,
          country: country || undefined,
          bucket: bucket || undefined,
          focus_entity: focusEntity || undefined,
        });
        setLastBatch(response);
        const [traceResponse, overviewResponse] = await Promise.all([getIngestTrace(), getJobsOverview({ limit: 30 })]);
        setTrace(traceResponse.content);
        setOverview(overviewResponse);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Batch oluşturulamadı.");
      }
    });
  };

  const onRunWorkers = () => {
    setError("");
    startTransition(async () => {
      try {
        const response = await runIngestWorkersOnce({
          max_jobs: Number(workerMaxJobs) || undefined,
        });
        setLastWorkerRun(response);
        const [traceResponse, overviewResponse] = await Promise.all([getIngestTrace(), getJobsOverview({ limit: 30 })]);
        setTrace(traceResponse.content);
        setOverview(overviewResponse);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Worker turu çalıştırılamadı.");
      }
    });
  };

  const runningTargets = sortTargets(overview.targets).slice(0, 18);
  const runningJobs = sortIngestJobs(overview.ingest_jobs).slice(0, 18);
  const jobsById = new Map(overview.ingest_jobs.map((item) => [item.id, item]));
  const recentEvents = overview.recent_events.slice(0, 80);
  const runtimeSnapshots = buildJobRuntimeSnapshots(overview.recent_events);
  const workerTopology = summarizeWorkerTopology(overview.ingest_jobs, runtimeSnapshots);
  const activeMediaTasks = collectTasks(runtimeSnapshots, jobsById, "activeMedia").slice(0, 12);
  const completedMediaTasks = collectTasks(runtimeSnapshots, jobsById, "completedMedia").slice(0, 12);
  const activeCommentTasks = collectTasks(runtimeSnapshots, jobsById, "activeComments").slice(0, 12);
  const completedCommentTasks = collectTasks(runtimeSnapshots, jobsById, "completedComments").slice(0, 12);
  const activeMergeTasks = collectTasks(runtimeSnapshots, jobsById, "activeMerges").slice(0, 8);
  const completedMergeTasks = collectTasks(runtimeSnapshots, jobsById, "completedMerges").slice(0, 8);

  return (
    <div className="space-y-6">
      <div className="grid gap-6 xl:grid-cols-[440px_minmax(0,1fr)]">
        <div className="space-y-6">
          <AppPanel>
            <SectionTitle
              eyebrow="Toplu İş Kontrolü"
              title="İş Kuyruğu Başlat"
              description="Instagram hedeflerini toplu olarak kuyruğa yaz, odak örgütünü belirt ve işleyici akışını ayrı kontrol et."
            />
            <form className="mt-5 space-y-4" onSubmit={onBatchSubmit}>
              <div>
                <FieldLabel>API Uç Noktası</FieldLabel>
                <div data-mono="true" className="rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] px-4 py-3 text-[13px] text-[var(--text-secondary)]">
                  {apiBaseUrl}
                </div>
              </div>
              <div>
                <FieldLabel>Hedefler</FieldLabel>
                <TextAreaControl
                  value={targetsText}
                  onChange={(event) => setTargetsText(event.target.value)}
                  rows={6}
                  placeholder={"https://www.instagram.com/cdk.liege/\ncdk.liege"}
                />
              </div>
              <div className="grid gap-4 md:grid-cols-2">
                <div>
                  <FieldLabel>Mod</FieldLabel>
                  <SelectControl value={mode} onChange={(event) => setMode(event.target.value as "all" | "seed_only")}>
                    <option value="all">all</option>
                    <option value="seed_only">seed_only</option>
                  </SelectControl>
                </div>
                <div>
                  <FieldLabel>Ülke</FieldLabel>
                  <InputControl value={country} onChange={(event) => setCountry(event.target.value)} className="py-0" />
                </div>
              </div>
              <div className="grid gap-4 md:grid-cols-2">
                <div>
                  <FieldLabel>Arşiv</FieldLabel>
                  <InputControl value={bucket} onChange={(event) => setBucket(event.target.value)} className="py-0" />
                </div>
                <div>
                  <FieldLabel>Odak Varlık</FieldLabel>
                  <InputControl value={focusEntity} onChange={(event) => setFocusEntity(event.target.value)} className="py-0" />
                </div>
              </div>
              <div className="flex flex-wrap gap-3">
                <Button type="submit" tone="primary" disabled={isPending}>
                  {isPending ? "Kuyruğa yazılıyor..." : "POST /jobs/batch"}
                </Button>
                <Button type="button" tone="secondary" onClick={refreshAll}>
                  Durumu Yenile
                </Button>
              </div>
            </form>
            {error ? <div className="mt-4 rounded-[12px] border border-[rgba(255,100,124,0.28)] bg-[rgba(255,100,124,0.1)] px-4 py-3 text-[13px] text-[#ffc1cb]">{error}</div> : null}
          </AppPanel>

          <AppPanel>
            <SectionTitle
              eyebrow="İşleyici Kontrolü"
              title="İşleyici Dalgası"
              description="Tek çağrıda kuyruktaki işleri çeker ve işler; toplu mod tümü iken yorumlardan bulunan takip adaylarını da değerlendirir, ancak varsayılan olarak yeni toplama isteği açmaz."
            />
            <div className="mt-5 grid gap-4 md:grid-cols-[minmax(0,1fr)_auto]">
              <div>
                <FieldLabel>Azami Paralel Hesap</FieldLabel>
                <InputControl value={workerMaxJobs} onChange={(event) => setWorkerMaxJobs(event.target.value)} className="py-0" />
              </div>
              <div className="flex items-end">
                <Button type="button" tone="primary" onClick={onRunWorkers} disabled={isPending}>
                  {isPending ? "Çalışıyor..." : "POST /ingest/workers/run-once"}
                </Button>
              </div>
            </div>
            {lastWorkerRun ? (
              <div className="mt-4 flex flex-wrap gap-2">
                <StatusBadge tone="info" mono>{lastWorkerRun.claimed_jobs} alındı</StatusBadge>
                <StatusBadge tone="success" mono>{lastWorkerRun.completed_jobs} tamamlandı</StatusBadge>
                <StatusBadge tone="danger" mono>{lastWorkerRun.failed_jobs} hata</StatusBadge>
                <StatusBadge tone="neutral" mono>{lastWorkerRun.lease_owner}</StatusBadge>
              </div>
            ) : null}
          </AppPanel>

          <AppPanel>
            <SectionTitle eyebrow="Son Yanıt" title="Son Toplu İş Özeti" />
            {lastBatch ? (
              <div className="mt-5 space-y-3">
                <BatchJobSummary item={lastBatch.batch_job} />
                <div className="flex flex-wrap gap-2">
                  <StatusBadge tone="info" mono>{lastBatch.targets.length} hedef satırı</StatusBadge>
                  <StatusBadge tone="warning" mono>{lastBatch.ingest_jobs.length} toplama işi</StatusBadge>
                </div>
              </div>
            ) : (
              <div className="mt-5 text-[14px] text-[var(--text-muted)]">Henüz batch oluşturulmadı.</div>
            )}
          </AppPanel>
        </div>

        <div className="space-y-6">
          <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-4">
            <MetricCard
              label="Aktif İşleyici"
              value={workerTopology.activeWorkers}
              hint="Aynı anda en az bir çalışan toplama işi taşıyan işleyici sayısı."
              tone="info"
            />
            <MetricCard
              label="Aktif Hesap İşi"
              value={workerTopology.runningJobsCount}
              hint="İşleyici katmanında şu an işlenen hesap düzeyi toplama işi sayısı."
              tone="accent"
            />
            <MetricCard
              label="Aktif Medya Hattı"
              value={workerTopology.activeMedia}
              hint="Olay akışına göre o anda açık olan tekil medya analizleri."
              tone="warning"
            />
            <MetricCard
              label="Aktif Yorum Hattı"
              value={workerTopology.activeComments}
              hint="Yorum aşaması içinde devam eden analizler."
              tone="warning"
            />
          </div>

          <AppPanel>
            <SectionTitle
              eyebrow="Soket Akışı"
              title="Canlı Telemetri"
              description="Operasyon ekranı artık web soketi ile canlı güncellenir; hat sayaçları ve süreler olay akışından saniye saniye türetilir."
            />
            <div className="mt-5 flex flex-wrap gap-2">
              <StatusBadge tone={socketStatus === "live" ? "success" : socketStatus === "retrying" ? "warning" : socketStatus === "offline" ? "neutral" : "info"}>
                {socketStatus}
              </StatusBadge>
              <StatusBadge tone="info" mono>{socketServerTime ? renderTimestamp(socketServerTime) : "sunucu saati bekleniyor"}</StatusBadge>
              <StatusBadge tone="accent" mono>{workerTopology.activeMedia} medya hattı</StatusBadge>
              <StatusBadge tone="accent" mono>{workerTopology.activeComments} yorum hattı</StatusBadge>
              <StatusBadge tone="accent" mono>{workerTopology.activeMerges} birleştirme hattı</StatusBadge>
            </div>
          </AppPanel>

          <AppPanel>
            <SectionTitle
              eyebrow="Yürütme Modeli"
              title="Sistem Nasıl Çalışıyor"
              description="İşleyici hesap işi taşır; her hesap işi içinde gönderi hazırlığı, tekil medya analizi, üst birleştirme, yorum analizi ve takip kararı ayrı alt işler olarak akar."
            />
            <div className="mt-5 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.15fr)]">
              <div className="rounded-[16px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] p-4 text-[13px] leading-7 text-[var(--text-secondary)]">
                <div className="font-semibold text-[var(--text-primary)]">Canlı okuma</div>
                <div className="mt-2">
                  Bu panelde &quot;aktif işleyici&quot; hesap düzeyi paralelliği, &quot;aktif medya hattı&quot; ise aynı hesap işi içinde paralel yürüyen tekil medya analizlerini gösterir.
                </div>
                <div className="mt-2">
                  Örneğin bir gönderide 4 medya varsa ve olay akışında dört `single_media_started` kaydı açık duruyorsa burada 4 aktif medya hattı olarak görünür.
                </div>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] px-4 py-3">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">Kuyruk</div>
                  <div data-mono="true" className="mt-3 text-[28px] font-bold text-[var(--text-primary)]">
                    {workerTopology.queuedJobsCount}
                  </div>
                  <div className="mt-2 text-[12px] leading-6 text-[var(--text-secondary)]">Bekliyor, kuyruğa alındı veya yeniden deneme bekliyor durumundaki hesap işi sayısı.</div>
                </div>
                <div className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] px-4 py-3">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">Birleştirme Hattı</div>
                  <div data-mono="true" className="mt-3 text-[28px] font-bold text-[var(--text-primary)]">
                    {workerTopology.activeMerges}
                  </div>
                  <div className="mt-2 text-[12px] leading-6 text-[var(--text-secondary)]">Çoklu medya postlarda çalışan parent merge adımı sayısı.</div>
                </div>
              </div>
            </div>
          </AppPanel>

          <AppPanel>
            <SectionTitle
              eyebrow="Canlı Hatlar"
              title="Paralel İş Şeması"
              description="Aynı anda çalışan medya, yorum ve birleştirme hatları burada akar. Bir hat bittiğinde süre sabitlenir ve yeşil tik ile tamamlandı olarak görünür."
            />
            <div className="mt-5 grid items-start gap-6 2xl:grid-cols-3">
              <div className="space-y-3 self-start">
                <div className="flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
                  <Images className="h-4 w-4 text-[var(--warning)]" />
                  Aktif Medya
                </div>
                <div className="grid max-h-[960px] gap-3 overflow-auto pr-1">
                  {activeMediaTasks.length ? activeMediaTasks.map(({ task, job }) => <TaskTile key={task.key} task={task} job={job} nowMs={nowMs} tone="warning" />) : <EmptyStateCard>Şu an aktif medya hattı yok.</EmptyStateCard>}
                </div>
                <div className="pt-2">
                  <div className="mb-3 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
                    <CheckCircle2 className="h-4 w-4 text-[var(--success)]" />
                    Tamamlanan Medya
                  </div>
                  <div className="grid gap-3">
                    {completedMediaTasks.length ? completedMediaTasks.map(({ task, job }) => <TaskTile key={task.key} task={task} job={job} nowMs={nowMs} tone="success" />) : <EmptyStateCard>Henüz biten medya hattı yok.</EmptyStateCard>}
                  </div>
                </div>
              </div>

              <div className="space-y-3 self-start">
                <div className="flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
                  <MessageSquareText className="h-4 w-4 text-[var(--warning)]" />
                  Aktif Yorumlar
                </div>
                <div className="grid max-h-[960px] gap-3 overflow-auto pr-1">
                  {activeCommentTasks.length ? activeCommentTasks.map(({ task, job }) => <TaskTile key={task.key} task={task} job={job} nowMs={nowMs} tone="warning" />) : <EmptyStateCard>Şu an aktif yorum hattı yok.</EmptyStateCard>}
                </div>
                <div className="pt-2">
                  <div className="mb-3 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
                    <CheckCircle2 className="h-4 w-4 text-[var(--success)]" />
                    Tamamlanan Yorumlar
                  </div>
                  <div className="grid gap-3">
                    {completedCommentTasks.length ? completedCommentTasks.map(({ task, job }) => <TaskTile key={task.key} task={task} job={job} nowMs={nowMs} tone="success" />) : <EmptyStateCard>Henüz biten yorum hattı yok.</EmptyStateCard>}
                  </div>
                </div>
              </div>

              <div className="space-y-3 self-start">
                <div className="flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
                  <GitBranch className="h-4 w-4 text-[var(--accent-primary)]" />
                  Birleştirme Hatları
                </div>
                <div className="grid max-h-[960px] gap-3 overflow-auto pr-1">
                  {activeMergeTasks.length ? activeMergeTasks.map(({ task, job }) => <TaskTile key={task.key} task={task} job={job} nowMs={nowMs} tone="accent" />) : <EmptyStateCard>Şu an aktif birleştirme hattı yok.</EmptyStateCard>}
                </div>
                <div className="pt-2">
                  <div className="mb-3 flex items-center gap-2 text-[12px] font-semibold uppercase tracking-[0.18em] text-[var(--text-muted)]">
                    <CheckCircle2 className="h-4 w-4 text-[var(--success)]" />
                    Tamamlanan Birleştirme
                  </div>
                  <div className="grid gap-3">
                    {completedMergeTasks.length ? completedMergeTasks.map(({ task, job }) => <TaskTile key={task.key} task={task} job={job} nowMs={nowMs} tone="success" />) : <EmptyStateCard>Henüz biten birleştirme hattı yok.</EmptyStateCard>}
                  </div>
                </div>
              </div>
            </div>
          </AppPanel>

          <AppPanel>
            <SectionTitle eyebrow="Toplu İş Kuyruğu" title="Toplu İş Akışı" description="Hangi toplu işin ne durumda olduğu, kaç takip adayı üretildiği ve şu an hangi dalganın çalıştığı burada görünür." />
            <div className="mt-5 grid gap-4">
              {overview.batches.length > 0 ? (
                overview.batches.map((item) => <BatchJobSummary key={item.id} item={item} />)
              ) : (
                <div className="text-[14px] text-[var(--text-muted)]">Henüz batch yok.</div>
              )}
            </div>
          </AppPanel>

          <AppPanel>
            <SectionTitle
              eyebrow="İşleyici Hatları"
              title="İşleyici Bazlı Görünüm"
              description="Her işleyici altında aynı anda kaç hesap işi, kaç medya hattı ve kaç yorum hattı aktığını burada görürsün."
            />
            <div className="mt-5 grid gap-4">
              {workerTopology.workers.length > 0 ? (
                workerTopology.workers.map((worker) => (
                  <WorkerLaneCard
                    key={worker.leaseOwner}
                    leaseOwner={worker.leaseOwner}
                    jobs={worker.jobs}
                    snapshots={runtimeSnapshots}
                    nowMs={nowMs}
                  />
                ))
              ) : (
                <div className="text-[14px] text-[var(--text-muted)]">Şu an aktif işleyici hattı görünmüyor.</div>
              )}
            </div>
          </AppPanel>

          <div className="grid gap-6 2xl:grid-cols-2">
            <AppPanel>
              <SectionTitle eyebrow="Hedef Akışı" title="Hedef Zinciri" description="İlk hedefler ve yorumlardan bulunan takip adayları aynı çizgide tutulur; adaylar varsayılan olarak öneri durumunda kalır." />
              <div className="mt-5 grid gap-4">
                {runningTargets.length > 0 ? (
                  runningTargets.map((item) => <TargetFlowCard key={item.id} item={item} />)
                ) : (
                  <div className="text-[14px] text-[var(--text-muted)]">Hedef akışı henüz boş.</div>
                )}
              </div>
            </AppPanel>

            <AppPanel>
              <SectionTitle eyebrow="İş Kuyruğu" title="Toplama İşleri" description="Şu an ne çalışıyor, ne bekliyor, hangi işin hangi işleyici üzerinde olduğu ve iş içinde kaç aktif medya/yorum hattı bulunduğu burada izlenir." />
              <div className="mt-5 grid gap-4">
                {runningJobs.length > 0 ? (
                  runningJobs.map((item) => <IngestJobCard key={item.id} item={item} snapshot={runtimeSnapshots.get(item.id)} nowMs={nowMs} />)
                ) : (
                  <div className="text-[14px] text-[var(--text-muted)]">Toplama kuyruğu boş.</div>
                )}
              </div>
            </AppPanel>
          </div>

          <div className="grid items-start gap-6 2xl:grid-cols-[320px_minmax(0,1fr)]">
            <AppPanel className="self-start">
              <SectionTitle eyebrow="İnceleme Kuyruğu" title="Takip Adayları" description="En çok bayrak alan yorumcular burada görünür; sistem araştırma adaylarını bulur ama varsayılan olarak otomatik toplama isteği açmaz." />
              <div className="mt-5 grid gap-3">
                {overview.review_queue.length > 0 ? (
                  overview.review_queue.slice(0, 12).map((item) => (
                    <div key={`${item.commenter_username}-${item.last_triggered_at}`} className="rounded-[14px] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] px-4 py-3">
                      <div className="flex items-center justify-between gap-3">
                        <div data-mono="true" className="text-[13px] font-semibold text-[var(--text-primary)]">
                          @{item.commenter_username}
                        </div>
                        <StatusBadge tone={statusTone(item.status)} mono>{item.trigger_count}</StatusBadge>
                      </div>
                      <div className="mt-2 text-[12px] leading-6 text-[var(--text-secondary)]">
                        {item.flag_reason_type || "belirsiz"} • {renderTimestamp(item.last_triggered_at)}
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="text-[14px] text-[var(--text-muted)]">İnceleme kuyruğu henüz boş.</div>
                )}
              </div>
            </AppPanel>

            <div className="space-y-6 self-start">
              <AppPanel className="self-start">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <SectionTitle eyebrow="Canlı Akış" title="Canlı Akış" description="Şu an hangi gönderi, medya, yorum ve inceleme/takip adımının çalıştığı burada görünür." />
                  <Button type="button" tone="secondary" onClick={refreshAll}>
                    Yenile
                  </Button>
                </div>
                <div className="mt-5 grid max-h-[960px] gap-3 overflow-auto pr-1">
                  {recentEvents.length > 0 ? (
                    recentEvents.map((item) => <EventCard key={item.id} item={item} job={jobsById.get(item.ingest_job_id)} />)
                  ) : (
                    <div className="text-[14px] text-[var(--text-muted)]">Henüz canlı olay kaydı yok.</div>
                  )}
                </div>
              </AppPanel>

              <AppPanel>
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <SectionTitle eyebrow="İz Çıktısı" title="Ham İz Kaydı" description="Detaylı istem/taşıyıcı veri ve ham akış günlüğü burada tutulur." />
                  <Button type="button" tone="secondary" onClick={refreshAll}>
                    Yenile
                  </Button>
                </div>
                <div className="mt-5 rounded-[var(--radius-card)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.96)] p-4">
                  <pre data-mono="true" className="h-[360px] overflow-auto whitespace-pre-wrap text-[12px] leading-6 text-[var(--text-secondary)]">
                    {trace || "İz günlüğü boş."}
                  </pre>
                </div>
              </AppPanel>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
