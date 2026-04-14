import {
  AccountDetail,
  AccountListItem,
  BatchJobCreateResponse,
  CommentListItem,
  DashboardSummary,
  GraphData,
  GraphAnalysisResult,
  GraphCaptureResult,
  IngestResponse,
  IngestWorkersRunOnceResponse,
  JobsOverview,
  JobsOverviewStreamPayload,
  PostItem,
  PromptTemplate,
  ReviewQueueItem,
} from "@/lib/types";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8080";

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });

  if (!response.ok) {
    throw new Error(`API request failed: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

export const apiBaseUrl = API_BASE_URL;
export const jobsOverviewWebSocketUrl = (() => {
  const base = API_BASE_URL.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
  return `${base}/ws/jobs/overview`;
})();

export async function getDashboardSummary() {
  return fetchJson<DashboardSummary>("/dashboard/summary");
}

export async function getAccounts(query?: string) {
  const suffix = query ? `?${query}` : "";
  return fetchJson<{ items: AccountListItem[] }>(`/accounts${suffix}`);
}

export async function getAccountDetail(accountId: string | number) {
  return fetchJson<AccountDetail>(`/accounts/${accountId}`);
}

export async function getAccountPosts(accountId: string | number) {
  return fetchJson<{ items: PostItem[] }>(`/accounts/${accountId}/posts`);
}

export async function getAccountComments(accountId: string | number, query?: string) {
  const suffix = query ? `?${query}` : "";
  return fetchJson<{ items: CommentListItem[] }>(`/accounts/${accountId}/comments${suffix}`);
}

export async function getAccountGraph(accountId: string | number) {
  return fetchJson<GraphData>(`/accounts/${accountId}/graph`);
}

export async function runAccountGraphAnalysis(
  accountId: string | number,
  payload?: { model?: string; graph_image_data_url?: string },
) {
  return fetchJson<GraphAnalysisResult>(`/accounts/${accountId}/graph-analysis`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function saveAccountGraphCapture(accountId: string | number, payload: { graph_image_data_url: string }) {
  return fetchJson<GraphCaptureResult>(`/accounts/${accountId}/graph-capture`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getReviewQueue(search?: string) {
  const query = search ? `?search=${encodeURIComponent(search)}` : "";
  return fetchJson<{ items: ReviewQueueItem[] }>(`/review-queue${query}`);
}

export async function getPromptTemplates() {
  return fetchJson<{ items: PromptTemplate[] }>("/prompts");
}

export async function getPromptTemplate(key: string) {
  return fetchJson<PromptTemplate>(`/prompts/${key}`);
}

export async function updatePromptTemplate(
  key: string,
  payload: { content: string; is_enabled?: boolean; reset_to_default?: boolean },
) {
  return fetchJson<PromptTemplate>(`/prompts/${key}`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
}

export async function runIngest(payload: {
  target_username: string;
  focus_entity?: string;
  debug_first_post_only?: boolean;
  max_posts?: number;
  max_media_items_per_post?: number;
  max_comments_per_post?: number;
  analyze_comments?: boolean;
}) {
  return fetchJson<IngestResponse>("/ingest/run", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getIngestTrace() {
  return fetchJson<{ path: string | null; content: string }>("/ingest/trace");
}

export async function createBatchJob(payload: {
  targets: string[];
  mode: "all" | "seed_only";
  country?: string;
  bucket?: string;
  focus_entity?: string;
  auto_enqueue_followups?: boolean;
}) {
  return fetchJson<BatchJobCreateResponse>("/jobs/batch", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getJobsOverview(params?: { batch_job_id?: number; limit?: number }) {
  const search = new URLSearchParams();
  if (params?.batch_job_id !== undefined) {
    search.set("batch_job_id", String(params.batch_job_id));
  }
  if (params?.limit !== undefined) {
    search.set("limit", String(params.limit));
  }
  const suffix = search.size > 0 ? `?${search.toString()}` : "";
  return fetchJson<JobsOverview>(`/jobs/overview${suffix}`);
}

export async function runIngestWorkersOnce(payload?: { max_jobs?: number; lease_owner?: string }) {
  return fetchJson<IngestWorkersRunOnceResponse>("/ingest/workers/run-once", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export type { JobsOverviewStreamPayload };
