export type DashboardSummary = {
  kpis: {
    incelenen_hesap: number;
    incelenen_post: number;
    incelenen_yorum: number;
    acik_review_queue: number;
    baskin_orgut: string;
  };
  riskli_hesaplar: AccountListItem[];
  son_bayrakli_yorumlar: CommentListItem[];
  kategori_dagilimi: ChartDatum[];
  tehdit_dagilimi: ChartDatum[];
};

export type ChartDatum = {
  name: string;
  value: number;
};

export type AccountListItem = {
  id: number;
  instagram_username: string;
  profile_photo_url?: string | null;
  bio?: string | null;
  account_profile_summary?: string | null;
  profil_ozeti?: string | null;
  tehdit_seviyesi?: string | null;
  max_onem_skoru?: number | null;
  tespit_edilen_orgut?: string | null;
  post_count?: number | null;
  comment_count?: number | null;
  flagged_comment_count?: number | null;
};

export type AccountDetail = {
  id: number;
  instagram_username: string;
  profile_photo_url?: string | null;
  bio?: string | null;
  account_profile_summary?: string | null;
  graph_ai_analysis?: string | null;
  graph_ai_analysis_model?: string | null;
  graph_ai_analysis_updated_at?: string | null;
  graph_capture_path?: string | null;
  graph_capture_updated_at?: string | null;
  tehdit_seviyesi?: string | null;
  tespit_edilen_orgut?: string | null;
  post_count: number;
  comment_count: number;
  flagged_comment_count: number;
  ortalama_onem_skoru: number;
  baskin_kategori: string;
};

export type GraphAnalysisResult = {
  account_id: number;
  model: string;
  analysis: string;
  updated_at?: string | null;
};

export type GraphCaptureResult = {
  account_id: number;
  capture_url: string;
  updated_at?: string | null;
};

export type PostItem = {
  id: number;
  media_type: "image" | "video";
  media_url: string;
  media_items?: Array<{
    media_type: "image" | "video";
    media_url: string;
  }>;
  caption?: string | null;
  post_analysis?: string | null;
  post_ozet?: string | null;
  structured_analysis?: Record<string, unknown>;
  icerik_kategorisi: string[];
  tehdit_seviyesi?: string | null;
  onem_skoru?: number | null;
  tespit_edilen_orgut?: string | null;
  source_post_id?: string | null;
  source_post_url?: string | null;
  created_at?: string | null;
  media_observations?: Array<{
    media_index?: number;
    media_type?: "image" | "video" | string;
    deep_required?: boolean;
    deep_status?: "not_required" | "completed" | "failed" | string;
    deep_reason?: string;
    location_confidence?: "low" | "medium" | "high" | "unclear" | string;
    contains_vehicle?: boolean;
    contains_plate?: boolean;
    deep_payload?: Record<string, unknown>;
    [key: string]: unknown;
  }>;
};

export type CommentListItem = {
  id?: number;
  instagram_username?: string;
  commenter_username?: string | null;
  commenter_profile_url?: string | null;
  comment_text: string;
  verdict: string;
  sentiment?: string;
  orgut_baglanti_skoru: number;
  bayrak: boolean | number;
  reason?: string | null;
  created_at?: string | null;
  source_post_id?: string | null;
  post_ozet?: string | null;
};

export type GraphData = {
  nodes: Array<{ id: string; type: string; label: string; weight?: number; avatar_url?: string | null }>;
  edges: Array<{ id: string; source: string; target: string; type: string }>;
};

export type ReviewQueueItem = {
  commenter_username: string;
  trigger_count: number;
  first_triggered_at: string;
  last_triggered_at: string;
  last_reason?: string | null;
  flag_reason_type?: string | null;
  status: string;
};

export type PromptTemplate = {
  key: string;
  display_name: string;
  description?: string | null;
  content: string;
  default_content: string;
  is_enabled: boolean;
  version: number;
  updated_at: string;
  is_overridden: boolean;
};

export type IngestResponse = {
  target_username: string;
  run_id: string;
  bucket: string;
  person_id: number;
  instagram_account_id: number;
  processed_posts: number;
  created_posts: number;
  updated_posts: number;
  processed_comments: number;
  created_comments: number;
  skipped_comments: number;
  flagged_users: number;
  flagged_usernames: string[];
  errors: string[];
};

export type IngestJob = {
  id: number;
  target_username: string;
  bucket: string;
  run_id: string;
  status: string;
  attempts: number;
  processed_posts: number;
  created_posts: number;
  updated_posts: number;
  processed_comments: number;
  created_comments: number;
  skipped_comments: number;
  flagged_users: number;
  error_message?: string | null;
  lease_owner?: string | null;
  lease_expires_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  batch_job_id?: number | null;
  batch_target_id?: number | null;
  source_kind?: string | null;
  parent_username?: string | null;
  focus_entity?: string | null;
  country?: string | null;
  current_stage?: string | null;
  current_event?: string | null;
  current_post_index?: number | null;
  total_posts?: number | null;
  current_post_id?: string | null;
  current_media_index?: number | null;
  total_media_items?: number | null;
  current_comment_index?: number | null;
  total_comments?: number | null;
  current_commenter_username?: string | null;
  last_event_at?: string | null;
  created_at: string;
  updated_at: string;
};

export type BatchJob = {
  id: number;
  mode: "all" | "seed_only";
  status: "queued" | "running" | "completed" | "failed" | "partial";
  bucket: string;
  country?: string | null;
  focus_entity?: string | null;
  auto_enqueue_followups?: boolean;
  requested_targets: string[];
  total_targets: number;
  initial_targets: number;
  discovered_followups: number;
  completed_targets: number;
  failed_targets: number;
  running_targets: number;
  pending_targets: number;
  created_at: string;
  updated_at: string;
};

export type BatchJobTarget = {
  id: number;
  batch_job_id: number;
  raw_target: string;
  normalized_username: string;
  source_kind: string;
  parent_username?: string | null;
  status: "pending" | "enqueued" | "running" | "completed" | "failed" | "skipped" | "missing_archive" | "suggested";
  ingest_job_id?: number | null;
  note?: string | null;
  created_at: string;
  updated_at: string;
};

export type BatchJobCreateResponse = {
  batch_job: BatchJob;
  targets: BatchJobTarget[];
  ingest_jobs: IngestJob[];
};

export type IngestJobEvent = {
  id: number;
  ingest_job_id: number;
  event_type: string;
  stage?: string | null;
  message: string;
  source_post_id?: string | null;
  commenter_username?: string | null;
  post_index?: number | null;
  post_total?: number | null;
  media_index?: number | null;
  media_total?: number | null;
  comment_index?: number | null;
  comment_total?: number | null;
  created_at: string;
};

export type JobsOverview = {
  batches: BatchJob[];
  targets: BatchJobTarget[];
  ingest_jobs: IngestJob[];
  review_queue: ReviewQueueItem[];
  recent_events: IngestJobEvent[];
};

export type JobsOverviewStreamPayload = {
  overview: JobsOverview;
  server_time: string;
};

export type IngestWorkersRunOnceResponse = {
  lease_owner: string;
  claimed_jobs: number;
  completed_jobs: number;
  failed_jobs: number;
  items: IngestJob[];
};
