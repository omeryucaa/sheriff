from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


MediaType = Literal["image", "video"]


class AnalyzeMediaRequest(BaseModel):
    bucket: str = Field(..., min_length=1)
    object_key: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    media_type: MediaType
    expires_seconds: int = Field(default=900, ge=60, le=7 * 24 * 3600)
    max_tokens: int = Field(default=256, ge=1, le=4096)
    model: str | None = None


class AnalyzeMediaResponse(BaseModel):
    model: str
    answer: str
    usage: dict[str, Any] | None = None
    finish_reason: str | None = None
    media_url: str


class AnalyzeGraphRequest(BaseModel):
    model: str | None = None
    graph_image_data_url: str | None = None


class AnalyzeGraphResponse(BaseModel):
    account_id: int
    model: str
    analysis: str
    updated_at: str | None = None


class SaveGraphCaptureRequest(BaseModel):
    graph_image_data_url: str = Field(..., min_length=32)


class SaveGraphCaptureResponse(BaseModel):
    account_id: int
    capture_url: str
    updated_at: str | None = None


CommentVerdict = Literal[
    "destekci_aktif",
    "destekci_pasif",
    "karsit",
    "tehdit",
    "bilgi_ifsa",
    "koordinasyon",
    "nefret_soylemi",
    "alakasiz",
    "belirsiz",
]
CommentSentiment = Literal["positive", "negative", "neutral"]
PostContentCategory = Literal[
    "haber_paylasim",
    "propaganda",
    "askeri_operasyon",
    "cenaze_anma_sehit",
    "kutlama_zafer",
    "tehdit_gozdag",
    "egitim_talim",
    "lojistik_koordinasyon",
    "ise_alim_radikallestirme",
    "kisisel_gunluk",
    "yuruyus_gosteri",
    "hukuki_savunma",
    "dini_ideolojik",
    "medya_kultur",
    "itiraf_ifsa",
    "belirsiz",
]
ThreatLevel = Literal["yok", "dusuk", "orta", "yuksek", "kritik"]


class CommentInput(BaseModel):
    commenter_username: str | None = None
    text: str = Field(..., min_length=1)


class AnalyzePostAndCommentsRequest(BaseModel):
    username: str = Field(..., min_length=1)
    instagram_username: str | None = None
    profile_photo_url: str | None = None
    bio: str | None = None
    caption: str | None = None
    focus_entity: str | None = None

    media_type: MediaType
    media_url: str | None = None
    bucket: str | None = None
    object_key: str | None = None
    expires_seconds: int = Field(default=900, ge=60, le=7 * 24 * 3600)

    comments: list[CommentInput] = Field(default_factory=list)
    model: str | None = None
    post_max_tokens: int = Field(default=1200, ge=1, le=4096)
    comment_max_tokens: int = Field(default=768, ge=1, le=4096)
    enable_deep_media_analysis: bool = True

    @model_validator(mode="after")
    def validate_media_source(self) -> "AnalyzePostAndCommentsRequest":
        has_direct_url = bool(self.media_url)
        has_bucket_object = bool(self.bucket and self.object_key)
        if not has_direct_url and not has_bucket_object:
            raise ValueError("Either media_url or bucket+object_key must be provided.")
        return self


class CommentAnalysis(BaseModel):
    commenter_username: str | None = None
    text: str
    verdict: CommentVerdict
    sentiment: CommentSentiment
    orgut_baglanti_skoru: int = Field(default=0, ge=0, le=10)
    bayrak: bool = False
    reason: str


class PostVisualAnalysis(BaseModel):
    sahne_tanimi: str = ""
    konum_tahmini: str = "belirsiz"
    kisi_sayisi: int = Field(default=0, ge=0)
    silah_patlayici_var_mi: bool = False
    bayrak_sembol_amblam: str = "belirsiz"
    uniforma_kiyafet: str = "belirsiz"


class PostOrganizationLink(BaseModel):
    tespit_edilen_orgut: str = "belirsiz"
    baglanti_gostergesi: str = "belirsiz"
    muhtemel_rol: str = "belirsiz"


class PostThreatAssessment(BaseModel):
    tehdit_seviyesi: ThreatLevel = "yok"
    eylem_plani_imasi: bool = False
    gelecek_eylem_detay: str = ""
    tehdit_veya_itiraf: str = ""
    hedef_belirtilmis_mi: bool = False
    hedef_detay: str = ""


class PostCrimeAssessment(BaseModel):
    suc_var_mi: bool = False
    suc_turleri: list[str] = Field(default_factory=list)
    aciklama: str = ""


class PostStructuredAnalysis(BaseModel):
    ozet: str = ""
    gorsel_analiz: PostVisualAnalysis = Field(default_factory=PostVisualAnalysis)
    icerik_tonu: str = "notral"
    icerik_kategorisi: list[PostContentCategory] = Field(default_factory=list)
    orgut_baglantisi: PostOrganizationLink = Field(default_factory=PostOrganizationLink)
    tehdit_degerlendirmesi: PostThreatAssessment = Field(default_factory=PostThreatAssessment)
    suc_unsuru: PostCrimeAssessment = Field(default_factory=PostCrimeAssessment)
    onem_skoru: int = Field(default=1, ge=1, le=10)
    analist_notu: str = ""


class AnalyzePostAndCommentsResponse(BaseModel):
    model: str
    person_id: int
    instagram_account_id: int
    post_id: int
    comment_ids: list[int]
    media_url: str
    post_analysis: str
    comment_analyses: list[CommentAnalysis]
    summary: dict[str, int]


class IngestInstagramAccountLatestRequest(BaseModel):
    target_username: str = Field(..., min_length=1)
    run_id: str | None = None
    bucket: str | None = None
    focus_entity: str | None = None
    debug_first_post_only: bool = False
    max_posts: int | None = Field(default=None, ge=1)
    max_media_items_per_post: int | None = Field(default=None, ge=1)
    max_comments_per_post: int | None = Field(default=None, ge=1)
    analyze_comments: bool = True
    model: str | None = None
    post_max_tokens: int = Field(default=1200, ge=1, le=4096)
    comment_max_tokens: int = Field(default=768, ge=1, le=4096)
    expires_seconds: int = Field(default=900, ge=60, le=7 * 24 * 3600)
    enable_deep_media_analysis: bool = True


class IngestInstagramAccountLatestResponse(BaseModel):
    target_username: str
    run_id: str
    bucket: str
    person_id: int
    instagram_account_id: int
    processed_posts: int
    created_posts: int
    updated_posts: int
    processed_comments: int
    created_comments: int
    skipped_comments: int
    flagged_users: int
    flagged_usernames: list[str]
    errors: list[str]


IngestJobStatus = Literal["pending", "discovered", "running", "completed", "failed", "retry_wait", "skipped"]


class IngestWatchScanRequest(BaseModel):
    bucket: str | None = None
    usernames: list[str] = Field(default_factory=list)


class IngestWatchScanResponse(BaseModel):
    bucket: str
    discovered_sources: int
    enqueued_jobs: int
    skipped_jobs: int
    usernames: list[str]


class IngestJobsEnqueueRequest(BaseModel):
    usernames: list[str] = Field(..., min_length=1)
    bucket: str | None = None
    run_ids: dict[str, str] = Field(default_factory=dict)


class IngestJobItem(BaseModel):
    id: int
    target_username: str
    bucket: str
    run_id: str
    status: IngestJobStatus
    attempts: int
    processed_posts: int = 0
    created_posts: int = 0
    updated_posts: int = 0
    processed_comments: int = 0
    created_comments: int = 0
    skipped_comments: int = 0
    flagged_users: int = 0
    error_message: str | None = None
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    batch_job_id: int | None = None
    batch_target_id: int | None = None
    source_kind: str | None = None
    parent_username: str | None = None
    focus_entity: str | None = None
    country: str | None = None
    current_stage: str | None = None
    current_event: str | None = None
    current_post_index: int | None = None
    total_posts: int | None = None
    current_post_id: str | None = None
    current_media_index: int | None = None
    total_media_items: int | None = None
    current_comment_index: int | None = None
    total_comments: int | None = None
    current_commenter_username: str | None = None
    last_event_at: str | None = None
    created_at: str
    updated_at: str


class IngestJobsListResponse(BaseModel):
    items: list[IngestJobItem]


class IngestWorkersRunOnceRequest(BaseModel):
    max_jobs: int | None = Field(default=None, ge=1, le=100)
    lease_owner: str | None = None


class IngestWorkersRunOnceResponse(BaseModel):
    lease_owner: str
    claimed_jobs: int
    completed_jobs: int
    failed_jobs: int
    items: list[IngestJobItem]


class PromptTemplateUpdateRequest(BaseModel):
    content: str = Field(..., min_length=1)
    is_enabled: bool = True
    reset_to_default: bool = False


BatchMode = Literal["all", "seed_only"]
BatchJobStatus = Literal["queued", "running", "completed", "failed", "partial"]
BatchTargetStatus = Literal["pending", "enqueued", "running", "completed", "failed", "skipped", "missing_archive", "suggested"]


class BatchJobsCreateRequest(BaseModel):
    targets: list[str] = Field(..., min_length=1)
    mode: BatchMode = "all"
    country: str | None = None
    bucket: str | None = None
    focus_entity: str | None = None
    auto_enqueue_followups: bool = False


class BatchJobTargetItem(BaseModel):
    id: int
    batch_job_id: int
    raw_target: str
    normalized_username: str
    source_kind: str
    parent_username: str | None = None
    status: BatchTargetStatus
    ingest_job_id: int | None = None
    note: str | None = None
    created_at: str
    updated_at: str


class BatchJobItem(BaseModel):
    id: int
    mode: BatchMode
    status: BatchJobStatus
    bucket: str
    country: str | None = None
    focus_entity: str | None = None
    auto_enqueue_followups: bool = False
    requested_targets: list[str] = Field(default_factory=list)
    total_targets: int = 0
    initial_targets: int = 0
    discovered_followups: int = 0
    completed_targets: int = 0
    failed_targets: int = 0
    running_targets: int = 0
    pending_targets: int = 0
    created_at: str
    updated_at: str


class BatchJobCreateResponse(BaseModel):
    batch_job: BatchJobItem
    targets: list[BatchJobTargetItem]
    ingest_jobs: list[IngestJobItem]


class IngestJobEventItem(BaseModel):
    id: int
    ingest_job_id: int
    event_type: str
    stage: str | None = None
    message: str
    source_post_id: str | None = None
    commenter_username: str | None = None
    post_index: int | None = None
    post_total: int | None = None
    media_index: int | None = None
    media_total: int | None = None
    comment_index: int | None = None
    comment_total: int | None = None
    created_at: str


class JobsOverviewResponse(BaseModel):
    batches: list[BatchJobItem]
    targets: list[BatchJobTargetItem]
    ingest_jobs: list[IngestJobItem]
    review_queue: list[dict[str, Any]]
    recent_events: list[IngestJobEventItem]
