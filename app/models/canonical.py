from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ConfidenceLevel = Literal["low", "medium", "high"]
PriorityLevel = Literal["low", "medium", "high", "critical"]
SignalStrength = Literal["weak", "moderate", "strong"]


class NormalizedMediaItem(BaseModel):
    media_type: Literal["image", "video"]
    media_url: str


class NormalizedCommentInput(BaseModel):
    commenter_username: str | None = None
    commenter_profile_url: str | None = None
    text: str
    discovered_at: str | None = None
    source_post_url: str | None = None


class NormalizedPostInput(BaseModel):
    username: str
    instagram_username: str
    profile_photo_url: str | None = None
    bio: str | None = None
    caption: str | None = None
    media_items: list[NormalizedMediaItem] = Field(default_factory=list)
    comments: list[NormalizedCommentInput] = Field(default_factory=list)
    account_profile_summary: str = ""
    post_history_summaries: list[dict[str, object]] = Field(default_factory=list)


class CanonicalSignal(BaseModel):
    family: str
    strength: SignalStrength
    evidence: list[str] = Field(default_factory=list)


class CanonicalMediaObservation(BaseModel):
    media_index: int
    media_type: Literal["image", "video"]
    scene_summary: str
    setting: str = "belirsiz"
    visible_person_count: str = "unclear"
    face_visibility: str = "unclear"
    visible_symbols: list[str] = Field(default_factory=list)
    visible_text_items: list[str] = Field(default_factory=list)
    notable_objects: list[str] = Field(default_factory=list)
    weapon_present: bool = False
    weapon_types: list[str] = Field(default_factory=list)
    clothing: str = "unclear"
    activity_types: list[str] = Field(default_factory=list)
    crowd_level: str = "unclear"
    audio_summary: dict[str, str] = Field(default_factory=dict)
    child_presence: str = "unclear"
    institutional_markers: list[str] = Field(default_factory=list)
    vehicles: list[str] = Field(default_factory=list)
    license_or_signage: list[str] = Field(default_factory=list)
    deep_review_hint: dict[str, str] = Field(
        default_factory=lambda: {"run_deep_analysis": "no", "confidence": "low", "reason_tr": ""}
    )
    deep_required: bool = False
    deep_status: str = "not_required"
    deep_reason: str = ""
    location_confidence: str = "unclear"
    contains_vehicle: bool = False
    contains_plate: bool = False
    deep_payload: dict[str, Any] = Field(default_factory=dict)
    raw_note: str = "belirsiz"
    legacy_payload: dict[str, Any] = Field(default_factory=dict)


class CanonicalReviewDecision(BaseModel):
    importance_score: int = Field(default=1, ge=1, le=10)
    priority_level: PriorityLevel = "low"
    human_review_required: bool = False
    confidence: ConfidenceLevel = "low"
    reason: str = ""


class CanonicalPostAnalysis(BaseModel):
    focus_entity: str | None = None
    summary: str = ""
    content_types: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    primary_themes: list[str] = Field(default_factory=list)
    dominant_language: str = "unclear"
    tone: str = "notral"
    sloganized_language: str = "unclear"
    detected_entities: list[str] = Field(default_factory=list)
    threat_level: str = "yok"
    role: str = "unclear"
    signals: list[CanonicalSignal] = Field(default_factory=list)
    ambiguity_flags: list[str] = Field(default_factory=list)
    organization_link_score: int = Field(default=0, ge=0, le=10)
    organization_confidence: ConfidenceLevel = "low"
    behavior_single_instance: str = "unclear"
    repeated_theme: str = "unclear"
    escalation_signal: str = "unclear"
    analyst_note: str = ""
    review: CanonicalReviewDecision = Field(default_factory=CanonicalReviewDecision)
    legacy_payload: dict[str, Any] = Field(default_factory=dict)


class CanonicalCommentAnalysis(BaseModel):
    focus_entity: str | None = None
    commenter_username: str | None = None
    text: str
    comment_type: str = "belirsiz"
    content_summary_tr: str = ""
    sentiment: str = "neutral"
    organization_link_score: int = Field(default=0, ge=0, le=10)
    signals: list[CanonicalSignal] = Field(default_factory=list)
    ambiguity_flags: list[str] = Field(default_factory=list)
    organization_confidence: ConfidenceLevel = "low"
    consistent_with_history: str = "unclear"
    repeated_support_language: str = "unclear"
    active_supporter_flag: bool = False
    threat_flag: bool = False
    information_leak_flag: bool = False
    coordination_flag: bool = False
    hate_speech_flag: bool = False
    overall_risk_level: PriorityLevel = "low"
    flagged: bool = False
    reason: str = ""
    review: CanonicalReviewDecision = Field(default_factory=CanonicalReviewDecision)
    legacy_payload: dict[str, Any] = Field(default_factory=dict)


class CanonicalAccountAggregate(BaseModel):
    account_id: int
    dominant_themes: list[str] = Field(default_factory=list)
    repeated_entities: list[str] = Field(default_factory=list)
    repeated_risk_indicators: list[str] = Field(default_factory=list)
    role_trend: str = "unclear"
    average_organization_link_score: float = 0.0
    max_importance_score: int = 0
    escalation_detected: bool = False
    human_review_recommended: bool = False


class LLMStageAttemptRecord(BaseModel):
    stage_name: str
    prompt_key: str
    prompt_version: int = 1
    rendered_prompt: str
    model: str
    raw_output: str
    validation_status: str
    validation_error: str | None = None
    repair_attempted: bool = False
    related_account_id: int | None = None
    related_post_id: int | None = None
    related_comment_id: int | None = None
