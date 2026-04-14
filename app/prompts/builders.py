from __future__ import annotations

from collections import Counter
from statistics import mean


KNOWN_ORGANIZATIONS = (
    "PKK/KCK/HPG/YPG/PYD/TAK, DHKP-C/Dev-Sol, FETÖ/PDY, DEAŞ/IŞİD, "
    "MLKP, TİKKO/TKP-ML, El-Kaide, Hizbullah (Türkiye) ve bağlantılı yapılar"
)
PROFILE_SUMMARY_MAX_WORDS = 550
TURKISH_OUTPUT_GUARD = "Cikti dili zorunlu: Turkce"


SHARED_SYSTEM_PROMPT_TEMPLATE = """You are a social media investigation and risk analysis engine for open-source content review.

Mission:
- Start from a seed account.
- Identify likely organizational alignment, behavior pattern, and operational relevance.
- Surface related actors, risky clusters, and plausible expansion candidates for follow-up research.
- Produce concise analyst-ready outputs that can later support relationship graphs and branching investigation.

Your task is to analyze posts, media items, captions, profile bios, prior posting patterns, comments, and graph summaries for indicators related to:
- propaganda
- symbolic affiliation
- direct support expressions
- leader/cadre praise
- mobilization or calls to action
- coordination signals
- threats or target designation
- organized crime indicators
- violent extremist or criminal network alignment

Core principles:
1. Separate observation from inference.
2. Do not make definitive claims of membership, guilt, or criminal responsibility.
3. Use conservative reasoning.
4. A single weak signal is not enough for a strong conclusion, but clear plain-language support/opposition in a comment should still be classified.
5. Use "unclear" only when the communicative intent or evidence is genuinely indeterminate.
6. Distinguish journalism, reporting, criticism, irony, mourning, and propaganda.
7. Consider profile history and repeated behavior patterns only when they are explicitly provided.
8. Prefer short, factual, reusable evidence phrases that can support later graph and follow-up analysis.
9. Use institutional, cautious, non-accusatory language suitable for analyst review.
10. Return only valid JSON matching the provided schema when a JSON schema is provided.
11. Scores must be evidence-based and calibrated.

Decision rules:
- One alias match alone is never enough for medium or high confidence.
- One symbol alone is never enough for a strong conclusion.
- At least 2 independent signals are required for medium confidence.
- At least 3 independent signals are required for high confidence.
- If evidence conflicts, lower confidence.
- If context suggests reporting or criticism, do not treat it as support unless there is clear endorsement.
- For comments, decide the communicative stance first, then decide risk level.
- For comments, if the text is plainly supportive, oppositional, neutral-small-talk, slogan-like, threatening, coordinating, insulting, or sharing actionable information, do not choose "unclear".
- For organization assessment, treat aligned entities as ranked evidence. Put the strongest/primary entity first and weaker or secondary entities later.
- Separate direct support, symbolic affinity, repeated narrative overlap, coordination signals, recruitment signals, and possible network-node behavior.
"""


JSON_REPAIR_PROMPT_TEMPLATE = """Repair the following model output so that it becomes valid JSON matching the target schema.

Rules:
- Return only valid JSON.
- Do not include markdown code fences.
- Preserve meaning where possible.
- If a field is missing, use the safest schema-valid fallback.
- Ignore any surrounding metadata, CSV fragments, or duplicated partial outputs outside the intended JSON.
- If the JSON is truncated, complete it conservatively using the schema and the visible content only.

TARGET SCHEMA:
[[TARGET_SCHEMA]]

INVALID OUTPUT:
[[INVALID_OUTPUT]]
"""


POST_ANALYSIS_JSON_SCHEMA = """{
  "content_types": ["news","announcement","propaganda","commemoration","activity_march","violence_conflict","political_message","religious_message","personal_daily","fundraising","unclear"],
  "primary_theme": ["leader_praise","organizational_symbolism","victimhood_narrative","resistance_narrative","gathering_call","mourning","celebration","threat","hate","information_sharing","unclear"],
  "summary_tr": "string",
  "language_and_tone": {
    "dominant_language": "tr|ku|ar|en|mixed|unclear",
    "tone": "neutral|emotional|angry|mobilizing|mourning|celebratory|threatening|unclear",
    "sloganized_language": "yes|no|unclear"
  },
  "risk_indicators": {
    "direct_support_expression": {"status": "present|absent|unclear", "evidence": ["string"]},
    "organizational_symbol_use": {"status": "present|absent|unclear", "evidence": ["string"]},
    "leader_or_cadre_praise": {"status": "present|absent|unclear", "evidence": ["string"]},
    "violence_praise_or_justification": {"status": "present|absent|unclear", "evidence": ["string"]},
    "call_to_action_or_gathering": {"status": "present|absent|unclear", "evidence": ["string"]},
    "coordination_signal": {"status": "present|absent|unclear", "evidence": ["string"]},
    "fundraising_or_resource_request": {"status": "present|absent|unclear", "evidence": ["string"]},
    "targeting_or_threat": {"status": "present|absent|unclear", "evidence": ["string"]},
    "organized_crime_indicator": {"status": "present|absent|unclear", "evidence": ["string"]}
  },
  "organization_assessment": {
    "aligned_entities": [
      {
        "entity": "string",
        "relationship_type": "direct_support|symbolic_affinity|repeated_narrative|weak_signal|unclear",
        "confidence": "low|medium|high",
        "reason_tr": "string"
      }
    ],
    "organization_link_score": 0,
    "confidence": "low|medium|high"
  },
  "profile_role_estimate": {
    "role": "supporter|propaganda_distributor|sympathizer|news_sharer|event_participant|possible_organizer|possible_network_node|unclear",
    "reason_tr": "string"
  },
  "behavior_pattern": {
    "single_instance": "yes|no|unclear",
    "repeated_theme": "yes|no|unclear",
    "escalation_signal": "yes|no|unclear",
    "reason_tr": "string"
  },
  "review_priority": {
    "importance_score": 1,
    "priority_level": "low|medium|high|critical",
    "human_review_required": "yes|no",
    "reason_tr": "string"
  },
  "analyst_note_tr": "string"
}"""


COMMENT_ANALYSIS_JSON_SCHEMA = """{
  "comment_type": "support|opposition|neutral|slogan|coordination|threat|insult|information_sharing|unclear",
  "content_summary_tr": "string",
  "sentiment": "positive|negative|neutral",
  "flags": {
    "active_supporter": {"flag": false, "reason_tr": "string"},
    "threat": {"flag": false, "reason_tr": "string"},
    "information_leak": {"flag": false, "reason_tr": "string"},
    "coordination": {"flag": false, "reason_tr": "string"},
    "hate_speech": {"flag": false, "reason_tr": "string"}
  },
  "organization_link_assessment": {
    "organization_link_score": 0,
    "confidence": "low|medium|high",
    "reason_tr": "string"
  },
  "behavior_pattern": {
    "consistent_with_history": "yes|no|unclear",
    "repeated_support_language": "yes|no|unclear",
    "reason_tr": "string"
  },
  "overall_risk": {
    "level": "low|medium|high|critical",
    "human_review_required": "yes|no"
  },
  "review": {
    "importance_score": 1,
    "priority_level": "low|medium|high|critical",
    "human_review_required": "yes|no",
    "confidence": "low|medium|high",
    "reason": "string"
  }
}"""


MEDIA_OBSERVATION_JSON_SCHEMA = """{
  "media_type": "image|video|text_image|unclear",
  "scene_summary": "string",
  "setting_type": "indoor|outdoor|street|crowd_area|stage|vehicle|unclear",
  "visible_person_count": "string",
  "face_visibility": "open|partially_covered|fully_covered|mixed|unclear",
  "clothing_types": ["civilian","uniform","camouflage","masked","unclear"],
  "notable_objects": ["string"],
  "weapon_presence": {
    "status": "yes|no|unclear",
    "types": ["firearm","long_gun","handgun","blade","explosive_like","unclear"]
  },
  "symbols_or_logos": [
    {
      "type": "flag|emblem|banner|badge|poster|text|gesture|unclear",
      "description": "string",
      "visible_text": "string"
    }
  ],
  "visible_text_items": [
    {
      "text": "string",
      "language": "tr|ku|ar|en|mixed|unclear"
    }
  ],
  "activity_type": ["rally","march","commemoration","celebration","speech","music_dance","conflict","daily_life","unclear"],
  "crowd_level": "single|small_group|crowd|unclear",
  "audio_elements": {
    "speech": "present|absent|unclear",
    "music": "present|absent|unclear",
    "chanting": "present|absent|unclear",
    "gunfire_or_blast": "present|absent|unclear"
  },
  "child_presence": "yes|no|unclear",
  "institutional_markers": ["police","military","municipality","media","civil_society","unclear"],
  "vehicles": ["car","pickup","motorcycle","armored_vehicle","public_transport","unclear"],
  "license_or_signage": ["string"],
  "deep_review_required": true,
  "raw_observation_note_tr": "string"
}"""

MEDIA_DEEP_ANALYSIS_JSON_SCHEMA = """{
  "location_assessment": {
    "location_identifiable": "yes|no|unclear",
    "location_confidence": "low|medium|high",
    "candidate_location_text": "string",
    "evidence": ["string"]
  },
  "vehicle_plate_assessment": {
    "vehicle_present": "yes|no|unclear",
    "vehicles": ["string"],
    "plate_visible": "yes|no|unclear",
    "plate_text_candidates": ["string"],
    "evidence": ["string"]
  },
  "sensitive_information": [
    {
      "type": "location|identity|operational|institutional|unclear",
      "value": "string",
      "confidence": "low|medium|high",
      "reason_tr": "string"
    }
  ],
  "followup_priority": "low|medium|high|critical",
  "analyst_note_tr": "string"
}"""


POST_ANALYSIS_PROMPT_TEMPLATE = """You are analyzing a complete social media post as evidence within a branching account investigation.

Mission:
Assess the post for risk indicators related to propaganda, symbolic affiliation, direct support, mobilization, coordination, threats, organized crime indicators, and network-aligned narratives.
Also determine whether this post contributes meaningful evidence about:
- likely primary organization alignment
- secondary or weaker organization links
- account role pattern
- whether the account may matter for downstream relationship mapping or follow-up targeting

Reference organizations:
[[KNOWN_ORGANIZATIONS]]

Profile context:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]
- Bio: [[BIO]]
- Caption: [[CAPTION]][[FOCUS_BLOCK]]

Additional context:
[[PROFILE_BLOCK]]
[[HISTORY_BLOCK]]
[[MEDIA_BLOCK]]

Task:
Evaluate all media items, the caption, profile context, and prior behavior together.

Important constraints:
- Use only the context that is actually provided in this prompt. If profile/history blocks are absent, do not assume hidden prior context.
- Do not make definitive claims of membership, guilt, or criminal responsibility.
- Use "unclear" when evidence is insufficient.
- Distinguish reporting, criticism, mourning, and irony from support or propaganda.
- Base higher scores only on multiple independent signals.
- In `organization_assessment.aligned_entities`, rank entities from strongest to weakest evidence instead of forcing a single label.
- Use `relationship_type` carefully: distinguish direct support, symbolic affinity, repeated narrative overlap, weak signal, and uncertain linkage.
- In `profile_role_estimate`, think in network terms: supporter, propagandist, amplifier, event participant, possible organizer, or possible network node.
- Reuse short evidence phrases that could later help graph summaries and follow-up target selection.
- Keep text concise.
- "summary_tr" and "analyst_note_tr" must each be max 2 sentences.
- Return only JSON.
- Do not use markdown code fences.

Schema:
[[POST_ANALYSIS_JSON_SCHEMA]]

Scoring Guide:
- organization_link_score: 0-10 evidence-based only.
- importance_score: 1-10 review priority only.
- confidence low = one weak signal or ambiguous context.
- confidence medium = at least 2 independent signals.
- confidence high = at least 3 independent signals strongly aligned in the same direction.
- If context suggests news reporting, critique, satire, or mourning without endorsement, lower both score and confidence.
- If signals conflict, lower confidence.
- If evidence is insufficient, prefer "unclear".
- For multi-entity cases, mention the primary entity first and keep secondary entities cautious and evidence-tied."""


MEDIA_ANALYSIS_PROMPT_TEMPLATE = """You are analyzing one single media item from a social media post.

Context:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]
- Bio: [[BIO]]
- Caption: [[CAPTION]]
- İncelenen parça: [[MEDIA_INDEX]]/[[MEDIA_COUNT]]
- İncelenen parça / Media item: [[MEDIA_INDEX]]/[[MEDIA_COUNT]] ([[MEDIA_LABEL]])

Task:
Report only what is directly visible or clearly audible in this media item.
Do not infer membership, intent, or criminal role.
If unclear, write "unclear".

Rules:
- Be short, factual, and concrete.
- Separate observable content from uncertainty.
- If there is visible text, transcribe briefly if legible.
- If this is a video, only report clearly perceivable visual/audio elements.
- Decide whether a second-pass deep analysis could realistically extract additional actionable details (for example location clues, plate-like text, sensitive operational hints). Set `deep_review_required` to true or false.
- Return only one JSON object.
- Do not use markdown code fences.

Schema:
[[MEDIA_OBSERVATION_JSON_SCHEMA]]"""

MEDIA_DEEP_ANALYSIS_PROMPT_TEMPLATE = """You are performing a second-pass deep investigation on one media item.

Context:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]
- Bio: [[BIO]]
- Caption: [[CAPTION]]
- İncelenen parça / Media item: [[MEDIA_INDEX]]/[[MEDIA_COUNT]] ([[MEDIA_LABEL]])

First-pass observation summary:
[[MEDIA_OBSERVATION_CONTEXT]]

Task:
Focus on actionable, high-value details:
- whether location can be identified
- whether vehicles and plate-like text exist
- whether sensitive or operational information is visible

Rules:
- Report only what is visible/audible.
- If uncertain, return "unclear" and low confidence.
- Do not infer criminal responsibility.
- Return only one JSON object.
- Do not use markdown code fences.

Schema:
[[MEDIA_DEEP_ANALYSIS_JSON_SCHEMA]]"""


COMMENT_ANALYSIS_PROMPT_TEMPLATE = """You are analyzing a single comment under an account that is already under investigation.

Context:
[[ACCOUNT_ALIGNMENT_BLOCK]]
[[ACCOUNT_PROFILE_BLOCK]]
[[POST_EVIDENCE_BLOCK]]
[[POST_ANALYSIS]]

Post owner:
- Username: [[USERNAME]]
- Bio: [[BIO]]
- Caption: [[CAPTION]]

Comment:
- Comment owner: [[COMMENTER_USERNAME]]
- Comment text: [[COMMENT_TEXT]]
[[COMMENTER_HISTORY_BLOCK]]

Task:
Analyze the comment as a reaction to this investigated account/post.

Rules:
- Read the comment in relation to this specific investigated account/post.
- If the comment expresses praise, admiration, approval, encouragement, solidarity, celebration, or symbolic positive support toward the investigated account/post, treat it as a support signal unless it is clearly critical, mocking, or unrelated.
- organization_link_score measures supportive or sympathetic alignment relevance in this focus context, not proof of membership.
- Lack of strong evidence for formal organizational connection does not require score 0.
- If a comment is supportive toward the investigated account/post, do not under-score it as 0-1 by default; use evidence-based but assertive calibration.
- Use score 0 only when the comment is neutral, unrelated, oppositional, or has no meaningful supportive/aligned signal.
- Stronger, repeated, slogan-like, or more explicit support should receive a higher score.
- If comment history shows repeated supportive behavior, reflect that in the reasoning and score.
- If the comment is classified as support or slogan toward the investigated account/post, flags.active_supporter.flag should normally be true unless the support is extremely weak or genuinely ambiguous.
- If the comment is supportive, do not describe sentiment as neutral. Use a positive/supportive sentiment label if sentiment is produced downstream.
- Supportive comments in this focus context should usually receive importance_score 2 or higher unless the support is negligible.
- If the comment includes threats, coordination, leaks, names, relationships, hierarchy, plans, locations, or operational details, reflect that clearly in the output.
- Keep reasoning standardized:
  1. state the comment stance,
  2. state the support/alignment strength in this focus context,
  3. state whether explicit organizational language is present.
- Do not make definitive claims of criminal responsibility.
- Return only one JSON object.
- Do not use markdown code fences.

Scoring:
- 0 = neutral, unrelated, oppositional, or no meaningful supportive/aligned signal
- 1-2 = very weak support only (single vague praise, no slogan, no history)
- 3-4 = weak but real support, praise, approval, or symbolic support
- 5-6 = clear support, slogan-like support, or repeated supportive behavior
- 7-8 = explicit loyalty, repeated committed support, or strong supportive history
- 9-10 = coordination, threat, leak, operational detail, or very strong repeated alignment

Calibration guidance for support-heavy contexts:
- If comment_type is support or slogan, score should usually be >=3 unless evidence is extremely weak.
- If post owner organization link score >=6 and comment supports the account/post, score should usually be >=4.
- If commenter history indicates repeated support language (or 2+ prior supportive comments), score should usually be >=5.
- If explicit slogan/loyalty language exists or repeated support is strong, score should usually be >=6-7.

Schema:
[[COMMENT_ANALYSIS_JSON_SCHEMA]]"""


PARENT_POST_ANALYSIS_PROMPT_TEMPLATE = """You are merging multiple standalone media-level analyses into one final social media post assessment for a branching account investigation.

Mission:
Assess the complete post for risk indicators related to propaganda, symbolic affiliation, direct support, mobilization, coordination, threats, organized crime indicators, and network-aligned narratives.
Also determine whether the merged post strengthens evidence about a primary aligned entity, secondary links, and the account's likely network role.

Reference organizations:
[[KNOWN_ORGANIZATIONS]]

Profile context:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]
- Bio: [[BIO]]
- Caption: [[CAPTION]][[FOCUS_BLOCK]]
- Media item count: [[MEDIA_COUNT]]

Standalone media analyses:
[[CHILD_ANALYSES_BLOCK]]

Task:
Combine the standalone media analyses with the caption and profile context, then produce one final post-level assessment.

Important constraints:
- Do not use cross-post history or profile-history assumptions.
- Use only the child analyses and the explicitly provided caption/profile context.
- Resolve conflicts conservatively; if the media items disagree, lower confidence.
- Do not make definitive claims of membership, guilt, or criminal responsibility.
- Use "unclear" when evidence is insufficient.
- Distinguish reporting, criticism, mourning, and irony from support or propaganda.
- In `organization_assessment.aligned_entities`, rank entities from strongest to weakest evidence and avoid forcing a single organization label when the signals are mixed.
- Separate direct support, symbolic affinity, repeated narrative overlap, coordination, recruitment-like messaging, and possible network-node behavior.
- Reuse short evidence phrases that can later support graph interpretation and follow-up candidate evaluation.
- Keep text concise.
- "summary_tr" and "analyst_note_tr" must each be max 2 sentences.
- Return only JSON.
- Do not use markdown code fences.

Schema:
[[POST_ANALYSIS_JSON_SCHEMA]]

Scoring Guide:
- organization_link_score: 0-10 evidence-based only.
- importance_score: 1-10 review priority only.
- confidence low = one weak signal or ambiguous context.
- confidence medium = at least 2 independent signals.
- confidence high = at least 3 independent signals strongly aligned in the same direction.
- If caption/context suggests news reporting, critique, satire, or mourning without endorsement, lower both score and confidence.
- If signals conflict, lower confidence.
- If evidence is insufficient, prefer "unclear".
- For multi-entity cases, mention the primary entity first and keep secondary entities cautious and evidence-tied.""" 


ACCOUNT_PROFILE_UPDATE_PROMPT_TEMPLATE = """You are updating a persistent profile summary for a social media account.

Goal:
- Summarize the account's general behavior pattern in a short, dense, current form.
- If the new post is low-value, mostly preserve the current summary.
- If the new post meaningfully changes organization alignment, threat level, or behavior pattern, update the summary accordingly.
- Remove repetitive and low-value detail.

Account:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]

Current profile summary:
[[CURRENT_SUMMARY]]

Latest post analysis:
- Summary: [[LATEST_POST_SUMMARY]]
- Categories: [[LATEST_POST_CATEGORIES]]
- Threat level: [[LATEST_THREAT_LEVEL]]
- Detected organization: [[LATEST_DETECTED_ORG]]
- Importance score: [[LATEST_IMPORTANCE_SCORE]]

Historical account overview:
[[HISTORY_STATS_CONTEXT]]

Output rules:
- Use at most [[PROFILE_SUMMARY_MAX_WORDS]] words.
- Return plain text only. Do not use JSON or markdown code fences.
- Start with overall account character, then dominant content pattern, then organization/risk trend.
- If the new post does not create a meaningful shift, avoid unnecessarily expanding the old summary.
- If evidence is uncertain, use cautious wording.
- Write 1 short paragraph or at most 2 short paragraphs."""


ACCOUNT_FINAL_SUMMARY_PROMPT_TEMPLATE = """You are writing the final persistent profile summary for a social media account after a full ingest run.

This summary also serves as the final persistent investigation summary for branching account review.

Goal:
- Summarize the account's overall behavior pattern using the finalized post set.
- Focus on repeated content patterns, organization/risk trends, and the overall account character.
- Explain why this account matters, or does not matter, in a branching investigation.
- Consider whether the account looks most like a supporter, propagandist, amplifier, event hub, or possible network node.
- Be dense, cautious, and non-repetitive.

Account:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]
- Bio: [[BIO]]

Historical account overview:
[[HISTORY_STATS_CONTEXT]]

Finalized post rollup:
[[FINAL_POSTS_CONTEXT]]

Output rules:
- Use at most [[PROFILE_SUMMARY_MAX_WORDS]] words.
- Return plain text only. Do not use JSON or markdown code fences.
- Start with overall account character, then dominant content pattern, then primary organization alignment and secondary links, then risk/importance trend.
- Use only the finalized post set in this prompt; do not invent continuity beyond it.
- Use cautious wording when evidence is mixed or uncertain.
- Write 1 short paragraph or at most 2 short paragraphs."""


GRAPH_ANALYSIS_PROMPT_TEMPLATE = """You are interpreting a relationship graph for a social media account in an operational network-analysis context.

Goal:
- Explain the main takeaway an analyst should derive from this graph.
- Identify the strongest clusters around the seed account.
- Name the most connected or highest-risk related actors.
- Explain whether ties look common, supportive, organizational, threat-related, or otherwise operationally relevant.
- End with a brief "main analytical takeaway" and "recommended follow-up targets".
- Be concise, interpretive, and avoid repetition.

Account:
- Platform username: [[INSTAGRAM_USERNAME]]
- Bio: [[BIO]]

Current profile summary:
[[ACCOUNT_PROFILE_SUMMARY]]

Relationship graph summary:
[[GRAPH_SUMMARY]]

Additional note:
- A high-resolution graph image may also be provided with this request.
- If an image is present, use it only as a supporting source.
- Always prioritize the textual and numeric graph summary.
- If parts of the image are unclear, do not guess beyond the textual summary.

Output rules:
- Return plain text only. Do not use JSON or markdown code fences.
- Use at most 220 words.
- Write 2 short paragraphs or at most 4 short bullets.
- If you mention counts, clearly explain what they refer to.
- If evidence is uncertain, use cautious wording.
- Start with the big picture, then mention the most important risks or attention points.
- Use institutional, presentation-ready language for analyst or official review."""


FOLLOWUP_CANDIDATE_ANALYSIS_JSON_SCHEMA = """{
  "candidate_username": "string",
  "relationship_to_seed": "supporter|peer|amplifier|possible_operator|unclear",
  "relationship_strength": "low|medium|high",
  "risk_level": "low|medium|high|critical",
  "primary_entity": "string",
  "secondary_entities": ["string"],
  "trigger_signals": ["string"],
  "branch_recommended": "yes|no",
  "priority_rank": 1,
  "reason_tr": "string"
}"""


FOLLOWUP_CANDIDATE_ANALYSIS_PROMPT_TEMPLATE = """You are evaluating whether a related social media actor should become a new branch target in an expanding account investigation.

Goal:
- Determine whether this related actor is worth follow-up investigation.
- Estimate the actor's relationship to the seed account.
- Summarize the strongest triggers for branching in short analyst-ready language.

Seed account:
- Username: [[USERNAME]]
- Platform username: [[INSTAGRAM_USERNAME]]
- Focus entity: [[FOCUS_ENTITY]]

Seed account investigation summary:
[[SEED_ACCOUNT_SUMMARY]]

Relationship evidence:
[[RELATIONSHIP_EVIDENCE]]

Interaction snippets:
[[INTERACTION_SNIPPETS]]

Graph tie summary:
[[GRAPH_TIE_SUMMARY]]

Candidate under review:
- Candidate username: [[CANDIDATE_USERNAME]]

Rules:
- Use only the provided evidence.
- Do not make definitive claims of membership, guilt, or criminal responsibility.
- Prefer cautious wording when evidence is mixed.
- `relationship_to_seed` should capture the actor's functional relationship to the seed account.
- `relationship_strength` should reflect the observed tie strength, not ideological certainty.
- `primary_entity` should reflect the strongest linked entity if any; otherwise use "unclear".
- Put only weaker or secondary links in `secondary_entities`.
- `trigger_signals` should be short reusable phrases.
- Set `branch_recommended="yes"` when the actor appears operationally relevant, repeatedly connected, high-risk, or a strong expansion candidate.
- `priority_rank` must be between 1 and 5, where 1 is highest follow-up priority.
- `reason_tr` must be concise and defensible.
- Return only one JSON object.
- Do not use markdown code fences.

Schema:
[[FOLLOWUP_CANDIDATE_ANALYSIS_JSON_SCHEMA]]"""


def _render_prompt_template(template: str, values: dict[str, object]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"[[{key}]]", str(value))
    return rendered


def _append_turkish_output_guard(prompt: str, *, expect_json: bool) -> str:
    if TURKISH_OUTPUT_GUARD in prompt:
        return prompt
    if expect_json:
        suffix = (
            "\n\n"
            "Cikti dili zorunlu: Turkce.\n"
            "- Tum serbest metin alanlarini (summary_tr, analyst_note_tr, content_summary_tr, reason_tr, review.reason dahil) yalnizca Turkce yaz.\n"
            "- Ingilizce kelime ve cumle kullanma; zorunlu ozel isimler disinda yabanci dil kullanma.\n"
            "- JSON semasina sadik kal ve yalnizca gecerli JSON dondur."
        )
    else:
        suffix = (
            "\n\n"
            "Cikti dili zorunlu: Turkce.\n"
            "- Yanit metnini yalnizca Turkce yaz.\n"
            "- Ingilizce cumle kurma; zorunlu ozel isimler disinda yabanci dil kullanma."
        )
    return prompt.rstrip() + suffix


def get_shared_system_prompt(template_content: str | None = None) -> str:
    base = template_content or SHARED_SYSTEM_PROMPT_TEMPLATE
    return _append_turkish_output_guard(base, expect_json=False)


def build_json_repair_prompt(invalid_output: str, target_schema: str, template_content: str | None = None) -> str:
    template = template_content or JSON_REPAIR_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "INVALID_OUTPUT": invalid_output,
            "TARGET_SCHEMA": target_schema,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)


def _build_post_history_context(summaries: list[dict], max_full: int = 5) -> str:
    if not summaries:
        return ""

    full_items = summaries[-max_full:]
    truncated_items = summaries[:-max_full]

    parts: list[str] = []
    if truncated_items:
        category_counter: Counter[str] = Counter()
        threat_counter: Counter[str] = Counter()
        org_counter: Counter[str] = Counter()
        scores: list[int] = []
        for item in truncated_items:
            for category in item.get("icerik_kategorisi", []) or []:
                if isinstance(category, str):
                    category_counter[category] += 1
            threat = str(item.get("tehdit_seviyesi") or "belirsiz")
            threat_counter[threat] += 1
            orgut = str(item.get("orgut") or "belirsiz")
            org_counter[orgut] += 1
            try:
                scores.append(int(item.get("onem_skoru") or 0))
            except (TypeError, ValueError):
                pass

        category_summary = ", ".join(f"{name}: {count}" for name, count in category_counter.most_common(5)) or "yok"
        avg_score = round(mean(scores), 1) if scores else 0
        dominant_threat = threat_counter.most_common(1)[0][0] if threat_counter else "belirsiz"
        dominant_org = org_counter.most_common(1)[0][0] if org_counter else "belirsiz"
        parts.append(
            f"ÖNCEKİ GÖNDERİLER İSTATİSTİĞİ ({len(truncated_items)} gönderi): {category_summary}\n"
            f"Ortalama tehdit seviyesi: {dominant_threat} | En sık örgüt: {dominant_org} | "
            f"Önem skoru ortalaması: {avg_score}"
        )

    parts.append("KULLANICININ ÖNCEKİ GÖNDERİLERİ (eskiden yeniye):")
    total = len(full_items)
    for index, item in enumerate(full_items, 1):
        date_label = str(item.get("tarih") or "-")
        categories = item.get("icerik_kategorisi", []) or []
        category_text = ", ".join(str(cat) for cat in categories if cat) or "belirsiz"
        parts.append(
            f"[{index}/{total}] Tarih: {date_label} | Özet: {item.get('ozet') or '-'} | "
            f"Kategori: {category_text} | Tehdit: {item.get('tehdit_seviyesi') or 'belirsiz'} | "
            f"Örgüt: {item.get('orgut') or 'belirsiz'}"
        )

    parts.append(
        "Bu kullanıcının önceki gönderilerini ve davranış örüntüsünü göz önünde bulundurarak "
        "yeni gönderiyi bütüncül biçimde değerlendir."
    )
    return "\n".join(parts)


def _build_account_profile_stats_context(summaries: list[dict]) -> str:
    if not summaries:
        return "Henüz geçmiş gönderi bulunmuyor."

    kind_counter: Counter[str] = Counter()
    category_counter: Counter[str] = Counter()
    threat_counter: Counter[str] = Counter()
    org_counter: Counter[str] = Counter()
    scores: list[int] = []

    for item in summaries:
        kind = str(item.get("source_kind") or "post").strip() or "post"
        kind_counter[kind] += 1
        for category in item.get("icerik_kategorisi", []) or []:
            if isinstance(category, str):
                category_counter[category] += 1
        threat = str(item.get("tehdit_seviyesi") or "belirsiz")
        threat_counter[threat] += 1
        orgut = str(item.get("orgut") or "belirsiz")
        org_counter[orgut] += 1
        try:
            scores.append(int(item.get("onem_skoru") or 0))
        except (TypeError, ValueError):
            pass

    content_mix = ", ".join(f"{name}: {count}" for name, count in kind_counter.most_common()) or "yok"
    category_summary = ", ".join(f"{name}: {count}" for name, count in category_counter.most_common(5)) or "yok"
    threat_summary = ", ".join(f"{name}: {count}" for name, count in threat_counter.most_common()) or "yok"
    org_summary = ", ".join(f"{name}: {count}" for name, count in org_counter.most_common(5)) or "yok"
    avg_score = round(mean(scores), 1) if scores else 0
    return (
        f"Toplam gönderi: {len(summaries)}\n"
        f"İçerik kırılımı: {content_mix}\n"
        f"En sık kategoriler: {category_summary}\n"
        f"Tehdit dağılımı: {threat_summary}\n"
        f"Örgüt dağılımı: {org_summary}\n"
        f"Önem skoru ortalaması: {avg_score}"
    )


def _build_commenter_history_context(history: list[dict], max_items: int = 10) -> str:
    if not history:
        return ""

    recent_items = history[-max_items:]
    verdict_counter = Counter(str(item.get("verdict") or "belirsiz") for item in history)
    flagged = any(bool(item.get("bayrak")) for item in history)
    scores = []
    for item in history:
        try:
            scores.append(int(item.get("orgut_baglanti_skoru") or 0))
        except (TypeError, ValueError):
            pass

    lines = ["BU YORUMCUNUN ÖNCEKİ YORUMLARI:"]
    for index, item in enumerate(recent_items, 1):
        lines.append(
            f"[{index}] Gönderi: \"{item.get('post_ozet') or '-'}\" | "
            f"Yorum: \"{item.get('comment_text') or '-'}\" | "
            f"Verdict: {item.get('verdict') or 'belirsiz'} | "
            f"Bağlantı skoru: {item.get('orgut_baglanti_skoru') or 0}"
        )

    verdict_summary = ", ".join(f"{name}: {count}" for name, count in verdict_counter.most_common()) or "yok"
    avg_score = round(mean(scores), 1) if scores else 0
    lines.append(
        f"Toplam: {len(history)} yorum | {verdict_summary} | "
        f"Bayraklı geçmiş: {'Evet' if flagged else 'Hayır'} | Ort. bağlantı skoru: {avg_score}"
    )
    lines.append("Önceki davranış örüntüsünü dikkate alarak bu yeni yorumu değerlendir.")
    return "\n".join(lines)


def _build_comment_account_alignment_context(
    *,
    focus_entity: str | None,
    detected_entities: list[str] | None,
    role: str | None,
    organization_link_score: int | None,
) -> str:
    entities: list[str] = []
    if focus_entity and focus_entity != "belirsiz":
        entities.append(focus_entity)
    for item in detected_entities or []:
        cleaned = str(item or "").strip()
        if cleaned and cleaned != "belirsiz" and cleaned not in entities:
            entities.append(cleaned)

    lines = ["- Investigative focus entity: " + (focus_entity or "unclear")]
    if entities:
        lines.append("- Post owner suspected/aligned entities: " + ", ".join(entities))
    else:
        lines.append("- Post owner suspected/aligned entities: unclear")
    lines.append("- Post owner estimated role: " + (role or "unclear"))
    if organization_link_score is None:
        lines.append("- Post owner organization link score: unclear")
    else:
        lines.append(f"- Post owner organization link score: {max(0, min(10, int(organization_link_score)))}")
    lines.append(
        "- Read the comment as a reaction to this specific account and post. The investigative focus is analyst-provided working context, so praise or encouragement toward the account/post should be evaluated against that context even when the specific post is personal or low-signal."
    )
    return "\n".join(lines)


def _build_comment_post_evidence_context(
    *,
    post_summary: str | None,
    caption: str | None,
    categories: list[str] | None,
    threat_level: str | None,
) -> str:
    category_text = ", ".join(str(item).strip() for item in (categories or []) if str(item).strip()) or "unclear"
    return "\n".join(
        [
            "- Post summary: " + (post_summary or "unclear"),
            "- Post categories: " + category_text,
            "- Post threat level: " + (threat_level or "unclear"),
            "- Post caption: " + (caption or "-"),
        ]
    )


def _build_parent_post_analysis_context(single_media_analyses: list[dict[str, object]]) -> str:
    if not single_media_analyses:
        return "Tekil medya analizi bulunmuyor."

    lines: list[str] = []
    total = len(single_media_analyses)
    for index, item in enumerate(single_media_analyses, 1):
        categories = item.get("icerik_kategorisi", []) or []
        category_text = ", ".join(str(category) for category in categories if category) or "belirsiz"
        lines.append(
            f"[{index}/{total}] Medya {item.get('media_index') or index} ({item.get('media_type') or 'image'}) | "
            f"Özet: {item.get('ozet') or '-'} | "
            f"Kategori: {category_text} | "
            f"Tehdit: {item.get('tehdit_seviyesi') or 'belirsiz'} | "
            f"Örgüt: {item.get('orgut') or 'belirsiz'} | "
            f"Not: {item.get('analist_notu') or '-'}"
        )
    return "\n".join(lines)


def _build_account_final_posts_context(summaries: list[dict], max_items: int | None = None) -> str:
    if not summaries:
        return "Henüz gönderi bulunmuyor."

    # Final profile summary should consider the full posting history by default.
    # Keep the optional cap only for explicit callers that need a hard limit.
    recent_items = summaries[-max_items:] if isinstance(max_items, int) and max_items > 0 else summaries
    lines = ["Finalized posts (eskiden yeniye):"]
    total = len(recent_items)
    for index, item in enumerate(recent_items, 1):
        content_kind = str(item.get("source_kind") or "post").strip() or "post"
        categories = item.get("icerik_kategorisi", []) or []
        category_text = ", ".join(str(category) for category in categories if category) or "belirsiz"
        lines.append(
            f"[{index}/{total}] Tür: {content_kind} | Tarih: {item.get('tarih') or '-'} | "
            f"Özet: {item.get('ozet') or '-'} | "
            f"Kategori: {category_text} | "
            f"Tehdit: {item.get('tehdit_seviyesi') or 'belirsiz'} | "
            f"Örgüt: {item.get('orgut') or 'belirsiz'} | "
            f"Önem: {item.get('onem_skoru') or 0}"
        )
    return "\n".join(lines)


def build_post_analysis_prompt(
    username: str,
    instagram_username: str | None,
    bio: str | None,
    caption: str | None,
    post_history_context: str,
    account_profile_summary: str = "",
    media_context: str = "",
    known_organizations: str | None = None,
    focus_entity: str | None = None,
    template_content: str | None = None,
) -> str:
    template = template_content or POST_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "KNOWN_ORGANIZATIONS": known_organizations or KNOWN_ORGANIZATIONS,
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "BIO": bio or "-",
            "CAPTION": caption or "-",
            "FOCUS_BLOCK": (
                f"\n- Öncelikli inceleme odağı: {focus_entity}\nBu odağa göre sinyalleri özellikle kontrol et; "
                "ama başka güçlü sinyalleri gizleme veya yok sayma."
                if focus_entity
                else ""
            ),
            "PROFILE_BLOCK": f"\n\nKULLANICI PROFİL ÖZETİ:\n{account_profile_summary}" if account_profile_summary else "",
            "HISTORY_BLOCK": f"\n\n{post_history_context}" if post_history_context else "",
            "MEDIA_BLOCK": f"\n\nMEDIA SUMMARY / GÖNDERİDEKİ MEDYA PARÇALARININ ÖN ANALİZLERİ:\n{media_context}" if media_context else "",
            "POST_ANALYSIS_JSON_SCHEMA": POST_ANALYSIS_JSON_SCHEMA,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)


def build_parent_post_analysis_prompt(
    username: str,
    instagram_username: str | None,
    bio: str | None,
    caption: str | None,
    media_count: int,
    single_media_analyses: list[dict[str, object]],
    known_organizations: str | None = None,
    focus_entity: str | None = None,
    template_content: str | None = None,
) -> str:
    template = template_content or PARENT_POST_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "KNOWN_ORGANIZATIONS": known_organizations or KNOWN_ORGANIZATIONS,
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "BIO": bio or "-",
            "CAPTION": caption or "-",
            "MEDIA_COUNT": media_count,
            "FOCUS_BLOCK": (
                f"\n- Öncelikli inceleme odağı: {focus_entity}\nBu odağa göre sinyalleri özellikle kontrol et; "
                "ama başka güçlü sinyalleri gizleme veya yok sayma."
                if focus_entity
                else ""
            ),
            "CHILD_ANALYSES_BLOCK": _build_parent_post_analysis_context(single_media_analyses),
            "POST_ANALYSIS_JSON_SCHEMA": POST_ANALYSIS_JSON_SCHEMA,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)


def build_media_analysis_prompt(
    username: str,
    instagram_username: str | None,
    bio: str | None,
    caption: str | None,
    media_index: int,
    media_count: int,
    media_type: str,
    template_content: str | None = None,
) -> str:
    template = template_content or MEDIA_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "BIO": bio or "-",
            "CAPTION": caption or "-",
            "MEDIA_INDEX": media_index,
            "MEDIA_COUNT": media_count,
            "MEDIA_LABEL": "video" if media_type == "video" else "görsel",
            "MEDIA_OBSERVATION_JSON_SCHEMA": MEDIA_OBSERVATION_JSON_SCHEMA,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)


def build_media_deep_analysis_prompt(
    username: str,
    instagram_username: str | None,
    bio: str | None,
    caption: str | None,
    media_index: int,
    media_count: int,
    media_type: str,
    media_observation_context: str,
    template_content: str | None = None,
) -> str:
    template = template_content or MEDIA_DEEP_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "BIO": bio or "-",
            "CAPTION": caption or "-",
            "MEDIA_INDEX": media_index,
            "MEDIA_COUNT": media_count,
            "MEDIA_LABEL": "video" if media_type == "video" else "gorsel",
            "MEDIA_OBSERVATION_CONTEXT": media_observation_context or "unclear",
            "MEDIA_DEEP_ANALYSIS_JSON_SCHEMA": MEDIA_DEEP_ANALYSIS_JSON_SCHEMA,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)


def build_comment_analysis_prompt(
    post_analysis: str,
    username: str,
    bio: str | None,
    caption: str | None,
    account_profile_summary: str | None,
    commenter_username: str | None,
    comment_text: str,
    commenter_history_context: str,
    focus_entity: str | None = None,
    template_content: str | None = None,
    post_summary: str | None = None,
    post_categories: list[str] | None = None,
    post_detected_entities: list[str] | None = None,
    post_role: str | None = None,
    post_organization_link_score: int | None = None,
    post_threat_level: str | None = None,
) -> str:
    template = template_content or COMMENT_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "POST_ANALYSIS": post_analysis,
            "USERNAME": username,
            "BIO": bio or "-",
            "CAPTION": caption or "-",
            "COMMENTER_USERNAME": commenter_username or "-",
            "COMMENT_TEXT": comment_text,
            "FOCUS_ENTITY": focus_entity or "unclear",
            "ACCOUNT_ALIGNMENT_BLOCK": _build_comment_account_alignment_context(
                focus_entity=focus_entity,
                detected_entities=post_detected_entities,
                role=post_role,
                organization_link_score=post_organization_link_score,
            ),
            "ACCOUNT_PROFILE_BLOCK": (
                f"\nSeed account profile summary:\n{str(account_profile_summary).strip()}"
                if str(account_profile_summary or "").strip()
                else ""
            ),
            "POST_EVIDENCE_BLOCK": _build_comment_post_evidence_context(
                post_summary=post_summary,
                caption=caption,
                categories=post_categories,
                threat_level=post_threat_level,
            ),
            "FOCUS_BLOCK": (
                f"\n- Öncelikli inceleme odağı: {focus_entity}\nBu odağa göre destek, aidiyet, koordinasyon veya karşıtlık sinyallerini özellikle kontrol et."
                if focus_entity
                else ""
            ),
            "COMMENTER_HISTORY_BLOCK": f"\n\n{commenter_history_context}" if commenter_history_context else "",
            "COMMENT_ANALYSIS_JSON_SCHEMA": COMMENT_ANALYSIS_JSON_SCHEMA,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)


def build_account_profile_update_prompt(
    username: str,
    instagram_username: str | None,
    current_summary: str | None,
    latest_post_summary: str,
    latest_post_categories: list[str],
    latest_threat_level: str,
    latest_detected_org: str,
    latest_importance_score: int,
    history_stats_context: str,
    template_content: str | None = None,
) -> str:
    template = template_content or ACCOUNT_PROFILE_UPDATE_PROMPT_TEMPLATE
    categories_text = ", ".join(latest_post_categories) if latest_post_categories else "belirsiz"
    rendered = _render_prompt_template(
        template,
        {
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "CURRENT_SUMMARY": current_summary or "Henüz profil özeti yok.",
            "LATEST_POST_SUMMARY": latest_post_summary,
            "LATEST_POST_CATEGORIES": categories_text,
            "LATEST_THREAT_LEVEL": latest_threat_level,
            "LATEST_DETECTED_ORG": latest_detected_org,
            "LATEST_IMPORTANCE_SCORE": latest_importance_score,
            "HISTORY_STATS_CONTEXT": history_stats_context,
            "PROFILE_SUMMARY_MAX_WORDS": PROFILE_SUMMARY_MAX_WORDS,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=False)


def build_account_final_summary_prompt(
    username: str,
    instagram_username: str | None,
    bio: str | None,
    post_history_summaries: list[dict],
    history_stats_context: str,
    template_content: str | None = None,
) -> str:
    template = template_content or ACCOUNT_FINAL_SUMMARY_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "BIO": bio or "-",
            "HISTORY_STATS_CONTEXT": history_stats_context,
            "FINAL_POSTS_CONTEXT": _build_account_final_posts_context(post_history_summaries),
            "PROFILE_SUMMARY_MAX_WORDS": PROFILE_SUMMARY_MAX_WORDS,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=False)


def build_graph_analysis_prompt(
    instagram_username: str | None,
    bio: str | None,
    account_profile_summary: str | None,
    graph_summary: str,
    template_content: str | None = None,
) -> str:
    template = template_content or GRAPH_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "BIO": bio or "-",
            "ACCOUNT_PROFILE_SUMMARY": account_profile_summary or "Henüz profil özeti yok.",
            "GRAPH_SUMMARY": graph_summary or "Graf özeti üretilemedi.",
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=False)


def build_followup_candidate_analysis_prompt(
    username: str,
    instagram_username: str | None,
    candidate_username: str,
    seed_account_summary: str,
    relationship_evidence: str,
    interaction_snippets: str,
    graph_tie_summary: str,
    focus_entity: str | None = None,
    template_content: str | None = None,
) -> str:
    template = template_content or FOLLOWUP_CANDIDATE_ANALYSIS_PROMPT_TEMPLATE
    rendered = _render_prompt_template(
        template,
        {
            "USERNAME": username,
            "INSTAGRAM_USERNAME": instagram_username or "-",
            "CANDIDATE_USERNAME": candidate_username,
            "FOCUS_ENTITY": focus_entity or "unclear",
            "SEED_ACCOUNT_SUMMARY": seed_account_summary or "Özet bulunmuyor.",
            "RELATIONSHIP_EVIDENCE": relationship_evidence or "İlişki kanıtı bulunmuyor.",
            "INTERACTION_SNIPPETS": interaction_snippets or "Etkileşim örneği bulunmuyor.",
            "GRAPH_TIE_SUMMARY": graph_tie_summary or "Graf bağı özeti bulunmuyor.",
            "FOLLOWUP_CANDIDATE_ANALYSIS_JSON_SCHEMA": FOLLOWUP_CANDIDATE_ANALYSIS_JSON_SCHEMA,
        },
    )
    return _append_turkish_output_guard(rendered, expect_json=True)
