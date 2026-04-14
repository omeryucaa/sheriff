from app.prompts.builders import (
    ACCOUNT_FINAL_SUMMARY_PROMPT_TEMPLATE,
    ACCOUNT_PROFILE_UPDATE_PROMPT_TEMPLATE,
    COMMENT_ANALYSIS_PROMPT_TEMPLATE,
    FOLLOWUP_CANDIDATE_ANALYSIS_PROMPT_TEMPLATE,
    GRAPH_ANALYSIS_PROMPT_TEMPLATE,
    JSON_REPAIR_PROMPT_TEMPLATE,
    MEDIA_ANALYSIS_PROMPT_TEMPLATE,
    MEDIA_DEEP_ANALYSIS_PROMPT_TEMPLATE,
    PARENT_POST_ANALYSIS_PROMPT_TEMPLATE,
    POST_ANALYSIS_PROMPT_TEMPLATE,
    SHARED_SYSTEM_PROMPT_TEMPLATE,
)


PROMPT_TEMPLATE_REGISTRY = {
    "post_analysis": {
        "display_name": "Post Analysis",
        "description": "Main prompt for integrated post-level analysis.",
        "content": POST_ANALYSIS_PROMPT_TEMPLATE,
    },
    "media_analysis": {
        "display_name": "Media Analysis",
        "description": "Single-media observation extraction prompt.",
        "content": MEDIA_ANALYSIS_PROMPT_TEMPLATE,
    },
    "media_deep_analysis": {
        "display_name": "Media Deep Analysis",
        "description": "Second-pass deep inspection for location, plates, vehicles, and sensitive details.",
        "content": MEDIA_DEEP_ANALYSIS_PROMPT_TEMPLATE,
    },
    "comment_analysis": {
        "display_name": "Comment Analysis",
        "description": "Comment analysis prompt for support, threat, coordination, and risk flags.",
        "content": COMMENT_ANALYSIS_PROMPT_TEMPLATE,
    },
    "account_profile_update": {
        "display_name": "Account Profile Update",
        "description": "Updates the persistent account profile summary after post analysis.",
        "content": ACCOUNT_PROFILE_UPDATE_PROMPT_TEMPLATE,
    },
    "account_final_summary": {
        "display_name": "Account Final Summary",
        "description": "Builds the final persistent account summary after the full ingest finishes.",
        "content": ACCOUNT_FINAL_SUMMARY_PROMPT_TEMPLATE,
    },
    "post_analysis_parent_merge": {
        "display_name": "Post Analysis Parent Merge",
        "description": "Merges standalone single-media post analyses into one final post analysis.",
        "content": PARENT_POST_ANALYSIS_PROMPT_TEMPLATE,
    },
    "graph_analysis": {
        "display_name": "Graph Analysis",
        "description": "Explains the relationship graph in concise analyst language.",
        "content": GRAPH_ANALYSIS_PROMPT_TEMPLATE,
    },
    "followup_candidate_analysis": {
        "display_name": "Follow-Up Candidate Analysis",
        "description": "Evaluates whether a related actor should become a new branch investigation target.",
        "content": FOLLOWUP_CANDIDATE_ANALYSIS_PROMPT_TEMPLATE,
    },
    "shared_system": {
        "display_name": "Shared System",
        "description": "Shared system rules used across all analysis stages.",
        "content": SHARED_SYSTEM_PROMPT_TEMPLATE,
    },
    "json_repair": {
        "display_name": "JSON Repair",
        "description": "Repairs invalid model output into schema-compliant JSON.",
        "content": JSON_REPAIR_PROMPT_TEMPLATE,
    },
}


def get_default_prompt_templates() -> list[dict[str, object]]:
    return [
        {
            "key": key,
            "display_name": value["display_name"],
            "description": value["description"],
            "content": value["content"],
            "is_enabled": True,
            "version": 1,
        }
        for key, value in PROMPT_TEMPLATE_REGISTRY.items()
    ]


def get_default_prompt_template(key: str) -> dict[str, object] | None:
    value = PROMPT_TEMPLATE_REGISTRY.get(key)
    if not value:
        return None
    return {
        "key": key,
        "display_name": value["display_name"],
        "description": value["description"],
        "content": value["content"],
        "is_enabled": True,
        "version": 1,
    }
