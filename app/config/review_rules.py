HUMAN_REVIEW_THRESHOLDS = {
    "organization_link_score_gte": 7,
    "importance_score_gte": 8,
    "threat": True,
    "coordination": True,
    "information_leak": True,
    "violence_praise": True,
    "escalation": True,
}

# Commenters with organization-link score at or above this threshold are added to review queue
# even if the raw `bayrak` flag is false.
COMMENT_REVIEW_QUEUE_MIN_SCORE = 5
