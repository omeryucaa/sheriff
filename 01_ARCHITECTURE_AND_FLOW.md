# Architecture and Flow

## 1. Objective

Build a production-grade social media intelligence analysis system for posts from Instagram, X/Twitter, TikTok, and similar platforms.

The system analyzes:
- profile metadata
- bio
- caption/post text
- one or more media items
- prior post summaries / prior behavior history
- comments and commenter history

The system detects structured indicators related to:
- propaganda
- symbolic affiliation
- direct support expressions
- leader/cadre praise
- mobilization / calls to action
- coordination
- threats / target designation
- organized crime indicators
- repeated aligned narratives
- escalation over time

## 2. Core Principles

### 2.1 Layer separation
Keep these layers separate:
1. direct observation
2. content classification
3. risk indicators
4. organization/network alignment assessment
5. profile role estimation
6. review priority scoring
7. human-review escalation reason

### 2.2 Conservative inference
- One weak signal is never enough for a strong conclusion.
- One alias match alone is never enough for medium or high confidence.
- One symbol, slogan, color, or gesture alone is never enough for a strong conclusion.
- If evidence is insufficient, output `unclear`.

### 2.3 Evidence-first reasoning
Every material conclusion should include short evidence items.

### 2.4 Distinguish support from reporting
The system must distinguish:
- support / endorsement
- journalism / reporting
- criticism / opposition
- irony / satire
- mourning / commemoration
- neutral resharing

## 3. Pipeline Overview

Implement a multi-stage pipeline:

1. Input normalization
2. Media-level observation extraction
3. Post-level integrated analysis
4. Comment-level analysis
5. Rule-based normalization and score correction
6. Account/case aggregation across posts
7. Human-review escalation

## 4. Stage Details

### 4.1 Input normalization
Normalize raw platform inputs into a common internal model.

Example fields:
- platform
- username
- platformUsername
- bio
- postId
- caption
- media items
- prior post summaries
- profile context
- comments

### 4.2 Media observation stage
Each media item is analyzed independently.
This stage extracts only visible or clearly audible facts.

Outputs include:
- media type
- scene summary
- setting type
- visible person count
- face visibility
- clothing types
- notable objects
- weapon presence
- visible symbols or logos
- visible text items
- activity type
- crowd level
- audio elements
- child presence
- institution markers
- vehicles
- license or signage
- raw observation note

### 4.3 Post analysis stage
This stage combines:
- profile info
- bio
- caption
- all media observations
- prior summaries
- optional profile block

Outputs include:
- content types
- primary theme
- language and tone
- risk indicators
- organization assessment
- profile role estimate
- behavior pattern
- review priority
- analyst note

### 4.4 Comment analysis stage
Each comment is analyzed in the context of the post analysis.

Outputs include:
- comment type
- content summary
- flags
- organization link assessment
- behavior pattern
- overall risk

### 4.5 Rule-based backend stage
After model output, apply deterministic rules:
- alias normalization
- evidence deduplication
- confidence clamping
- contradiction checks
- human review thresholds

### 4.6 Aggregation stage
Aggregate repeated signals across time:
- repeated themes
- repeated entities
- escalating behavior
- recurring coordination signals
- recurring support language
- role trend

## 5. Data Flow

### 5.1 End-to-end flow
1. Collect raw post payload
2. Normalize to internal input model
3. Run media prompt per media item
4. Validate each media JSON
5. Build compact media summary block
6. Build compact prior-history block
7. Run post prompt
8. Validate post JSON
9. Apply post-processing rules
10. For each comment, run comment prompt
11. Validate comment JSON
12. Apply comment post-processing
13. Aggregate account-level trends
14. Trigger human review if thresholds are met

## 6. Compact Prompt Rendering Strategy

### 6.1 Media block rendering
Do not dump full raw JSON into the post prompt.
Render a compact summary block.

Example:

```text
MEDIA SUMMARY:
1. image: small crowd, banners visible, slogan text partially legible, no clear weapon
2. video: chanting present, outdoor street scene, flag-like symbol visible, no clear institution marker
3. image: portrait poster, memorial wording visible
```

### 6.2 History block rendering
Summarize prior history instead of dumping raw past outputs.

Example:

```text
PRIOR HISTORY SUMMARY:
- Prior post 1: commemorative tone, recurring symbolism, low-medium risk
- Prior post 2: repeated narrative alignment, no direct threat
- Prior post 3: mobilizing language stronger than previous posts
```

## 7. Human Review Escalation

Human review should be required if any of the following is true:
- organization_link_score >= 7
- importance_score >= 8
- threat flag true
- coordination flag true
- information leak flag true
- violence praise present
- repeated escalation detected

## 8. Critical Priority Examples

Examples of critical priority:
- clear threat plus target indication
- coordination details for action or gathering
- fundraising/resource mobilization
- doxxing or operational information exposure
- repeated strong propaganda pattern with explicit support language

## 9. Validation and Reliability

Every model output must be schema-validated.
If invalid:
1. retry once with a repair prompt
2. if still invalid, mark the case as failed and store raw output

Validation rules:
- enums must match exactly
- score ranges must be enforced
- required fields must exist
- extra keys may be stripped

## 10. Non-Goals

Do not implement:
- definitive criminal classification
- arrest/deconfliction logic
- automated punitive action
- unsupervised high-risk enforcement
- high-confidence identity attribution from weak social signals

This is an intelligence triage and review-priority system, not a judicial truth engine.
