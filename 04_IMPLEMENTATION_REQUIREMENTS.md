# Implementation Requirements

## 1. Expected Deliverables

Codex should generate:
1. full architecture plan
2. folder structure
3. TypeScript types
4. prompt files
5. schema validators
6. normalization utilities
7. scoring guards
8. pipeline orchestration
9. test scaffolding
10. example fixtures for media/post/comment flows

## 2. Recommended Project Layout

```text
src/
  config/
    org-groups.ts
    scoring.ts
    review-rules.ts

  prompts/
    system-prompt.ts
    media-prompt.ts
    post-prompt.ts
    comment-prompt.ts

  types/
    input.ts
    media.ts
    post.ts
    comment.ts
    aggregate.ts

  services/
    llm-client.ts
    media-analysis-service.ts
    post-analysis-service.ts
    comment-analysis-service.ts
    aggregation-service.ts
    normalization-service.ts
    review-service.ts

  utils/
    alias-normalizer.ts
    json-parse.ts
    validation.ts
    evidence.ts
    scoring-guards.ts

  schemas/
    media.schema.ts
    post.schema.ts
    comment.schema.ts

  pipeline/
    run-media-stage.ts
    run-post-stage.ts
    run-comment-stage.ts
    run-case-aggregation.ts

  tests/
    media-analysis.test.ts
    post-analysis.test.ts
    comment-analysis.test.ts
    normalization.test.ts
    aggregation.test.ts
```

## 3. Validation Requirements

Every model output must be schema-validated.
If invalid:
1. retry once with a repair prompt
2. if still invalid, mark case as failed and store raw output

Validation rules:
- enums must match exactly
- score ranges must be enforced
- required fields must exist
- extra keys may be stripped

## 4. Retry Strategy

Implement:
- first pass
- JSON repair pass
- final fallback error state

The system should retain raw LLM output for debugging when validation fails.

## 5. Pipeline Behavior

### 5.1 Media stage
For each media item:
- prepare prompt
- send to model
- validate JSON
- store structured observation

### 5.2 Post stage
- convert media results into compact structured text block
- inject history summaries
- send post prompt
- validate JSON
- post-process

### 5.3 Comment stage
For each comment:
- inject compact post summary
- inject comment history if available
- send prompt
- validate JSON
- post-process

### 5.4 Aggregation stage
- combine structured post/comment results for account
- compute repeated patterns
- produce account-level review object

## 6. Account Aggregation Model

```ts
interface AccountAggregate {
  accountId: string;
  dominantThemes: string[];
  repeatedEntities: string[];
  repeatedRiskIndicators: string[];
  roleTrend: string;
  averageOrganizationLinkScore: number;
  maxImportanceScore: number;
  escalationDetected: boolean;
  humanReviewRecommended: boolean;
}
```

## 7. Escalation Heuristic

Mark escalation if:
- recent posts have higher average scores than older posts
- support language becomes more direct over time
- coordination indicators appear after earlier symbolic-only content
- threat language appears after lower-risk content

## 8. Engineering Priorities

The implementation must prioritize:
1. correctness
2. explainability
3. conservative risk estimation
4. JSON reliability
5. maintainability

## 9. Acceptance Criteria

The project is acceptable if it provides:
- typed models for all stages
- prompt builders for media, post, and comment analysis
- schema validation for all LLM outputs
- deterministic post-processing rules
- compact prompt rendering for history and media
- account aggregation support
- human review escalation logic
- test coverage for core normalization and scoring behavior

## 10. Suggested Instruction To Codex

Use the files in this package as the source of truth.
Read all files before planning.
Then:
- produce a modular implementation plan
- create the folder structure
- define types and schemas first
- implement prompt builders
- implement services and post-processing
- add tests and sample fixtures

Focus on conservative scoring, strict JSON validation, and auditable outputs.
