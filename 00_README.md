# Social Intelligence Analysis System Spec

This package contains a production-oriented specification for a social media intelligence analysis system focused on structured risk analysis of posts, media, captions, profile context, prior behavior summaries, and comments.

## Files

- `01_ARCHITECTURE_AND_FLOW.md` — full system architecture, pipeline stages, data flow, module design, validation rules, and escalation logic
- `02_PROMPTS.md` — production prompt set for media analysis, post analysis, comment analysis, and shared system rules
- `03_CONFIG_AND_SCORING.md` — organization alias config, scoring rules, confidence logic, human review thresholds, and normalization rules
- `04_IMPLEMENTATION_REQUIREMENTS.md` — engineering requirements, folder layout, contracts, validation, retry strategy, and acceptance criteria

## Recommended Usage With Codex

Give Codex these files together.

Suggested order:
1. `01_ARCHITECTURE_AND_FLOW.md`
2. `02_PROMPTS.md`
3. `03_CONFIG_AND_SCORING.md`
4. `04_IMPLEMENTATION_REQUIREMENTS.md`

Tell Codex to:
- read all files first
- produce an implementation plan
- create the folder structure
- implement strongly typed models and validators
- wire prompt builders and pipeline services
- add deterministic post-processing and tests

## Design Goals

The system must:
- separate observation from inference
- use conservative scoring
- avoid definitive criminal claims based only on social content
- produce schema-valid JSON outputs
- support auditing and human review

## Non-Goals

The system is not a judicial truth engine.
It must not:
- assert definitive criminal guilt
- assert definitive membership from weak signals
- automate punitive action without human review
