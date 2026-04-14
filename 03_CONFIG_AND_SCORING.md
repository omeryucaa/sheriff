# Config and Scoring

## 1. Organization Groups

Use a simple alias configuration that can be extended later.

```ts
export const ORG_GROUPS = {
  terror: [
    { name: "PKK", aliases: ["PKK", "KCK", "HPG", "YPG", "PYD", "TAK"] },
    { name: "DHKP-C", aliases: ["DHKP-C", "Dev-Sol"] },
    { name: "FETÖ", aliases: ["FETÖ", "PDY"] },
    { name: "DEAŞ", aliases: ["DEAŞ", "IŞİD", "ISIS", "ISIL"] },
    { name: "MLKP", aliases: ["MLKP"] },
    { name: "TİKKO/TKP-ML", aliases: ["TİKKO", "TKP-ML"] },
    { name: "El-Kaide", aliases: ["El-Kaide", "Al-Qaeda", "Al Qaida"] },
    { name: "Hizbullah", aliases: ["Hizbullah"] }
  ],
  organized_crime: [
    { name: "Daltons", aliases: ["Daltons", "Daltonlar"] },
    { name: "Sarallar", aliases: ["Sarallar"] },
    { name: "Cübbeliler", aliases: ["Cübbeliler"] },
    { name: "Gündoğmuşlar", aliases: ["Gündoğmuşlar"] },
    { name: "Anucurlar", aliases: ["Anucurlar"] },
    { name: "RedKitler", aliases: ["RedKitler", "Red Kitler"] }
  ]
} as const;
```

## 2. Matching Rules

```ts
export const MATCHING_RULES = {
  singleMatchNotEnough: true,
  allowUnclear: true,
  minIndependentSignalsForMediumConfidence: 2,
  minIndependentSignalsForHighConfidence: 3
} as const;
```

## 3. Profile Roles

```ts
export const PROFILE_ROLES = [
  "supporter",
  "propaganda_distributor",
  "sympathizer",
  "news_sharer",
  "event_participant",
  "possible_organizer",
  "possible_network_node",
  "unclear"
] as const;
```

Role guidance:
- supporter = explicit supportive language
- propaganda_distributor = repeated dissemination of aligned content
- sympathizer = weak but recurring affinity signals
- news_sharer = mostly reporting/resharing behavior
- event_participant = visible participation or repeated event-linked sharing
- possible_organizer = time/place/action/attendance coordination signals
- possible_network_node = repeated interaction patterns suggesting linkage
- unclear = insufficient evidence

## 4. organization_link_score

Measures how strongly the content appears aligned with or supportive of a known entity/network.

- 0 = no signal
- 1–2 = very weak or ambiguous signal
- 3–4 = weak symbolic or textual alignment
- 5–6 = multiple consistent signals or repeated sympathetic framing
- 7–8 = direct support, leader praise, propaganda pattern, repeated aligned narrative
- 9 = coordination, mobilization, fundraising, target designation, strong repeated multi-signal evidence
- 10 = multiple strong and explicit evidence clusters; still requires human review

## 5. importance_score

Measures operational review priority, not guilt.

- 1–2 = low-value, neutral, personal
- 3–4 = mild relevance
- 5–6 = repeated symbolic/propaganda relevance
- 7–8 = mobilizing, threatening, or network-signaling content
- 9 = likely coordination, target designation, critical information exposure
- 10 = urgent human review priority

## 6. Confidence Rules

Allowed values:
- low
- medium
- high

Meaning:
- low = one weak signal or ambiguous context
- medium = at least 2 independent signals
- high = at least 3 independent signals strongly aligned in same direction

If signals conflict, confidence must decrease.

## 7. Human Review Rules

```ts
export const HUMAN_REVIEW_RULES = {
  organizationLinkScoreGte: 7,
  importanceScoreGte: 8,
  threatFlag: true,
  coordinationFlag: true,
  informationLeakFlag: true,
  violencePraisePresent: true
} as const;
```

If any threshold is met, force:
- `human_review_required = "yes"`

## 8. Alias Normalization Utility

```ts
export function normalizeEntityName(text: string, orgGroups: typeof ORG_GROUPS): string | null {
  const source = text.toLowerCase();

  for (const groupList of Object.values(orgGroups)) {
    for (const org of groupList) {
      for (const alias of org.aliases) {
        const a = alias.toLowerCase();
        if (source.includes(a)) {
          return org.name;
        }
      }
    }
  }

  return null;
}
```

Improve later with:
- regex boundaries
- token matching
- false-positive suppression

## 9. Post-Processing Rules

After model output:
- normalize aligned entity names
- deduplicate repeated evidence strings
- clamp confidence if only one weak signal exists
- downgrade role if coordination evidence is absent
- downgrade when signals conflict
- force human review when thresholds are met
