# POST_1_COMMENT_1

## PAYLOAD

```json
{
  "model": "gemma-4-31b-it",
  "messages": [
    {
      "role": "system",
      "content": "You are a social media investigation and risk analysis engine for open-source content review.\n\nMission:\n- Start from a seed account.\n- Identify likely organizational alignment, behavior pattern, and operational relevance.\n- Surface related actors, risky clusters, and plausible expansion candidates for follow-up research.\n- Produce concise analyst-ready outputs that can later support relationship graphs and branching investigation.\n\nYour task is to analyze posts, media items, captions, profile bios, prior posting patterns, comments, and graph summaries for indicators related to:\n- propaganda\n- symbolic affiliation\n- direct support expressions\n- leader/cadre praise\n- mobilization or calls to action\n- coordination signals\n- threats or target designation\n- organized crime indicators\n- violent extremist or criminal network alignment\n\nCore principles:\n1. Separate observation from inference.\n2. Do not make definitive claims of membership, guilt, or criminal responsibility.\n3. Use conservative reasoning.\n4. A single weak signal is not enough for a strong conclusion, but clear plain-language support/opposition in a comment should still be classified.\n5. Use \"unclear\" only when the communicative intent or evidence is genuinely indeterminate.\n6. Distinguish journalism, reporting, criticism, irony, mourning, and propaganda.\n7. Consider profile history and repeated behavior patterns only when they are explicitly provided.\n8. Prefer short, factual, reusable evidence phrases that can support later graph and follow-up analysis.\n9. Use institutional, cautious, non-accusatory language suitable for analyst review.\n10. Return only valid JSON matching the provided schema when a JSON schema is provided.\n11. Scores must be evidence-based and calibrated.\n\nDecision rules:\n- One alias match alone is never enough for medium or high confidence.\n- One symbol alone is never enough for a strong conclusion.\n- At least 2 independent signals are required for medium confidence.\n- At least 3 independent signals are required for high confidence.\n- If evidence conflicts, lower confidence.\n- If context suggests reporting or criticism, do not treat it as support unless there is clear endorsement.\n- For comments, decide the communicative stance first, then decide risk level.\n- For comments, if the text is plainly supportive, oppositional, neutral-small-talk, slogan-like, threatening, coordinating, insulting, or sharing actionable information, do not choose \"unclear\".\n- For organization assessment, treat aligned entities as ranked evidence. Put the strongest/primary entity first and weaker or secondary entities later.\n- Separate direct support, symbolic affinity, repeated narrative overlap, coordination signals, recruitment signals, and possible network-node behavior.\n"
    },
    {
      "role": "user",
      "content": "You are analyzing a single comment in the context of a social media post during a branching account investigation.\n\nPost owner investigation context:\n- Investigative focus entity: Daltons\n- Post owner suspected/aligned entities: Daltons\n- Post owner estimated role: unclear\n- Post owner organization link score: 2\n- Read the comment as a reaction to this specific account and post; if the wording praises or encourages the account/post, evaluate whether that praise supports this investigation context.\n\nPost being reacted to:\n- Post summary: Kullanıcıya ait kişisel bir fotoğraf paylaşılmıştır. Gönderi metni bulunmamaktadır.\n- Post categories: kisisel_gunluk\n- Post threat level: yok\n- Post caption: -\n\nPost analysis summary:\n{\"ozet\":\"Kullanıcıya ait kişisel bir fotoğraf paylaşılmıştır. Gönderi metni bulunmamaktadır.\",\"gorsel_analiz\":{\"sahne_tanimi\":\"\",\"konum_tahmini\":\"belirsiz\",\"kisi_sayisi\":0,\"silah_patlayici_var_mi\":false,\"bayrak_sembol_amblam\":\"belirsiz\",\"uniforma_kiyafet\":\"belirsiz\"},\"icerik_tonu\":\"neutral\",\"icerik_kategorisi\":[\"kisisel_gunluk\"],\"orgut_baglantisi\":{\"tespit_edilen_orgut\":\"Daltons\",\"baglanti_gostergesi\":\"Daltons\",\"muhtemel_rol\":\"belirsiz\"},\"tehdit_degerlendirmesi\":{\"tehdit_seviyesi\":\"yok\",\"eylem_plani_imasi\":false,\"gelecek_eylem_detay\":\"\",\"tehdit_veya_itiraf\":\"\",\"hedef_belirtilmis_mi\":false,\"hedef_detay\":\"\"},\"suc_unsuru\":{\"suc_var_mi\":false,\"suc_turleri\":[],\"aciklama\":\"\"},\"onem_skoru\":2,\"analist_notu\":\"Gönderi sadece bir portre fotoğrafından ibarettir. Risk analizi için profil biyografisindeki 'Bedeli Ölüm Olsa Bile Kimseye Boyun Eğmeyin' ifadesi not edilse de, bu gönderi özelinde bir bulgu yoktur.\"}\n\nPost context:\n- Post owner / Yorum sahibi bağlamı: huseyinulukoylu54\n- Bio: 🥷Bedeli Ölüm Olsa Bile Kimseye Boyun Eğmeyin ✍️\nB.C.G 🇷🇺🇪🇺\n- Caption: -\n\nComment:\n- Comment owner / Yorum sahibi: bunyaminkacak65\n- Comment text: 🔥\n- Öncelikli inceleme odağı: Daltons\nBu odağa göre destek, aidiyet, koordinasyon veya karşıtlık sinyallerini özellikle kontrol et.\n\n\nBU YORUMCUNUN ÖNCEKİ YORUMLARI:\n[1] Gönderi: \"Aynı gönderideki diğer yorum\" | Yorum: \"🔥\" | Verdict: ayni_post_diger_yorum | Bağlantı skoru: 0\nToplam: 1 yorum | ayni_post_diger_yorum: 1 | Bayraklı geçmiş: Hayır | Ort. bağlantı skoru: 0\nÖnceki davranış örüntüsünü dikkate alarak bu yeni yorumu değerlendir.\n\nTask:\nAnalyze the comment together with the post context.\nCheck for support signals, slogan repetition, coordination, information leakage, threats, hate speech, and repeated behavior.\n\nRules:\n- First decide the comment's communicative stance. Only then score risk.\n- Use this stance order mentally before final JSON output: support, slogan, opposition, neutral, threat, coordination, information_sharing, insult, unclear.\n- Treat the post owner and post itself as the primary context. The commenter is reacting to this exact post/account, not to an abstract topic.\n- If the investigation context explicitly states that the post owner is being assessed around a specific organization or suspicious alignment, use that as direct context for interpreting praise, celebration, encouragement, solidarity, or slogan language aimed at this post/account.\n- If this account/post is already being investigated around [[FOCUS_ENTITY]] or another suspicious alignment, then clearly approving, praising, celebrating, glorifying, saluting, or encouraging the post/account is evidence of sympathy or alignment toward that focus context unless the wording clearly shows mockery, criticism, or unrelated casual chatter.\n- Use \"support\" for plain supportive or approving comments, even if the support is short, emotional, low-detail, or socially phrased.\n- Use \"slogan\" for repetitive chant/slogan style support language.\n- Use \"neutral\" for off-topic, generic, conversational, or low-signal comments that are still understandable.\n- Use \"opposition\" for clearly critical, rejecting, or condemning comments.\n- Use \"unclear\" only if the text is truly too ambiguous, too fragmentary, or too context-dependent to classify.\n- If the text clearly praises, blesses, salutes, congratulates, encourages, stands in solidarity, repeats approval language, or uses martyrdom praise, do not choose \"unclear\".\n- If the comment is mainly approving emoji or symbolic praise under a suspicious/supportive post, classify it as support rather than unclear whenever that reading is more plausible than neutrality.\n- If the text repeats a movement slogan or celebratory ideological phrase, prefer \"slogan\" or \"support\", not \"unclear\".\n- If the text is understandable everyday conversation, a greeting, or simple side-commentary, prefer \"neutral\" over \"unclear\".\n- If the text is understandable but weakly supportive, prefer \"support\" with low confidence over \"unclear\".\n- Set `flags.active_supporter.flag=true` only for explicit, affirmative, endorsing, or repeated support that suggests the commenter is more than casually approving; otherwise it may remain false even when `comment_type=\"support\"`.\n- If `comment_type` is support or slogan in a suspicious focus context, `organization_link_score` should usually not remain 0.\n- Do not make definitive claims of criminal responsibility.\n- If prior history contains repeated signals, mention that in the reasoning.\n- Keep `content_summary_tr` concrete and short; quote or paraphrase the core stance.\n- Tie the reasoning to the actual wording of the comment, not vague speculation.\n- Return only one JSON object.\n- Do not use markdown code fences.\n\nSchema:\n{\n  \"comment_type\": \"support|opposition|neutral|slogan|coordination|threat|insult|information_sharing|unclear\",\n  \"content_summary_tr\": \"string\",\n  \"flags\": {\n    \"active_supporter\": {\"flag\": false, \"reason_tr\": \"string\"},\n    \"threat\": {\"flag\": false, \"reason_tr\": \"string\"},\n    \"information_leak\": {\"flag\": false, \"reason_tr\": \"string\"},\n    \"coordination\": {\"flag\": false, \"reason_tr\": \"string\"},\n    \"hate_speech\": {\"flag\": false, \"reason_tr\": \"string\"}\n  },\n  \"organization_link_assessment\": {\n    \"organization_link_score\": 0,\n    \"confidence\": \"low|medium|high\",\n    \"reason_tr\": \"string\"\n  },\n  \"behavior_pattern\": {\n    \"consistent_with_history\": \"yes|no|unclear\",\n    \"repeated_support_language\": \"yes|no|unclear\",\n    \"reason_tr\": \"string\"\n  },\n  \"overall_risk\": {\n    \"level\": \"low|medium|high|critical\",\n    \"human_review_required\": \"yes|no\"\n  }\n}"
    }
  ],
  "max_tokens": 180,
  "stream": false
}
```

## RAW_RESPONSE

```json
{
  "id": "chatcmpl-ac3a8b76ab97dfab",
  "object": "chat.completion",
  "created": 1775908680,
  "model": "gemma-4-31b-it",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "{\n  \"comment_type\": \"support\",\n  \"content_summary_tr\": \"Ateş emojisi ile gönderiye destek verilmiş.\",\n  \"flags\": {\n    \"active_supporter\": {\n      \"flag\": false,\n      \"reason_tr\": \"Tek bir emoji kullanımı düşük yoğunluklu bir destek göstergesidir; aktif destekleyici olduğunu kanıtlamak için yetersizdir.\"\n    },\n    \"threat\": {\n      \"flag\": false,\n      \"reason_tr\": \"Herhangi bir tehdit unsuru bulunmamaktadır.\"\n    },\n    \"information_leak\": {\n      \"flag\": false,\n      \"reason_tr\": \"Bilgi sızdırma tespit edilmemiştir.\"\n    },\n    \"coordination\": {\n      \"flag\": false,",
        "refusal": null,
        "annotations": null,
        "audio": null,
        "function_call": null,
        "tool_calls": [],
        "reasoning": null
      },
      "logprobs": null,
      "finish_reason": "length",
      "stop_reason": null,
      "token_ids": null
    }
  ],
  "service_tier": null,
  "system_fingerprint": null,
  "usage": {
    "prompt_tokens": 2302,
    "total_tokens": 2482,
    "completion_tokens": 180,
    "prompt_tokens_details": null
  },
  "prompt_logprobs": null,
  "prompt_token_ids": null,
  "kv_transfer_params": null
}
```

## Quick Notes

- Yorum bağlamı `Daltons` odağıyla gönderiliyor.
- Yorumcu geçmişi prompta eklenmiş.
- İlk cevap kesilmiş; `finish_reason` değeri `length`.
- Bu yüzden asıl kullanılabilir sonuç trace'te repair veya parsed result bloklarında devam ediyor.
