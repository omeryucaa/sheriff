from app.prompts import (
    build_account_final_summary_prompt,
    build_comment_analysis_prompt,
    build_followup_candidate_analysis_prompt,
    build_graph_analysis_prompt,
    build_parent_post_analysis_prompt,
    build_post_analysis_prompt,
    get_default_prompt_template,
    get_default_prompt_templates,
)
def test_comment_prompt_explicitly_reduces_false_unclear_classification() -> None:
    prompt = build_comment_analysis_prompt(
        post_analysis="Post summary",
        username="seed",
        bio="bio",
        caption="caption",
        commenter_username="yorumcu",
        comment_text="Helal olsun",
        commenter_history_context="",
    )

    assert "already under investigation" in prompt
    assert "a low non-zero score is usually appropriate" in prompt
    assert "Lack of strong evidence for formal organizational connection does not require score 0." in prompt
    assert "Use score 0 only when the comment is neutral, unrelated, oppositional" in prompt
    assert "flags.active_supporter.flag should normally be true" in prompt
    assert "do not describe sentiment as neutral" in prompt
    assert "importance_score 2 or higher" in prompt
    assert "1-3 = weak but real support, praise, approval, or symbolic support" in prompt
    assert '"sentiment": "positive|negative|neutral"' in prompt
    assert '"review": {' in prompt
    assert "Cikti dili zorunlu: Turkce" in prompt

def test_post_prompts_emphasize_ranked_entities_and_network_roles() -> None:
    post_prompt = build_post_analysis_prompt(
        username="seed",
        instagram_username="seed.ig",
        bio="bio",
        caption="caption",
        post_history_context="",
        account_profile_summary="",
        media_context="tek medya",
    )
    parent_prompt = build_parent_post_analysis_prompt(
        username="seed",
        instagram_username="seed.ig",
        bio="bio",
        caption="caption",
        media_count=2,
        single_media_analyses=[
            {"media_index": 1, "media_type": "image", "ozet": "poster", "icerik_kategorisi": ["propaganda"], "tehdit_seviyesi": "orta", "orgut": "PKK/KCK", "analist_notu": "not"},
            {"media_index": 2, "media_type": "image", "ozet": "kalabalik", "icerik_kategorisi": ["yuruyus_gosteri"], "tehdit_seviyesi": "dusuk", "orgut": "belirsiz", "analist_notu": "not"},
        ],
    )

    assert "primary organization alignment" in post_prompt
    assert "secondary or weaker organization links" in post_prompt
    assert "supporter, propagandist, amplifier" in post_prompt
    assert "rank entities from strongest to weakest evidence" in parent_prompt
    assert "possible network-node behavior" in parent_prompt


def test_account_final_summary_prompt_is_investigation_ready() -> None:
    prompt = build_account_final_summary_prompt(
        username="seed",
        instagram_username="seed.ig",
        bio="bio",
        post_history_summaries=[
            {
                "tarih": "2026-04-09",
                "ozet": "Orgut sembolleri iceren paylasim",
                "icerik_kategorisi": ["propaganda"],
                "tehdit_seviyesi": "orta",
                "orgut": "PKK/KCK",
                "onem_skoru": 7,
            }
        ],
        history_stats_context="Toplam gönderi: 1",
    )

    assert "branching investigation" in prompt
    assert "supporter, propagandist, amplifier, event hub, or possible network node" in prompt
    assert "primary organization alignment and secondary links" in prompt


def test_account_final_summary_prompt_includes_full_post_history_by_default() -> None:
    history = [
        {
            "tarih": f"2026-04-{day:02d}",
            "source_kind": "story" if day == 1 else "post",
            "ozet": f"post-{day}",
            "icerik_kategorisi": ["propaganda"],
            "tehdit_seviyesi": "orta",
            "orgut": "PKK/KCK",
            "onem_skoru": 5,
        }
        for day in range(1, 15)
    ]
    prompt = build_account_final_summary_prompt(
        username="seed",
        instagram_username="seed.ig",
        bio="bio",
        post_history_summaries=history,
        history_stats_context="Toplam gönderi: 14",
    )

    assert "post-1" in prompt
    assert "post-14" in prompt
    assert "Tür: story" in prompt


def test_graph_analysis_prompt_requests_clusters_risks_and_followup_targets() -> None:
    prompt = build_graph_analysis_prompt(
        instagram_username="seed.ig",
        bio="bio",
        account_profile_summary="hesap özeti",
        graph_summary="graf özeti",
    )

    assert "strongest clusters around the seed account" in prompt
    assert "most connected or highest-risk related actors" in prompt
    assert "recommended follow-up targets" in prompt
    assert "presentation-ready language" in prompt


def test_followup_candidate_prompt_is_registered_and_renderable() -> None:
    keys = {item["key"] for item in get_default_prompt_templates()}
    assert "followup_candidate_analysis" in keys
    template = get_default_prompt_template("followup_candidate_analysis")
    assert template is not None
    assert "new branch target" in template["content"]

    prompt = build_followup_candidate_analysis_prompt(
        username="seed",
        instagram_username="seed.ig",
        candidate_username="target_1",
        seed_account_summary="Seed hesap ozeti",
        relationship_evidence="Aynı sloganları tekrar ediyor.",
        interaction_snippets="Helal olsun, yanınızdayız",
        graph_tie_summary="3 kez yorum, 2 ortak düğüm",
        focus_entity="PKK/KCK",
    )

    assert "Candidate username: target_1" in prompt
    assert '"relationship_to_seed": "supporter|peer|amplifier|possible_operator|unclear"' in prompt
    assert "branch_recommended" in prompt
    assert "PKK/KCK" in prompt
