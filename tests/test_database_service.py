from pathlib import Path
import sqlite3

from app.database_service import DatabaseService


def test_database_service_persists_person_post_comments(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    result = svc.persist_post_and_comments(
        person_name="Yiğit",
        instagram_username="yigit.ig",
        profile_photo_url="https://cdn.example/p.jpg",
        bio="bio text",
        media_type="video",
        media_url="https://cdn.example/v.mp4",
        media_items=[{"media_type": "video", "media_url": "https://cdn.example/v.mp4"}],
        caption="caption",
        post_analysis='{"ozet":"analysis"}',
        structured_analysis={
            "ozet": "analysis",
            "icerik_kategorisi": ["propaganda"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "orta"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "PKK/KCK"},
            "onem_skoru": 7,
        },
        model="gemma-4-31b-it",
        comment_analyses=[
            {
                "commenter_username": "u1",
                "text": "yorum 1",
                "verdict": "destekci_aktif",
                "sentiment": "positive",
                "orgut_baglanti_skoru": 8,
                "bayrak": True,
                "reason": "destekliyor",
            },
            {
                "commenter_username": "u2",
                "text": "yorum 2",
                "verdict": "alakasiz",
                "sentiment": "neutral",
                "orgut_baglanti_skoru": 0,
                "bayrak": False,
                "reason": "alakasız",
            },
        ],
    )

    assert result.person_id > 0
    assert result.instagram_account_id > 0
    assert result.post_id > 0
    assert len(result.comment_ids) == 2

    conn = sqlite3.connect(str(db_path))
    queue_rows = conn.execute(
        "SELECT commenter_username, trigger_count, flag_reason_type FROM review_queue"
    ).fetchall()
    assert queue_rows == [("u1", 1, "destekci_aktif")]
    post_rows = conn.execute(
        "SELECT icerik_kategorisi, tehdit_seviyesi, onem_skoru, orgut_baglantisi FROM instagram_posts"
    ).fetchall()
    assert post_rows == [("propaganda", "orta", 7, "PKK/KCK")]
    conn.close()


def test_database_service_upserts_account_for_same_person(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    r1 = svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url="https://a",
        bio="b1",
        media_type="image",
        media_url="https://i1",
        media_items=[{"media_type": "image", "media_url": "https://i1"}],
        caption="c1",
        post_analysis='{"ozet":"p1"}',
        structured_analysis={"ozet": "p1", "icerik_kategorisi": ["belirsiz"], "onem_skoru": 1},
        model="gemma-4-31b-it",
        comment_analyses=[],
    )
    r2 = svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url="https://b",
        bio="b2",
        media_type="image",
        media_url="https://i2",
        media_items=[{"media_type": "image", "media_url": "https://i2"}],
        caption="c2",
        post_analysis='{"ozet":"p2"}',
        structured_analysis={"ozet": "p2", "icerik_kategorisi": ["belirsiz"], "onem_skoru": 2},
        model="gemma-4-31b-it",
        comment_analyses=[],
    )

    assert r1.person_id == r2.person_id
    assert r1.instagram_account_id == r2.instagram_account_id
    assert r1.post_id != r2.post_id


def test_upsert_post_and_comment_idempotency(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    person_id, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )
    assert person_id > 0
    assert account_id > 0

    p1 = svc.upsert_post(
        instagram_account_id=account_id,
        media_type="image",
        media_url="https://img",
        media_items=[{"media_type": "image", "media_url": "https://img"}],
        caption="c1",
        post_analysis='{"ozet":"a1"}',
        structured_analysis={"ozet": "a1", "icerik_kategorisi": ["propaganda"], "onem_skoru": 4},
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="post1",
        source_post_url="https://ig/p/post1",
    )
    p2 = svc.upsert_post(
        instagram_account_id=account_id,
        media_type="image",
        media_url="https://img2",
        media_items=[
            {"media_type": "image", "media_url": "https://img2"},
            {"media_type": "video", "media_url": "https://vid2"},
        ],
        caption="c2",
        post_analysis='{"ozet":"a2"}',
        structured_analysis={"ozet": "a2", "icerik_kategorisi": ["propaganda"], "onem_skoru": 5},
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="post1",
        source_post_url="https://ig/p/post1",
    )
    assert p1.created is True
    assert p2.created is False
    assert p1.post_id == p2.post_id

    c1 = svc.upsert_comment(
        instagram_post_id=p1.post_id,
        commenter_username="u1",
        commenter_profile_url="https://ig/u1",
        comment_text="text",
        verdict="destekci_aktif",
        sentiment="positive",
        orgut_baglanti_skoru=9,
        bayrak=True,
        reason="ok",
        discovered_at="2026-04-08T10:59:51+00:00",
        source_run_id="run1",
        source_post_id="post1",
        source_post_url="https://ig/p/post1",
    )
    c2 = svc.upsert_comment(
        instagram_post_id=p1.post_id,
        commenter_username="u1",
        commenter_profile_url="https://ig/u1",
        comment_text="text",
        verdict="destekci_aktif",
        sentiment="positive",
        orgut_baglanti_skoru=9,
        bayrak=True,
        reason="ok",
        discovered_at="2026-04-08T10:59:51+00:00",
        source_run_id="run1",
        source_post_id="post1",
        source_post_url="https://ig/p/post1",
    )
    assert c1.created is True
    assert c2.created is False
    assert c1.comment_id == c2.comment_id


def test_history_queries_return_structured_context(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    result = svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
        media_type="image",
        media_url="https://img",
        media_items=[{"media_type": "image", "media_url": "https://img"}],
        caption="c1",
        post_analysis='{"ozet":"dag kadraji"}',
        structured_analysis={
            "ozet": "dag kadraji",
            "icerik_kategorisi": ["askeri_operasyon"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "yuksek"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "PKK/KCK"},
            "onem_skoru": 8,
        },
        model="gemma-4-31b-it",
        comment_analyses=[
            {
                "commenter_username": "u1",
                "text": "haziriz",
                "verdict": "koordinasyon",
                "sentiment": "negative",
                "orgut_baglanti_skoru": 9,
                "bayrak": True,
                "reason": "eylem koordinasyonu",
            }
        ],
    )

    post_history = svc.get_post_history_summaries(result.instagram_account_id)
    commenter_history = svc.get_commenter_history("u1")

    assert post_history[0]["ozet"] == "dag kadraji"
    assert post_history[0]["tehdit_seviyesi"] == "yuksek"
    assert commenter_history[0]["post_ozet"] == "dag kadraji"
    assert commenter_history[0]["bayrak"] is True


def test_post_history_includes_story_highlight_and_orders_by_source_created_at(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    _, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )

    def _structured(summary: str) -> dict[str, object]:
        return {
            "ozet": summary,
            "icerik_kategorisi": ["propaganda"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "orta"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "belirsiz"},
            "onem_skoru": 5,
        }

    svc.upsert_post(
        instagram_account_id=account_id,
        source_kind="post",
        media_type="image",
        media_url="https://img/p",
        media_items=[{"media_type": "image", "media_url": "https://img/p"}],
        caption="p",
        post_analysis="post",
        structured_analysis=_structured("post"),
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="post:1",
        source_created_at="2026-01-03T00:00:00+00:00",
    )
    svc.upsert_post(
        instagram_account_id=account_id,
        source_kind="story",
        media_type="image",
        media_url="https://img/s",
        media_items=[{"media_type": "image", "media_url": "https://img/s"}],
        caption=None,
        post_analysis="story",
        structured_analysis=_structured("story"),
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="story:1",
        source_created_at="2026-01-01T00:00:00+00:00",
    )
    svc.upsert_post(
        instagram_account_id=account_id,
        source_kind="highlight",
        media_type="image",
        media_url="https://img/h",
        media_items=[{"media_type": "image", "media_url": "https://img/h"}],
        caption="Highlight: test",
        post_analysis="highlight",
        structured_analysis=_structured("highlight"),
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="highlight:h1:i1",
        source_created_at="2026-01-02T00:00:00+00:00",
    )

    history = svc.get_post_history_summaries(account_id)

    assert [item["source_kind"] for item in history] == ["story", "highlight", "post"]
    assert [item["ozet"] for item in history] == ["story", "highlight", "post"]


def test_account_profile_summary_is_stored_and_updated(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    _, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )

    assert svc.get_account_profile_summary(account_id) == ""
    svc.update_account_profile_summary(account_id, "Hesap agirlikli olarak propaganda icerikleri paylasiyor.")
    assert svc.get_account_profile_summary(account_id) == "Hesap agirlikli olarak propaganda icerikleri paylasiyor."


def test_account_graph_analysis_is_stored_and_updated(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    _, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )

    empty = svc.get_account_graph_analysis(account_id)
    assert empty["analysis"] == ""
    svc.update_account_graph_analysis(account_id, "Graf, propaganda ve destekci yorum yogunlugunu one cikariyor.", "gemma-4-31b-it")
    saved = svc.get_account_graph_analysis(account_id)
    assert saved["analysis"] == "Graf, propaganda ve destekci yorum yogunlugunu one cikariyor."
    assert saved["model"] == "gemma-4-31b-it"


def test_account_graph_capture_is_stored_and_updated(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    _, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )

    empty = svc.get_account_graph_capture(account_id)
    assert empty["path"] == ""
    svc.update_account_graph_capture(account_id, "/captures/account_1_graph.png")
    saved = svc.get_account_graph_capture(account_id)
    assert saved["path"] == "/captures/account_1_graph.png"


def test_prompt_templates_seed_and_update(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    prompts = svc.list_prompt_templates()
    assert any(item["key"] == "post_analysis" for item in prompts)

    updated = svc.update_prompt_template("post_analysis", "ozel prompt icerigi", True)
    assert updated is not None
    assert updated["content"] == "ozel prompt icerigi"
    assert svc.get_prompt_content("post_analysis") == "ozel prompt icerigi"

    reset = svc.reset_prompt_template("post_analysis")
    assert reset is not None
    assert "You are analyzing a complete social media post as evidence within a branching account investigation." in reset["content"]
    assert any(item["key"] == "media_deep_analysis" for item in prompts)


def test_save_media_observations_persists_deep_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    _, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )
    post = svc.upsert_post(
        instagram_account_id=account_id,
        source_kind="post",
        media_type="image",
        media_url="https://img/p",
        media_items=[{"media_type": "image", "media_url": "https://img/p"}],
        caption="p",
        post_analysis="post",
        structured_analysis={"ozet": "post"},
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="post:1",
        source_created_at="2026-01-03T00:00:00+00:00",
    )

    svc.save_media_observations(
        post.post_id,
        [
            {
                "media_index": 1,
                "media_type": "image",
                "scene_summary": "street",
                "deep_required": True,
                "deep_status": "completed",
                "deep_reason": "license_or_signage, model_hint",
                "location_confidence": "high",
                "contains_vehicle": True,
                "contains_plate": True,
                "deep_payload": {"location_assessment": {"location_confidence": "high"}},
            }
        ],
    )

    posts = svc.list_account_posts(account_id)
    assert len(posts) == 1
    observations = posts[0]["media_observations"]
    assert len(observations) == 1
    observation = observations[0]
    assert observation["deep_required"] is True
    assert observation["deep_status"] == "completed"
    assert observation["location_confidence"] == "high"
    assert observation["contains_vehicle"] is True
    assert observation["contains_plate"] is True
    assert isinstance(observation["deep_payload"], dict)


def test_dashboard_summary_and_graph(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    result = svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
        media_type="image",
        media_url="https://img",
        media_items=[{"media_type": "image", "media_url": "https://img"}],
        caption="c1",
        post_analysis='{"ozet":"dag kadraji"}',
        structured_analysis={
            "ozet": "dag kadraji",
            "icerik_kategorisi": ["askeri_operasyon", "propaganda"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "yuksek"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "PKK/KCK"},
            "onem_skoru": 8,
        },
        model="gemma-4-31b-it",
        comment_analyses=[
            {
                "commenter_username": "u1",
                "text": "haziriz",
                "verdict": "koordinasyon",
                "sentiment": "negative",
                "orgut_baglanti_skoru": 9,
                "bayrak": True,
                "reason": "eylem koordinasyonu",
            }
        ],
    )

    summary = svc.get_dashboard_summary()
    graph = svc.get_account_graph(result.instagram_account_id)

    assert summary["kpis"]["incelenen_hesap"] == 1
    assert summary["kpis"]["baskin_orgut"] == "PKK/KCK"
    assert any(node["type"] == "organization" for node in graph["nodes"])
    assert any(edge["type"] == "commented_on" for edge in graph["edges"])


def test_account_org_summary_updates_from_multiple_post_inferences(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    first = svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
        media_type="image",
        media_url="https://img1",
        media_items=[{"media_type": "image", "media_url": "https://img1"}],
        caption="c1",
        post_analysis='{"ozet":"p1"}',
        structured_analysis={
            "ozet": "p1",
            "icerik_kategorisi": ["propaganda"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "dusuk"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "PKK/KCK"},
            "onem_skoru": 5,
        },
        model="gemma-4-31b-it",
        comment_analyses=[],
    )
    svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
        media_type="image",
        media_url="https://img2",
        media_items=[{"media_type": "image", "media_url": "https://img2"}],
        caption="c2",
        post_analysis='{"ozet":"p2"}',
        structured_analysis={
            "ozet": "p2",
            "icerik_kategorisi": ["medya_kultur"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "orta"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "DHKP-C"},
            "onem_skoru": 6,
        },
        model="gemma-4-31b-it",
        comment_analyses=[],
    )
    svc.persist_post_and_comments(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
        media_type="image",
        media_url="https://img3",
        media_items=[{"media_type": "image", "media_url": "https://img3"}],
        caption="c3",
        post_analysis='{"ozet":"p3"}',
        structured_analysis={
            "ozet": "p3",
            "icerik_kategorisi": ["belirsiz"],
            "tehdit_degerlendirmesi": {"tehdit_seviyesi": "yok"},
            "orgut_baglantisi": {"tespit_edilen_orgut": "belirsiz"},
            "onem_skoru": 2,
        },
        model="gemma-4-31b-it",
        comment_analyses=[],
    )

    detail = svc.get_account_detail(first.instagram_account_id)
    accounts = svc.list_accounts()
    assert detail is not None
    assert detail["tespit_edilen_orgut"] == "DHKP-C, PKK/KCK"
    assert detail["tehdit_seviyesi"] == "orta"
    assert accounts[0]["tespit_edilen_orgut"] == "DHKP-C, PKK/KCK"


def test_list_account_posts_returns_media_items(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    svc = DatabaseService(str(db_path))
    svc.init_schema()

    _, account_id = svc.get_or_create_person_account(
        person_name="Ali",
        instagram_username="ali.ig",
        profile_photo_url=None,
        bio="bio",
    )
    svc.upsert_post(
        instagram_account_id=account_id,
        media_type="video",
        media_url="https://img/1",
        media_items=[
            {"media_type": "image", "media_url": "https://img/1"},
            {"media_type": "video", "media_url": "https://vid/2"},
        ],
        caption="caption",
        post_analysis="analysis",
        structured_analysis={"ozet": "analysis", "icerik_kategorisi": ["propaganda"], "onem_skoru": 3},
        model="gemma-4-31b-it",
        source_target_username="ali.ig",
        source_run_id="run1",
        source_post_id="post1",
        source_post_url="https://ig/p/post1",
    )

    posts = svc.list_account_posts(account_id)

    assert posts[0]["media_items"] == [
        {"media_type": "image", "media_url": "https://img/1"},
        {"media_type": "video", "media_url": "https://vid/2"},
    ]
