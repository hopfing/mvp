"""Tests for the decoupled event mapper."""

from datetime import date, datetime

import polars as pl
import pytest
from pathlib import Path
from unittest.mock import patch

from mvp.common.event_mapper import (
    MappingResult,
    build_match_catalog,
    build_player_lookup,
    map_book_events,
    _strip_circuit_prefix,
    _match_by_date,
    _match_tournament,
    _parse_book_round,
    _tournament_agrees,
)


# ---------------------------------------------------------------------------
# Player name lookup
# ---------------------------------------------------------------------------

class TestBuildPlayerLookup:
    def test_loads_bio_names(self, tmp_path):
        """Bio first_name + last_name are the baseline lookup."""
        bio = pl.DataFrame({
            "player_id": ["A001", "B002"],
            "first_name": ["Roger", "Rafael"],
            "last_name": ["Federer", "Nadal"],
        })
        bio_path = tmp_path / "stage" / "atptour" / "players.parquet"
        bio_path.parent.mkdir(parents=True)
        bio.write_parquet(bio_path)

        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup()

        assert lookup["roger federer"] == "A001"
        assert lookup["rafael nadal"] == "B002"

    def test_aliases_override_bio(self, tmp_path):
        """Per-book aliases take priority over bio names."""
        bio = pl.DataFrame({
            "player_id": ["A001"],
            "first_name": ["Roger"],
            "last_name": ["Federer"],
        })
        bio_path = tmp_path / "stage" / "atptour" / "players.parquet"
        bio_path.parent.mkdir(parents=True)
        bio.write_parquet(bio_path)

        aliases_path = tmp_path / "aliases.yaml"
        aliases_path.write_text('\"R. Federer\": \"A001\"\n')

        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup(aliases_path=aliases_path)

        assert lookup["r. federer"] == "A001"
        assert lookup["roger federer"] == "A001"

    def test_accent_normalization(self, tmp_path):
        """Accented names are normalized for matching."""
        bio = pl.DataFrame({
            "player_id": ["C003"],
            "first_name": ["Jiří"],
            "last_name": ["Lehečka"],
        })
        bio_path = tmp_path / "stage" / "atptour" / "players.parquet"
        bio_path.parent.mkdir(parents=True)
        bio.write_parquet(bio_path)

        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup()

        assert lookup["jiri lehecka"] == "C003"

    def test_empty_bio_graceful(self, tmp_path):
        """No bio file produces empty lookup without error."""
        with patch("mvp.common.event_mapper.get_data_root", return_value=tmp_path):
            lookup = build_player_lookup()

        assert lookup == {}


# ---------------------------------------------------------------------------
# Match catalog
# ---------------------------------------------------------------------------

class TestBuildMatchCatalog:
    def _matches_df(self):
        return pl.DataFrame({
            "match_uid": ["m1", "m2", "m3"],
            "player_id": ["A001", "A001", "B002"],
            "opp_id": ["B002", "C003", "C003"],
            "tournament_id": ["403", "403", "580"],
            "year": [2026, 2026, 2026],
            "tournament_name": ["Miami Open", "Miami Open", "Australian Open"],
        })

    def test_indexes_by_player_pair(self):
        catalog = build_match_catalog(self._matches_df())
        pair_ab = frozenset({"A001", "B002"})
        assert pair_ab in catalog
        assert len(catalog[pair_ab]) == 1
        assert catalog[pair_ab][0]["match_uid"] == "m1"

    def test_deduplicates_by_match_uid(self):
        """Same match_uid from both perspectives should produce one entry."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m1"],
            "player_id": ["A001", "B002"],
            "opp_id": ["B002", "A001"],
            "tournament_id": ["403", "403"],
            "year": [2026, 2026],
        })
        catalog = build_match_catalog(df)
        pair = frozenset({"A001", "B002"})
        assert len(catalog[pair]) == 1

    def test_multiple_matches_same_pair(self):
        """Same pair in different tournaments produces multiple entries."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "B002"],
            "tournament_id": ["403", "580"],
            "year": [2026, 2026],
            "tournament_name": ["Miami", "Indian Wells"],
        })
        catalog = build_match_catalog(df)
        pair = frozenset({"A001", "B002"})
        assert len(catalog[pair]) == 2

    def test_missing_columns_raises(self):
        df = pl.DataFrame({"match_uid": ["m1"], "player_id": ["A001"]})
        with pytest.raises(ValueError, match="missing required columns"):
            build_match_catalog(df)

    def test_collision_warning_round_robin(self, caplog):
        """Same pair, same tournament+year logs a warning."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "B002"],
            "tournament_id": ["605", "605"],
            "year": [2026, 2026],
        })
        with caplog.at_level("WARNING"):
            build_match_catalog(df)
        assert "collision" in caplog.text.lower()

    def test_excludes_completed_matches(self):
        """Matches with a non-null result_type are excluded from the catalog."""
        df = pl.DataFrame({
            "match_uid": ["m_done", "m_ret", "m_wo", "m_open"],
            "player_id": ["A001", "A001", "A001", "A001"],
            "opp_id": ["B002", "B002", "B002", "B002"],
            "tournament_id": ["403", "580", "605", "1536"],
            "year": [2026, 2026, 2026, 2026],
            "result_type": ["completed", "retirement", "walkover", None],
        })
        catalog = build_match_catalog(df)
        pair = frozenset({"A001", "B002"})
        assert len(catalog[pair]) == 1
        assert catalog[pair][0]["match_uid"] == "m_open"

    def test_carries_round(self):
        """Round is included in catalog entries when available."""
        df = pl.DataFrame({
            "match_uid": ["m1", "m2"],
            "player_id": ["A001", "A001"],
            "opp_id": ["B002", "B002"],
            "tournament_id": ["1536", "1536"],
            "year": [2026, 2026],
            "round": ["Q2", "R128"],
        })
        catalog = build_match_catalog(df)
        rounds = {e["round"] for e in catalog[frozenset({"A001", "B002"})]}
        assert rounds == {"Q2", "R128"}


# ---------------------------------------------------------------------------
# Tournament name matching
# ---------------------------------------------------------------------------

class TestMatchByDate:
    """Date fallback for pairs that met twice in a season.

    Tournament-name matching only narrows when the book's naming aligns with ours.
    For oddspapi it never does ('French Open Men Singles' against our Roland Garros
    naming), so the string comparison silently no-ops and every repeat meeting
    lands as a collision — 224 of them before this existed.
    """

    def _candidates(self):
        return [
            {"match_uid": "early", "effective_match_date": date(2026, 5, 29)},
            {"match_uid": "late", "effective_match_date": date(2026, 7, 3)},
        ]

    def test_picks_the_nearer_match(self):
        got = _match_by_date(datetime(2026, 7, 3, 15, 0), self._candidates())
        assert got["match_uid"] == "late"

    def test_picks_the_other_one_from_the_other_date(self):
        got = _match_by_date(datetime(2026, 5, 29, 9, 0), self._candidates())
        assert got["match_uid"] == "early"

    def test_refuses_when_two_are_equally_close(self):
        """Better a collision than a coin flip on which match a bet belongs to."""
        cands = [
            {"match_uid": "a", "effective_match_date": date(2026, 6, 1)},
            {"match_uid": "b", "effective_match_date": date(2026, 6, 2)},
        ]
        assert _match_by_date(datetime(2026, 6, 1, 12, 0), cands) is None

    def test_refuses_when_nothing_is_close_enough(self):
        assert _match_by_date(datetime(2026, 9, 1), self._candidates()) is None

    def test_accepts_a_date_object_as_well_as_datetime(self):
        got = _match_by_date(date(2026, 7, 3), self._candidates())
        assert got["match_uid"] == "late"

    def test_no_dates_in_catalog_yields_none(self):
        """Backward compatibility: callers that never supplied dates are unaffected."""
        cands = [{"match_uid": "a"}, {"match_uid": "b"}]
        assert _match_by_date(datetime(2026, 6, 1), cands) is None

    def test_no_fetched_at_yields_none(self):
        assert _match_by_date(None, self._candidates()) is None

    def test_partial_dates_still_resolve(self):
        cands = [
            {"match_uid": "dated", "effective_match_date": date(2026, 7, 3)},
            {"match_uid": "undated"},
        ]
        got = _match_by_date(datetime(2026, 7, 3), cands)
        assert got["match_uid"] == "dated"


class TestTournamentConstraint:
    """Tournament identity, where the caller can resolve it.

    `_match_tournament` compares strings, and for oddspapi zero of 184 tournament
    names match one of ours, so it no-ops and leaves every meeting of the pair in
    play. A caller that maps its own tournament text to our ids passes them instead.
    """

    def _odds(self, tournament_ids, when=datetime(2026, 5, 8, 12, 0)):
        return pl.DataFrame({
            "oap_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP Madrid, Spain Men Singles"] * 2,
            "fetched_at": [when, when],
            "tournament_ids": [tournament_ids, tournament_ids],
        })

    def _lookup(self):
        return {"roger federer": "A001", "rafael nadal": "B002"}

    def _catalog(self):
        """The same pair met twice in 2026, at different tournaments."""
        return {
            frozenset({"A001", "B002"}): [
                {"match_uid": "madrid", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "p1_id": "A001",
                 "effective_match_date": date(2026, 5, 8)},
                {"match_uid": "rome", "tournament_id": "416", "year": 2026,
                 "tournament_name": "Rome 2", "p1_id": "A001",
                 "effective_match_date": date(2026, 5, 15)},
            ],
        }

    def test_pins_the_meeting_at_that_tournament(self):
        result = map_book_events(
            self._odds(["1536"]), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(),
        )
        assert [em.match_uid for em in result.event_matches] == ["madrid"]

    def test_picks_the_other_tournament_when_told_so(self):
        """Same date on the row — only the tournament differs, so this fails if the
        date is still doing the work."""
        result = map_book_events(
            self._odds(["416"]), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(),
        )
        assert [em.match_uid for em in result.event_matches] == ["rome"]

    def test_no_match_in_that_tournament_is_rejected(self):
        result = map_book_events(
            self._odds(["9999"]), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(),
        )
        assert result.event_matches == []
        assert result.tournament_rejected == [("e1", "Roger Federer", "Rafael Nadal")]

    def test_a_narrowed_set_still_lets_the_date_decide(self):
        """A city hosting two same-circuit events narrows to both; the date then
        chooses between two candidates rather than every meeting ever."""
        result = map_book_events(
            self._odds(["1536", "416"]), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(),
        )
        assert [em.match_uid for em in result.event_matches] == ["madrid"]

    def test_absent_column_is_a_no_op(self):
        """The live scrapers pass no tournament_ids; behaviour must be unchanged."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Madrid"] * 2,
        })
        result = map_book_events(
            odds, "dk_event_id", "dk", self._lookup(), self._catalog(),
        )
        assert result.tournament_rejected == []
        # And it still resolves the way it always did — 'ATP - Madrid' strips to
        # 'Madrid', which substring-matches our 'Madrid 1'. That the scrapers'
        # strings align with ours is exactly why they never needed tournament ids.
        assert [em.match_uid for em in result.event_matches] == ["madrid"]


class TestSeasonIsAConstraintNotJustATiebreak:
    """A single candidate used to be accepted regardless of which season it was in.

    The year filter only narrows when there are two or more candidates, and keeps
    them all when none matches the year. So a pair that met once, in a prior season,
    resolved to that match unchecked: 33 of 8,946 oddspapi fixtures took 2026 prices
    onto 2018-2025 matches, which in a backtest settles a bet against a different
    match entirely.

    The constraint is the SEASON, with the date only as the New Year escape hatch.
    Gating on the date itself rejected 210 fixtures rather than 33, because
    `effective_match_date` is estimated for rounds whose real date we lack — most of
    the false rejections were same-season qualifying matches 3 days off.
    """

    def _odds(self, when):
        return pl.DataFrame({
            "oap_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["Wimbledon Men Singles", "Wimbledon Men Singles"],
            "fetched_at": [when, when],
        })

    def _lookup(self):
        return {"roger federer": "A001", "rafael nadal": "B002"}

    def _catalog(self, match_date):
        return {
            frozenset({"A001", "B002"}): [
                {"match_uid": "2024_540_SGL_F_A001_B002", "tournament_id": "540",
                 "year": 2024, "tournament_name": "Wimbledon",
                 "effective_match_date": match_date, "p1_id": "A001"},
            ],
        }

    def test_lone_candidate_from_another_season_is_rejected(self):
        result = map_book_events(
            self._odds(datetime(2026, 7, 19, 12, 0)), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(date(2024, 7, 14)),
        )
        assert result.event_matches == []
        assert len(result.date_rejected) == 1
        eid, a, b, muid, gap = result.date_rejected[0]
        assert (eid, muid) == ("e1", "2024_540_SGL_F_A001_B002")
        assert gap > 700

    def test_lone_candidate_on_the_right_date_is_kept(self):
        result = map_book_events(
            self._odds(datetime(2024, 7, 14, 12, 0)), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(date(2024, 7, 14)),
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []

    def test_tolerance_spans_a_new_year_rollover(self):
        """A tournament running across New Year has a prior-season `year` while
        being days away — a year-equality check would wrongly reject it."""
        result = map_book_events(
            self._odds(datetime(2026, 1, 1, 3, 0)), "oap_event_id", "oddspapi",
            self._lookup(), self._catalog(date(2025, 12, 31)),
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []

    def _undated(self, year):
        return {
            frozenset({"A001", "B002"}): [
                {"match_uid": f"{year}_540_SGL_F_A001_B002", "tournament_id": "540",
                 "year": year, "tournament_name": "Wimbledon", "p1_id": "A001"},
            ],
        }

    def test_undated_candidate_in_another_season_is_rejected(self):
        """No date means no escape hatch, and a wrong season is the defect itself."""
        result = map_book_events(
            self._odds(datetime(2026, 7, 19, 12, 0)), "oap_event_id", "oddspapi",
            self._lookup(), self._undated(2024),
        )
        assert result.event_matches == []
        assert result.date_rejected[0][4] == -1     # gap unknown

    def test_undated_candidate_in_the_same_season_is_kept(self):
        """Most of the catalog predates the date column; absence is not evidence."""
        result = map_book_events(
            self._odds(datetime(2026, 7, 19, 12, 0)), "oap_event_id", "oddspapi",
            self._lookup(), self._undated(2026),
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []

    def test_same_season_estimated_date_is_not_rejected(self):
        """`effective_match_date` is estimated for rounds whose real date we lack.
        Gating on the date rejected 210 fixtures instead of 33 — mostly same-season
        qualifiers sitting 3 days off their estimate."""
        result = map_book_events(
            self._odds(datetime(2026, 7, 19, 12, 0)), "oap_event_id", "oddspapi",
            self._lookup(),
            {frozenset({"A001", "B002"}): [
                {"match_uid": "2026_301_SGL_Q1_A001_B002", "tournament_id": "301",
                 "year": 2026, "tournament_name": "Wimbledon", "p1_id": "A001",
                 "effective_match_date": date(2026, 7, 16)},
            ]},
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []

    def test_the_live_path_cannot_fire_this(self):
        """The scrapers pass no date column, so `fetched_at` is None and the guard
        is unreachable for them. event_mapper is shared with the live pipeline."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Wimbledon", "ATP - Wimbledon"],
        })
        result = map_book_events(
            odds, "dk_event_id", "dk", self._lookup(), self._catalog(date(2024, 7, 14)),
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []


class TestTournamentNameGate:
    """A March Sao Paulo event took the pair's only open match — a July Liberec QF.

    Every discriminating signal the mapper had was wired as a tiebreak among
    candidates: the year filter and `_match_tournament` both only run when there
    are two or more. With one candidate the pair WAS the key, and the tournament
    text sitting on the row was never read. `_match_tournament` could not have
    saved it either — it returns every candidate when none matches, so a total
    disagreement is indistinguishable from a naming convention that never aligns.
    """

    def _odds(self, tournament, event_id_col="br_event_id"):
        return pl.DataFrame({
            event_id_col: ["e1", "e1"],
            "player_name": ["Juan Bautista Torres", "Gonzalo Bueno"],
            "tournament": [tournament, tournament],
        })

    def _lookup(self):
        return {"juan bautista torres": "T0DM", "gonzalo bueno": "B0GR"}

    def _only_liberec(self):
        """The Sao Paulo meeting is completed, so it is not in the catalog at all
        — `build_match_catalog` excludes finished matches. The pair's only open
        match is the one four months away."""
        return {
            frozenset({"T0DM", "B0GR"}): [
                {"match_uid": "2026_6795_SGL_QF_B0GR_T0DM", "tournament_id": "6795",
                 "year": 2026, "tournament_name": "Liberec", "round": "QF",
                 "p1_id": "T0DM"},
            ],
        }

    @pytest.mark.parametrize("tournament", [
        "São Paulo",                               # br
        "Challenger - Sao Paulo",                  # dk
        "ATP Challenger Sao Paulo (BRA) - Clay",   # mgm
    ])
    def test_stale_event_no_longer_takes_the_only_open_match(self, tournament):
        result = map_book_events(
            self._odds(tournament), "br_event_id", "br",
            self._lookup(), self._only_liberec(),
        )
        assert result.event_matches == []
        assert result.tournament_rejected == [
            ("e1", "Juan Bautista Torres", "Gonzalo Bueno")
        ]

    def test_the_real_liberec_event_still_maps(self):
        """The gate must not cost us the event it was protecting."""
        result = map_book_events(
            self._odds("ATP Challenger Liberec (CZE) - Clay"), "br_event_id", "br",
            self._lookup(), self._only_liberec(),
        )
        assert [em.match_uid for em in result.event_matches] == [
            "2026_6795_SGL_QF_B0GR_T0DM"
        ]

    @pytest.mark.parametrize("book_text,our_name", [
        ("French Open", "Roland Garros"),          # alias
        ("French Open Paris - Men", "Roland Garros"),
        ("ATP Madrid 2026", "Madrid 1"),           # season year + our variant index
        ("ATP Masters Rome (ITA) - Clay", "Rome 2"),
        ("Shymkent Challenger 2026", "Shymkent 2"),  # circuit word as a suffix
        ("ATP Quals. - Rome", "Rome 2"),
        ("ATP Rome - Round 1", "Rome 2"),          # round token mid-string
        ("Queens Semifinals", "London"),           # alias + round token
        ("Challenger de Roma - ClasificaciÃ³n", "Rome 1"),   # mojibaked es. qualifying
        ("ATP Marrakesh (MAR) - Clay", "Marrakech"),
    ])
    def test_names_that_diverge_but_agree(self, book_text, our_name):
        """Each of these was a false rejection before the normalization was
        extended. Measured over the full event map, the gate now keeps 98.7% of
        healthy mappings while rejecting 84% of the stale ones."""
        assert _tournament_agrees(book_text, {"tournament_name": our_name})

    @pytest.mark.parametrize("book_text,our_name", [
        ("ATP Masters Miami (USA) - Hard", "Washington"),
        ("ATP Challenger Busan (KOR) - Hard", "Wuxi"),
        ("ATP Challenger Quito (ECU) - Clay", "Bogota"),
        ("São Paulo", "Oeiras 4"),
    ])
    def test_names_that_plainly_disagree(self, book_text, our_name):
        assert not _tournament_agrees(book_text, {"tournament_name": our_name})

    @pytest.mark.parametrize("book_text,our_name", [
        ("Pau", "Sao Paulo"),          # "pau" sits inside "sao paulo"
        ("Pau", "São Paulo"),
        ("ATP Challenger Sao Paulo (BRA) - Clay", "Pau"),
        ("Bol", "M15 Bologna"),
    ])
    def test_a_name_nested_inside_another_is_not_agreement(self, book_text, our_name):
        """Containment is over whole tokens. A raw substring test reads Pau as
        agreeing with Sao Paulo, and there are 381 such nesting pairs among the
        tournaments seen since 2024 — enough to reintroduce the original bug in a
        new disguise."""
        assert not _tournament_agrees(book_text, {"tournament_name": our_name})

    def test_a_genuine_token_prefix_still_agrees(self):
        """The token rule must not cost us the real prefix relationships it was
        chosen to keep."""
        assert _tournament_agrees(
            "ATP Hertogenbosch (NED) - Grass", {"tournament_name": "'s-Hertogenbosch"}
        )

    @pytest.mark.parametrize("book_text", ["", "   ", None])
    def test_illegible_book_text_rejects_nothing(self, book_text):
        """A book with no usable tournament string must be left exactly as it was,
        not have every one of its events rejected."""
        assert _tournament_agrees(book_text, {"tournament_name": "Liberec"})

    def test_missing_catalog_name_rejects_nothing(self):
        assert _tournament_agrees("ATP Challenger Liberec (CZE) - Clay", {})

    def test_variant_index_is_the_tiebreaks_job_not_the_gates(self):
        """The gate deliberately accepts both Madrid 1 and Madrid 2 — choosing
        between them is `_match_tournament`'s, and collapsing that into the gate
        would reject whichever the book did not spell out."""
        for ours in ("Madrid 1", "Madrid 2"):
            assert _tournament_agrees("ATP - Madrid", {"tournament_name": ours})

    def test_caller_supplied_ids_win_over_our_strings(self):
        """oddspapi resolves tournament identity itself, and none of its 184
        tournament names match ours. Second-guessing a resolved id with a string
        comparison would reject every one of its events."""
        odds = pl.DataFrame({
            "oap_event_id": ["e1", "e1"],
            "player_name": ["Juan Bautista Torres", "Gonzalo Bueno"],
            "tournament": ["Some Name We Never Match"] * 2,
            "fetched_at": [datetime(2026, 7, 30, 12, 0)] * 2,
            "tournament_ids": [["6795"], ["6795"]],
        })
        result = map_book_events(
            odds, "oap_event_id", "oddspapi", self._lookup(), self._only_liberec(),
        )
        assert [em.match_uid for em in result.event_matches] == [
            "2026_6795_SGL_QF_B0GR_T0DM"
        ]
        assert result.tournament_rejected == []


class TestStalenessBackstop:
    """The season constraint cannot see a same-season stale event: a March event
    and a July match share a year. `max_date_gap_days` is the backstop, and it is
    opt-in because a caller whose dates are not comparable must be unaffected.
    """

    def _odds(self, when):
        return pl.DataFrame({
            "br_event_id": ["e1", "e1"],
            "player_name": ["Juan Bautista Torres", "Gonzalo Bueno"],
            "tournament": ["ATP Challenger Liberec (CZE) - Clay"] * 2,
            "fetched_at": [when, when],
        })

    def _lookup(self):
        return {"juan bautista torres": "T0DM", "gonzalo bueno": "B0GR"}

    def _catalog(self):
        return {
            frozenset({"T0DM", "B0GR"}): [
                {"match_uid": "2026_6795_SGL_QF_B0GR_T0DM", "tournament_id": "6795",
                 "year": 2026, "tournament_name": "Liberec", "p1_id": "T0DM",
                 "effective_match_date": date(2026, 7, 31)},
            ],
        }

    def test_same_season_stale_event_is_rejected(self):
        """Same tournament name, same year, four months dead — the case the name
        gate cannot catch on its own."""
        result = map_book_events(
            self._odds(datetime(2026, 3, 24, 16, 0)), "br_event_id", "br",
            self._lookup(), self._catalog(), max_date_gap_days=14,
        )
        assert result.event_matches == []
        assert len(result.date_rejected) == 1
        assert result.date_rejected[0][4] == 129

    def test_a_live_event_days_out_is_kept(self):
        result = map_book_events(
            self._odds(datetime(2026, 7, 29, 16, 0)), "br_event_id", "br",
            self._lookup(), self._catalog(), max_date_gap_days=14,
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []

    def test_tolerance_is_loose_enough_for_an_estimated_date(self):
        """`effective_match_date` is estimated for rounds whose real date we lack,
        so the backstop must not double as a disambiguator."""
        result = map_book_events(
            self._odds(datetime(2026, 7, 22, 16, 0)), "br_event_id", "br",
            self._lookup(), self._catalog(), max_date_gap_days=14,
        )
        assert len(result.event_matches) == 1

    def test_opt_in_default_leaves_existing_callers_alone(self):
        result = map_book_events(
            self._odds(datetime(2026, 3, 24, 16, 0)), "br_event_id", "br",
            self._lookup(), self._catalog(),
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []

    def test_inert_without_a_catalog_date(self):
        """Most of the catalog predates the date column; absence is not evidence."""
        undated = {
            frozenset({"T0DM", "B0GR"}): [
                {"match_uid": "2026_6795_SGL_QF_B0GR_T0DM", "tournament_id": "6795",
                 "year": 2026, "tournament_name": "Liberec", "p1_id": "T0DM"},
            ],
        }
        result = map_book_events(
            self._odds(datetime(2026, 3, 24, 16, 0)), "br_event_id", "br",
            self._lookup(), undated, max_date_gap_days=14,
        )
        assert len(result.event_matches) == 1
        assert result.date_rejected == []


class TestCatalogCarriesDate:
    def _df(self, with_date: bool):
        data = {
            "match_uid": ["m1"],
            "player_id": ["A001"],
            "opp_id": ["B002"],
            "tournament_id": ["403"],
            "year": [2026],
        }
        if with_date:
            data["effective_match_date"] = [date(2026, 5, 29)]
        return pl.DataFrame(data)

    def test_date_is_carried_when_present(self):
        catalog = build_match_catalog(self._df(with_date=True))
        entry = catalog[frozenset({"A001", "B002"})][0]
        assert entry["effective_match_date"] == date(2026, 5, 29)

    def test_absent_date_column_is_not_required(self):
        catalog = build_match_catalog(self._df(with_date=False))
        entry = catalog[frozenset({"A001", "B002"})][0]
        assert "effective_match_date" not in entry


class TestStripCircuitPrefix:
    def test_atp_prefix(self):
        assert _strip_circuit_prefix("ATP - Miami") == "Miami"

    def test_challenger_prefix(self):
        assert _strip_circuit_prefix("Challenger - Phoenix") == "Phoenix"

    def test_challenger_quals(self):
        assert _strip_circuit_prefix("Challenger Quals. - Murcia") == "Murcia"

    def test_no_prefix(self):
        assert _strip_circuit_prefix("Miami Open") == "Miami Open"


class TestParseBookRound:
    def test_main_draw_numbered_round(self):
        assert _parse_book_round("ATP Madrid - Round 1") == "main"
        assert _parse_book_round("ATP Madrid - Round 2") == "main"

    def test_main_draw_final_variants(self):
        assert _parse_book_round("ATP Houston - Final") == "main"
        assert _parse_book_round("ATP Houston - Semifinales") == "main"
        assert _parse_book_round("Challenger Mexico City - 1/4 Final") == "main"

    def test_qualifier_forms(self):
        assert _parse_book_round("ATP Challenger Busan - Qualification - Hard") == "qual"
        assert _parse_book_round("Challenger Quals. - Madrid") == "qual"
        assert _parse_book_round("ATP Madrid - Qualifying") == "qual"

    def test_no_round_signal(self):
        assert _parse_book_round("ATP - Madrid") is None
        assert _parse_book_round("ATP Madrid 2026") is None
        assert _parse_book_round("Madrid") is None
        assert _parse_book_round("") is None


class TestMatchTournament:
    def test_narrows_by_name(self):
        candidates = [
            {"match_uid": "m1", "tournament_id": "403", "tournament_name": "Miami Open"},
            {"match_uid": "m2", "tournament_id": "580", "tournament_name": "Indian Wells"},
        ]
        result = _match_tournament("ATP - Miami", candidates)
        assert len(result) == 1
        assert result[0]["match_uid"] == "m1"

    def test_no_match_returns_all(self):
        candidates = [
            {"match_uid": "m1", "tournament_id": "403", "tournament_name": "Miami Open"},
        ]
        result = _match_tournament("ATP - Paris", candidates)
        assert len(result) == 1  # falls back to all candidates

    def test_empty_tournament_returns_all(self):
        candidates = [{"match_uid": "m1", "tournament_name": "Miami"}]
        result = _match_tournament("", candidates)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Core mapping logic
# ---------------------------------------------------------------------------

class TestMapBookEvents:
    def _staged_odds(self):
        return pl.DataFrame({
            "dk_event_id": ["e1", "e1", "e2", "e2", "e3", "e3"],
            "player_name": [
                "Roger Federer", "Rafael Nadal",
                "Novak Djokovic", "Andy Murray",
                "Unknown Player", "Roger Federer",
            ],
            "tournament": [
                "ATP - Miami", "ATP - Miami",
                "ATP - Miami", "ATP - Miami",
                "ATP - Miami", "ATP - Miami",
            ],
        })

    def _player_lookup(self):
        return {
            "roger federer": "A001",
            "rafael nadal": "B002",
            "novak djokovic": "C003",
            "andy murray": "D004",
        }

    def _match_catalog(self):
        return {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
            ],
            frozenset({"C003", "D004"}): [
                {"match_uid": "m2", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
            ],
        }

    def test_maps_known_events(self):
        result = map_book_events(
            self._staged_odds(), "dk_event_id", "dk",
            self._player_lookup(), self._match_catalog(),
        )
        assert len(result.event_matches) == 2
        uids = {em.match_uid for em in result.event_matches}
        assert uids == {"m1", "m2"}

    def test_unresolved_names_tracked(self):
        result = map_book_events(
            self._staged_odds(), "dk_event_id", "dk",
            self._player_lookup(), self._match_catalog(),
        )
        assert "Unknown Player" in result.unresolved_names

    def test_skips_existing_events(self):
        result = map_book_events(
            self._staged_odds(), "dk_event_id", "dk",
            self._player_lookup(), self._match_catalog(),
            existing_event_ids={"e1"},
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "m2"

    def test_no_match_tracked(self):
        """Both names resolve but no match in catalog."""
        odds = pl.DataFrame({
            "dk_event_id": ["e99", "e99"],
            "player_name": ["Roger Federer", "Novak Djokovic"],
            "tournament": ["ATP - Miami", "ATP - Miami"],
        })
        catalog = {}  # empty catalog
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.no_match_found) == 1
        assert result.no_match_found[0][0] == "e99"

    def test_ambiguous_match_collision(self):
        """Multiple candidates that can't be disambiguated.

        Both carry the tournament name the book gave, so the name gate passes
        them through and the ambiguity is real: with no dates to separate them,
        neither can be chosen.
        """
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Miami Open", "ATP - Miami Open"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
                {"match_uid": "m2", "tournament_id": "580", "year": 2026,
                 "tournament_name": "Miami Open"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 0
        assert len(result.collisions) == 1

    def test_tournament_matching_none_of_the_candidates_is_rejected(self):
        """Previously a collision: with no candidate in the book's tournament,
        "we cannot tell which" was the wrong diagnosis — the answer is neither."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Unknown", "ATP - Unknown"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
                {"match_uid": "m2", "tournament_id": "580", "year": 2026,
                 "tournament_name": "Indian Wells"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 0
        assert result.collisions == []
        assert result.tournament_rejected == [("e1", "Roger Federer", "Rafael Nadal")]

    def test_disambiguation_by_tournament(self):
        """Multiple candidates narrowed to one by tournament name."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Miami", "ATP - Miami"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "403", "year": 2026,
                 "tournament_name": "Miami Open"},
                {"match_uid": "m2", "tournament_id": "580", "year": 2026,
                 "tournament_name": "Indian Wells Masters"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "m1"

    def test_round_gate_rejects_single_qualifier_when_book_main(self):
        """Single candidate must be rejected if book says main draw but catalog has Q*."""
        odds = pl.DataFrame({
            "b365_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP Madrid - Round 1", "ATP Madrid - Round 1"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "q_match", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "Q2"},
            ],
        }
        result = map_book_events(
            odds, "b365_event_id", "b365",
            self._player_lookup(), catalog,
        )
        assert result.event_matches == []
        assert len(result.no_match_found) == 1

    def test_round_gate_resolves_quals_vs_main_ambiguity(self):
        """Same-tournament Q2 + R128 pair — book 'Round 1' picks R128."""
        odds = pl.DataFrame({
            "b365_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP Madrid - Round 1", "ATP Madrid - Round 1"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "q_match", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "Q2"},
                {"match_uid": "r128_match", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "R128"},
            ],
        }
        result = map_book_events(
            odds, "b365_event_id", "b365",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "r128_match"

    def test_round_gate_is_noop_when_book_has_no_round_signal(self):
        """DK-style tournament text without round info leaves all candidates in play."""
        odds = pl.DataFrame({
            "dk_event_id": ["e1", "e1"],
            "player_name": ["Roger Federer", "Rafael Nadal"],
            "tournament": ["ATP - Madrid", "ATP - Madrid"],
        })
        catalog = {
            frozenset({"A001", "B002"}): [
                {"match_uid": "m1", "tournament_id": "1536", "year": 2026,
                 "tournament_name": "Madrid 1", "round": "R128"},
            ],
        }
        result = map_book_events(
            odds, "dk_event_id", "dk",
            self._player_lookup(), catalog,
        )
        assert len(result.event_matches) == 1
        assert result.event_matches[0].match_uid == "m1"
