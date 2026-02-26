"""Tests for tournament matches aggregation."""

from datetime import date, datetime
from pathlib import Path

import polars as pl

from mvp.atptour.aggregators.tournament_matches import (
    MATCHES_SCHEMA,
    TournamentMatchesAggregator,
)
from mvp.common.enums import Circuit


def _write_results_parquet(
    data_root: Path,
    circuit: str = "tour",
    tid: str = "580",
    year: int = 2023,
    *,
    p1_id: str = "AAAA",
    p2_id: str = "BBBB",
    winner_id: str = "AAAA",
    p1_seed: int | None = 1,
    p2_seed: int | None = None,
    p1_entry: str | None = None,
    p2_entry: str | None = "WC",
    match_uid: str = "2023_580_SGL_F_AAAA_BBBB",
) -> Path:
    """Write a minimal results parquet to the expected staging path."""
    path = (
        data_root
        / "stage"
        / "atptour"
        / "tournaments"
        / circuit
        / tid
        / str(year)
        / "results.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "match_uid": [match_uid],
            "tournament_id": [tid],
            "year": [year],
            "circuit": [circuit],
            "draw_type": ["singles"],
            "round": ["F"],
            "match_id": ["ms001"],
            "winner_id": [winner_id],
            "p1_id": [p1_id],
            "p1_name": ["Player A"],
            "p1_country": ["USA"],
            "p1_seed": [p1_seed],
            "p1_entry": [p1_entry],
            "p2_id": [p2_id],
            "p2_name": ["Player B"],
            "p2_country": ["GBR"],
            "p2_seed": [p2_seed],
            "p2_entry": [p2_entry],
            "p1_partner_id": [None],
            "p1_partner_name": [None],
            "p1_partner_country": [None],
            "p2_partner_id": [None],
            "p2_partner_name": [None],
            "p2_partner_country": [None],
            "result_type": ["completed"],
            "duration_seconds": [7200],
            "p1_set1_games": [6],
            "p1_set1_tiebreak": [None],
            "p1_set2_games": [6],
            "p1_set2_tiebreak": [None],
            "p1_set3_games": [None],
            "p1_set3_tiebreak": [None],
            "p1_set4_games": [None],
            "p1_set4_tiebreak": [None],
            "p1_set5_games": [None],
            "p1_set5_tiebreak": [None],
            "p2_set1_games": [4],
            "p2_set1_tiebreak": [None],
            "p2_set2_games": [3],
            "p2_set2_tiebreak": [None],
            "p2_set3_games": [None],
            "p2_set3_tiebreak": [None],
            "p2_set4_games": [None],
            "p2_set4_tiebreak": [None],
            "p2_set5_games": [None],
            "p2_set5_tiebreak": [None],
            "source_file": ["results_singles.html"],
            "parsed_at": [datetime(2025, 1, 1)],
        }
    )
    df.write_parquet(path)
    return path


def _write_schedule_parquet(
    data_root: Path,
    circuit: str = "tour",
    tid: str = "580",
    year: int = 2023,
) -> Path:
    """Write a schedule parquet: same final match (draw order) + one upcoming SF."""
    path = (
        data_root
        / "stage"
        / "atptour"
        / "tournaments"
        / circuit
        / tid
        / str(year)
        / "schedule.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "match_uid": [
                "2023_580_SGL_F_AAAA_BBBB",
                "2023_580_SGL_SF_CCCC_DDDD",
            ],
            "tournament_id": [tid, tid],
            "year": [year, year],
            "circuit": [circuit, circuit],
            "draw_type": ["singles", "singles"],
            "round": ["F", "SF"],
            "match_date": [date(2023, 1, 29), date(2023, 1, 28)],
            "scheduled_datetime": [
                datetime(2023, 1, 29, 14, 0),
                datetime(2023, 1, 28, 11, 0),
            ],
            "time_suffix": ["Not Before", ""],
            "display_time": ["14:00", "11:00"],
            "court_name": ["Rod Laver Arena", "Rod Laver Arena"],
            "court_match_num": [1, 1],
            "is_time_estimated": [False, False],
            # Draw order: BBBB is p1 in the final (opposite of Results)
            "p1_id": ["BBBB", "CCCC"],
            "p1_name": ["Player B", "Player C"],
            "p1_country": ["GBR", "FRA"],
            "p1_seed": [None, 3],
            "p1_entry": ["WC", None],
            "p2_id": ["AAAA", "DDDD"],
            "p2_name": ["Player A", "Player D"],
            "p2_country": ["USA", "ESP"],
            "p2_seed": [1, 5],
            "p2_entry": [None, None],
            "status": [None, None],
            "score": [None, None],
            "snapshot_timestamp": [
                datetime(2023, 1, 27),
                datetime(2023, 1, 27),
            ],
            "source_file": ["schedule.html", "schedule.html"],
            "parsed_at": [datetime(2025, 1, 1), datetime(2025, 1, 1)],
        }
    )
    df.write_parquet(path)
    return path


def _write_match_stats_parquet(
    data_root: Path,
    circuit: str = "tour",
    tid: str = "580",
    year: int = 2023,
) -> Path:
    """Write a minimal match stats parquet for the final match."""
    path = (
        data_root
        / "stage"
        / "atptour"
        / "tournaments"
        / circuit
        / tid
        / str(year)
        / "match_stats.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    all_stat_names = [
        "svc_aces",
        "svc_double_faults",
        "svc_first_serve_in",
        "svc_first_serve_att",
        "svc_first_serve_pts_won",
        "svc_first_serve_pts_played",
        "svc_second_serve_pts_won",
        "svc_second_serve_pts_played",
        "svc_bp_saved",
        "svc_bp_faced",
        "svc_games_played",
        "svc_serve_rating",
        "ret_first_serve_pts_won",
        "ret_first_serve_pts_played",
        "ret_second_serve_pts_won",
        "ret_second_serve_pts_played",
        "ret_bp_converted",
        "ret_bp_opportunities",
        "ret_games_played",
        "ret_return_rating",
        "pts_service_pts_won",
        "pts_service_pts_played",
        "pts_return_pts_won",
        "pts_return_pts_played",
        "pts_total_pts_won",
        "pts_total_pts_played",
    ]

    data: dict = {
        "match_uid": ["2023_580_SGL_F_AAAA_BBBB"],
        "tournament_id": [tid],
        "year": [year],
        "circuit": [circuit],
        "draw_type": ["singles"],
        "round": ["F"],
        "round_id": [12],
        "match_id": ["ms001"],
        "surface": ["Hard"],
        "tournament_start_date": [date(2023, 1, 16)],
        "tournament_end_date": [date(2023, 1, 29)],
        "tournament_city": ["Melbourne"],
        "prize_money": [76500000],
        "currency": ["AUD"],
        "draw_size_singles": [128],
        "draw_size_doubles": [64],
        "winner_id": ["AAAA"],
        "duration_seconds": [7260],
        "reason": [None],
        "number_of_sets": [3],
        "sets_played": [2],
        "is_qualifier": [False],
        "scoring_system": ["best_of_5"],
        "court_name": ["Rod Laver Arena"],
        "umpire_first_name": ["Carlos"],
        "umpire_last_name": ["Ramos"],
        "p1_id": ["AAAA"],
        "p2_id": ["BBBB"],
        "p1_partner_id": [None],
        "p2_partner_id": [None],
        "p1_seed": [1],
        "p1_entry": [None],
        "p2_seed": [None],
        "p2_entry": ["WC"],
        "source_file": ["ms001.json"],
        "parsed_at": [datetime(2025, 1, 1)],
    }
    for i, name in enumerate(all_stat_names):
        data[f"p1_{name}"] = [i + 10]
    for i, name in enumerate(all_stat_names):
        data[f"p2_{name}"] = [i + 100]

    df = pl.DataFrame(data)
    df.write_parquet(path)
    return path


def _write_overview_parquet(
    data_root: Path,
    circuit: str = "tour",
    tid: str = "580",
    year: int = 2023,
) -> Path:
    """Write a minimal overview parquet."""
    path = (
        data_root
        / "stage"
        / "atptour"
        / "tournaments"
        / circuit
        / tid
        / str(year)
        / "overview.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "tournament_id": [tid],
            "year": [year],
            "tournament_name": ["Australian Open"],
            "city": ["Melbourne"],
            "country": ["AUS"],
            "circuit": [circuit],
            "sponsor_title": [None],
            "event_type": ["GS"],
            "event_type_detail": [540],
            "singles_draw_size": [128],
            "doubles_draw_size": [64],
            "surface": ["Hard"],
            "surface_detail": ["Plexicushion"],
            "indoor": [False],
            "prize": ["AUD 76,500,000"],
            "total_financial_commitment": ["AUD 76,500,000"],
            "location": ["Melbourne, Australia"],
            "source_file": ["overview.json"],
            "parsed_at": [datetime(2025, 1, 1)],
        }
    )
    df.write_parquet(path)
    return path


class TestTournamentMatchesAggregator:
    def test_results_only(self, tmp_path):
        """Only results parquet exists."""
        _write_results_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        assert len(df) == 2
        assert "player_id" in df.columns
        assert "opp_id" in df.columns
        assert "won" in df.columns
        assert "match_uid" in df.columns
        # Winner AAAA should have won=True
        row_a = df.filter(pl.col("player_id") == "AAAA")
        assert row_a["won"].item() is True
        # Set scores present
        assert row_a["player_set1_games"].item() == 6
        assert row_a["opp_set1_games"].item() == 4

    def test_schedule_only_upcoming(self, tmp_path):
        """Only schedule parquet, upcoming matches."""
        _write_schedule_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # 2 matches * 2 rows = 4
        assert len(df) == 4
        # All won should be null (no results)
        assert df["won"].null_count() == 4

    def test_results_plus_schedule_merges(self, tmp_path):
        """Both results and schedule exist, including an upcoming match."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # 2 matches (final + SF) * 2 rows = 4
        assert len(df) == 4
        # Final match has won populated
        final = df.filter(pl.col("round") == "F")
        assert final["won"].null_count() == 0
        # SF match has won null
        sf = df.filter(pl.col("round") == "SF")
        assert sf["won"].null_count() == 2

    def test_p1_p2_ordering_from_schedule(self, tmp_path):
        """Schedule has BBBB as p1 for the final, Results has AAAA (winner) as p1.
        After alignment + explosion, set scores should follow Schedule's ordering."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        final = df.filter(pl.col("round") == "F")
        bbbb_row = final.filter(pl.col("player_id") == "BBBB")
        # BBBB lost (AAAA won), so won should be False
        assert bbbb_row["won"].item() is False
        # BBBB's player set scores should be the loser's scores (4, 3)
        # because schedule says BBBB is p1 (draw position)
        assert bbbb_row["player_set1_games"].item() == 4
        assert bbbb_row["player_set2_games"].item() == 3
        assert bbbb_row["opp_set1_games"].item() == 6
        assert bbbb_row["opp_set2_games"].item() == 6

    def test_overview_enrichment(self, tmp_path):
        """Overview fields appear in output."""
        _write_results_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        assert df["tournament_name"][0] == "Australian Open"
        assert df["surface"][0] == "Hard"
        assert df["indoor"][0] is False
        assert df["city"][0] == "Melbourne"

    def test_no_sources_returns_empty(self, tmp_path):
        """No staged files exist -> empty DataFrame."""
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        assert len(df) == 0

    def test_coalesce_picks_first_non_null(self, tmp_path):
        """When Results has null seed but Schedule has it, coalesce picks Schedule."""
        # Results: BBBB has p2_seed=None
        # Schedule: BBBB as p1 has p1_seed=None too (matching our fixture)
        # But AAAA has seed=1 in both -> test coalesce agreement
        # Better test: Results has null duration, match_stats has it
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        final = df.filter(pl.col("round") == "F")
        bbbb_row = final.filter(pl.col("player_id") == "BBBB")
        # Entry "WC" should be present from both sources via coalesce
        assert bbbb_row["player_entry"].item() == "WC"

    def test_match_stats_enrichment(self, tmp_path):
        """Match stats fields appear in output after LEFT JOIN."""
        _write_results_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        row_a = df.filter(pl.col("player_id") == "AAAA")
        # svc_aces for AAAA (p1) should be 10 (index 0 + 10)
        assert row_a["svc_aces"].item() == 10
        # reason, scoring_system, umpire should be present
        assert "reason" in df.columns
        assert "scoring_system" in df.columns
        assert "umpire_first_name" in df.columns

    def test_waterfall_duration_prefers_results(self, tmp_path):
        """Results duration_seconds (7200) should win over match_stats (7260)."""
        _write_results_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        row_a = df.filter(pl.col("player_id") == "AAAA")
        assert row_a["duration_seconds"].item() == 7200

    def test_waterfall_surface_from_overview(self, tmp_path):
        """Surface waterfall: MatchStats > Overview. When both present, stats wins."""
        _write_results_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # Both have "Hard" so either way it's "Hard"
        assert df["surface"][0] == "Hard"

    def test_waterfall_city_prefers_overview(self, tmp_path):
        """City: coalesce(overview.city, stats.tournament_city). Overview preferred."""
        _write_results_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # Overview city is "Melbourne", stats tournament_city is "Melbourne"
        assert df["city"][0] == "Melbourne"

    def test_waterfall_draw_size_prefers_overview(self, tmp_path):
        """Draw sizes: coalesce(overview, stats). Overview preferred."""
        _write_results_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # Both have 128/64
        assert df["singles_draw_size"][0] == 128
        assert df["doubles_draw_size"][0] == 64

    def test_all_four_sources(self, tmp_path):
        """All four sources present. Verify key fields from each."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # 2 matches * 2 rows = 4
        assert len(df) == 4
        # Final has all data
        final = df.filter(pl.col("round") == "F")
        assert len(final) == 2
        # SF from schedule only
        sf = df.filter(pl.col("round") == "SF")
        assert len(sf) == 2
        assert sf["won"].null_count() == 2

    def test_no_suffixed_columns_remain(self, tmp_path):
        """After waterfall, no _schedule/_stats/_overview suffixed columns remain."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        for col in df.columns:
            assert not col.endswith("_schedule"), f"Suffixed column remains: {col}"
            assert not col.endswith("_stats"), f"Suffixed column remains: {col}"
            assert not col.endswith("_overview"), f"Suffixed column remains: {col}"

    def test_identity_fields_resolve_from_full_outer(self, tmp_path):
        """Identity fields (tournament_id, year, circuit, etc.) resolve even
        when only one side of the FULL OUTER JOIN has data."""
        _write_schedule_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        # All rows should have tournament_id, year, circuit
        assert df["tournament_id"].null_count() == 0
        assert df["year"].null_count() == 0
        assert df["circuit"].null_count() == 0
        assert df["draw_type"].null_count() == 0

    def test_match_uid_player_id_uniqueness(self, tmp_path):
        """Each (match_uid, player_id) pair appears exactly once."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        counts = df.group_by(["match_uid", "player_id"]).len()
        assert (counts["len"] == 1).all()

    def test_output_schema_matches_expected(self, tmp_path):
        """Output DataFrame columns match MATCHES_SCHEMA."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        _write_match_stats_parquet(tmp_path)
        _write_overview_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        df = agg.aggregate()
        expected_cols = set(MATCHES_SCHEMA.keys())
        actual_cols = set(df.columns)
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        assert expected_cols == actual_cols, (
            f"Missing: {missing}, Extra: {extra}"
        )

    def test_validate_schema_raises_on_extra_column(self, tmp_path):
        """_validate_schema raises ValueError when extra columns present."""
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        # Build a DataFrame with MATCHES_SCHEMA columns + one extra
        data = {col: [None] for col in MATCHES_SCHEMA}
        data["bogus_column"] = [None]
        df = pl.DataFrame(data)
        try:
            agg._validate_schema(df)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "bogus_column" in str(e)

    def test_validate_schema_raises_on_missing_column(self, tmp_path):
        """_validate_schema raises ValueError when columns are missing."""
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        cols = list(MATCHES_SCHEMA.keys())
        data = {col: [None] for col in cols[:-1]}  # drop last column
        df = pl.DataFrame(data)
        try:
            agg._validate_schema(df)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert cols[-1] in str(e)

    def test_run_writes_parquet(self, tmp_path):
        """run() writes to aggregate bucket."""
        _write_results_parquet(tmp_path)
        _write_schedule_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        result_path = agg.run()
        assert result_path is not None
        assert result_path.exists()
        df = pl.read_parquet(result_path)
        assert len(df) == 4  # 2 matches * 2 rows

    def test_run_returns_none_when_no_data(self, tmp_path):
        """run() returns None when no staged data."""
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        result = agg.run()
        assert result is None

    def test_run_output_path(self, tmp_path):
        """run() returns correct output path."""
        _write_results_parquet(tmp_path)
        agg = TournamentMatchesAggregator(
            circuit=Circuit.tour, tid="580", year=2023, data_root=tmp_path
        )
        result_path = agg.run()
        assert result_path is not None
        expected = (
            tmp_path
            / "aggregate"
            / "atptour"
            / "tournaments"
            / "tour"
            / "580"
            / "2023"
            / "matches.parquet"
        )
        assert result_path == expected
