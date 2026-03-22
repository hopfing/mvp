"""Tests for MatchStatsTransformer — raw JSON to staged parquet."""

import json
from pathlib import Path

import polars as pl

from mvp.atptour.tournament import Tournament
from mvp.atptour.transformers.match_stats import MatchStatsTransformer
from mvp.common.enums import Circuit


def _stat_block(
    *,
    svc_aces: int = 6,
    svc_double_faults: int = 2,
    svc_first_serve_in: int = 54,
    svc_first_serve_att: int = 73,
    svc_first_serve_pts_won: int = 38,
    svc_first_serve_pts_played: int = 54,
    svc_second_serve_pts_won: int = 11,
    svc_second_serve_pts_played: int = 19,
    svc_bp_saved: int = 2,
    svc_bp_faced: int = 4,
    svc_games_played: int = 10,
    svc_serve_rating: int = 288,
    ret_first_serve_pts_won: int = 20,
    ret_first_serve_pts_played: int = 44,
    ret_second_serve_pts_won: int = 13,
    ret_second_serve_pts_played: int = 26,
    ret_bp_converted: int = 4,
    ret_bp_opportunities: int = 11,
    ret_games_played: int = 9,
    ret_return_rating: int = 175,
    pts_service_pts_won: int = 49,
    pts_service_pts_played: int = 73,
    pts_return_pts_won: int = 33,
    pts_return_pts_played: int = 70,
    pts_total_pts_won: int = 82,
    pts_total_pts_played: int = 143,
) -> dict:
    """Build a Stats JSON object with defaults matching real data."""
    return {
        "Time": "02:01:00",
        "ServiceStats": {
            "ServeRating": {"Number": svc_serve_rating, "IsStatBetter": None},
            "DoubleFaults": {"Number": svc_double_faults, "IsStatBetter": True},
            "Aces": {"Number": svc_aces, "IsStatBetter": True},
            "FirstServe": {
                "Percent": 74,
                "Dividend": svc_first_serve_in,
                "Divisor": svc_first_serve_att,
                "IsStatBetter": True,
            },
            "FirstServePointsWon": {
                "Percent": 70,
                "Dividend": svc_first_serve_pts_won,
                "Divisor": svc_first_serve_pts_played,
                "IsStatBetter": True,
            },
            "SecondServePointsWon": {
                "Percent": 58,
                "Dividend": svc_second_serve_pts_won,
                "Divisor": svc_second_serve_pts_played,
                "IsStatBetter": True,
            },
            "BreakPointsSaved": {
                "Percent": 50,
                "Dividend": svc_bp_saved,
                "Divisor": svc_bp_faced,
                "IsStatBetter": False,
            },
            "ServiceGamesPlayed": {"Number": svc_games_played, "IsStatBetter": True},
        },
        "ReturnStats": {
            "ReturnRating": {"Number": ret_return_rating, "IsStatBetter": None},
            "FirstServeReturnPointsWon": {
                "Percent": 45,
                "Dividend": ret_first_serve_pts_won,
                "Divisor": ret_first_serve_pts_played,
                "IsStatBetter": True,
            },
            "SecondServeReturnPointsWon": {
                "Percent": 50,
                "Dividend": ret_second_serve_pts_won,
                "Divisor": ret_second_serve_pts_played,
                "IsStatBetter": True,
            },
            "BreakPointsConverted": {
                "Percent": 36,
                "Dividend": ret_bp_converted,
                "Divisor": ret_bp_opportunities,
                "IsStatBetter": True,
            },
            "ReturnGamesPlayed": {"Number": ret_games_played, "IsStatBetter": False},
        },
        "PointStats": {
            "TotalServicePointsWon": {
                "Percent": 67,
                "Dividend": pts_service_pts_won,
                "Divisor": pts_service_pts_played,
                "IsStatBetter": True,
            },
            "TotalReturnPointsWon": {
                "Percent": 47,
                "Dividend": pts_return_pts_won,
                "Divisor": pts_return_pts_played,
                "IsStatBetter": True,
            },
            "TotalPointsWon": {
                "Percent": 57,
                "Dividend": pts_total_pts_won,
                "Divisor": pts_total_pts_played,
                "IsStatBetter": True,
            },
        },
    }


def _team_data(
    player_id: str,
    partner_id: str | None = None,
    stats: dict | None = None,
    set_scores: list | None = None,
) -> dict:
    """Build PlayerTeam/OpponentTeam data."""
    player = {
        "PlayerId": player_id,
        "PlayerCountry": "USA",
        "PlayerFirstName": "Test",
        "PlayerLastName": "Player",
        "PlayerCountryName": "United States",
        "PlayerScRelativeUrlPlayerProfile": f"/en/players/x/{player_id}/overview",
    }
    partner = None
    if partner_id is not None:
        partner = {
            "PlayerId": partner_id,
            "PlayerCountry": "GBR",
            "PlayerFirstName": "Partner",
            "PlayerLastName": "Player",
            "PlayerCountryName": "Great Britain",
            "PlayerScRelativeUrlPlayerProfile": f"/en/players/x/{partner_id}/overview",
        }
    if set_scores is None:
        if stats is None:
            stats = _stat_block()
        set_scores = [
            {"SetNumber": 0, "SetScore": None, "TieBreakScore": None, "Stats": stats},
            {"SetNumber": 1, "SetScore": "6", "TieBreakScore": None, "Stats": None},
            {"SetNumber": 2, "SetScore": "4", "TieBreakScore": None, "Stats": None},
        ]
    return {
        "Player": player,
        "Partner": partner,
        "GameScore": None,
        "SetScores": set_scores,
    }


def _player_team_flat(
    player_id: str,
    seed: str | None = None,
    partner_id: str | None = None,
) -> dict:
    """Build PlayerTeam1/PlayerTeam2 flat data."""
    return {
        "PlayerId": player_id,
        "PartnerId": partner_id,
        "PlayerFirstName": "T.",
        "PlayerFirstNameFull": "Test",
        "PlayerLastName": "Player",
        "PlayerCountryCode": "USA",
        "SeedPlayerTeam": seed,
        "EntryStatusPlayerTeam": None,
    }


def _match_json(
    *,
    match_id: str = "MS001",
    is_doubles: bool = False,
    round_long_name: str = "Finals",
    round_id: int = 7,
    winner: str = "D643",
    duration: str = "02:01:00",
    reason: str | None = None,
    number_of_sets: int = 2,
    is_qualifier: bool | None = None,
    scoring_system: str | None = None,
    court_name: str | None = None,
    umpire_first_name: str = "",
    umpire_last_name: str = "",
    player_team: dict | None = None,
    opponent_team: dict | None = None,
    player_team1: dict | None = None,
    player_team2: dict | None = None,
    surface: str = "Clay",
    start_date: str = "2023-01-16T00:00:00",
    end_date: str = "2023-01-29T00:00:00",
    tournament_city: str = "Melbourne",
    prize_money: int = 53120,
    currency: str = "$",
    draw_size_singles: int = 128,
    draw_size_doubles: int = 64,
    p1_id: str = "D643",
    p2_id: str = "TE51",
    p1_partner_id: str | None = None,
    p2_partner_id: str | None = None,
    p1_seed: str | None = None,
    p2_seed: str | None = None,
    p1_stats: dict | None = None,
    p2_stats: dict | None = None,
) -> dict:
    """Build a complete match stats JSON with all defaults."""
    if player_team is None:
        player_team = _team_data(p1_id, partner_id=p1_partner_id, stats=p1_stats)
    if opponent_team is None:
        opponent_team = _team_data(p2_id, partner_id=p2_partner_id, stats=p2_stats)
    if player_team1 is None:
        player_team1 = _player_team_flat(p1_id, seed=p1_seed, partner_id=p1_partner_id)
    if player_team2 is None:
        player_team2 = _player_team_flat(p2_id, seed=p2_seed, partner_id=p2_partner_id)

    return {
        "Tournament": {
            "EventYear": 2023,
            "EventId": "580",
            "EventDisplayName": "Australian Open",
            "Court": surface,
            "StartDate": start_date,
            "EndDate": end_date,
            "Singles": draw_size_singles,
            "Doubles": draw_size_doubles,
            "TournamentName": "Australian Open",
            "PrizeMoney": prize_money,
            "CurrencySymbol": currency,
            "TournamentCity": tournament_city,
        },
        "Match": {
            "MatchId": match_id,
            "IsDoubles": is_doubles,
            "Round": {
                "RoundId": round_id,
                "ShortName": "F",
                "LongName": round_long_name,
            },
            "CourtName": court_name,
            "MatchTimeTotal": duration,
            "Winner": winner,
            "IsQualifier": is_qualifier,
            "NumberOfSets": number_of_sets,
            "ScoringSystem": scoring_system,
            "Reason": reason,
            "UmpireFirstName": umpire_first_name,
            "UmpireLastName": umpire_last_name,
            "PlayerTeam": player_team,
            "OpponentTeam": opponent_team,
            "PlayerTeam1": player_team1,
            "PlayerTeam2": player_team2,
        },
    }


def _make_tournament(**kwargs) -> Tournament:
    defaults = {
        "tournament_id": "580",
        "year": 2023,
        "circuit": Circuit.tour,
        "location": "Melbourne, Australia",
    }
    defaults.update(kwargs)
    return Tournament(**defaults)


def _write_json(
    tmp_path: Path,
    tournament: Tournament,
    filename: str,
    data: dict | None,
) -> None:
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "match_stats"
    raw_dir.mkdir(parents=True, exist_ok=True)
    with (raw_dir / filename).open("w", encoding="utf-8") as f:
        json.dump(data, f)


class TestTransformSingles:
    def test_produces_parquet(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", _match_json())
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        assert paths[0].exists()
        assert paths[0].name == "match_stats.parquet"

    def test_correct_record_count(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", _match_json())
        _write_json(
            tmp_path,
            t,
            "ms002.json",
            _match_json(
                match_id="MS002",
                round_long_name="Semifinals",
                p1_id="A111",
                p2_id="B222",
                winner="A111",
            ),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 2

    def test_player_ids_uppercased(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(p1_id="d643", p2_id="te51", winner="d643"),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["p1_id"] == "D643"
        assert row["p2_id"] == "TE51"

    def test_context_fields(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", _match_json())
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["tournament_id"] == "580"
        assert row["year"] == 2023
        assert row["circuit"] == "tour"
        assert row["draw_type"] == "singles"
        assert row["round"] == "F"
        assert row["match_id"] == "MS001"

    def test_tournament_metadata(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(
                surface="Hard",
                tournament_city="Melbourne",
                prize_money=75000000,
                currency="$",
                draw_size_singles=128,
                draw_size_doubles=64,
                start_date="2023-01-16T00:00:00",
                end_date="2023-01-29T00:00:00",
            ),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["surface"] == "Hard"
        assert row["tournament_city"] == "Melbourne"
        assert row["prize_money"] == 75000000
        assert row["currency"] == "$"
        assert row["draw_size_singles"] == 128
        assert row["draw_size_doubles"] == 64
        from datetime import date

        assert row["tournament_start_date"] == date(2023, 1, 16)
        assert row["tournament_end_date"] == date(2023, 1, 29)

    def test_match_metadata(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(
                reason="RET",
                court_name="Rod Laver Arena",
                is_qualifier=True,
                umpire_first_name="Carlos",
                umpire_last_name="Ramos",
                round_id=5,
            ),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["reason"] == "RET"
        assert row["court_name"] == "Rod Laver Arena"
        assert row["is_qualifier"] is True
        assert row["umpire_first_name"] == "Carlos"
        assert row["umpire_last_name"] == "Ramos"
        assert row["round_id"] == 5

    def test_stat_fields(self, tmp_path):
        t = _make_tournament()
        p1_stats = _stat_block(svc_aces=10, ret_bp_converted=5, pts_total_pts_won=90)
        p2_stats = _stat_block(svc_aces=3, ret_bp_converted=2, pts_total_pts_won=70)
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(p1_stats=p1_stats, p2_stats=p2_stats),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["p1_svc_aces"] == 10
        assert row["p2_svc_aces"] == 3
        assert row["p1_ret_bp_converted"] == 5
        assert row["p2_ret_bp_converted"] == 2
        assert row["p1_pts_total_pts_won"] == 90
        assert row["p2_pts_total_pts_won"] == 70

    def test_duration_parsed(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(duration="02:30:00"),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        # 2*3600 + 30*60 + 0 = 9000
        assert row["duration_seconds"] == 9000

    def test_seed_extracted(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(p1_seed="4", p2_seed="7"),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["p1_seed"] == 4
        assert row["p2_seed"] == 7
        assert row["p1_entry"] is None
        assert row["p2_entry"] is None

    def test_seed_with_entry(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(p1_seed="1/WC", p2_seed="Q"),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["p1_seed"] == 1
        assert row["p1_entry"] == "WC"
        assert row["p2_seed"] is None
        assert row["p2_entry"] == "Q"

    def test_null_seed(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "ms001.json",
            _match_json(p1_seed=None, p2_seed=None),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["p1_seed"] is None
        assert row["p2_seed"] is None
        assert row["p1_entry"] is None
        assert row["p2_entry"] is None

    def test_match_uid_computed(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", _match_json())
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["match_uid"] is not None
        assert row["match_uid"] == "2023_580_SGL_F_D643_TE51"

    def test_source_file_recorded(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", _match_json())
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert "ms001.json" in row["source_file"]


class TestTransformDoubles:
    def test_doubles_fields(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "md001.json",
            _match_json(
                match_id="MD001",
                is_doubles=True,
                p1_id="A853",
                p2_id="B123",
                p1_partner_id="R513",
                p2_partner_id="C456",
                winner="A853",
                scoring_system="9",
            ),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)

        assert row["draw_type"] == "doubles"
        assert row["p1_partner_id"] == "R513"
        assert row["p2_partner_id"] == "C456"
        assert row["scoring_system"] == "9"

    def test_doubles_match_uid(self, tmp_path):
        t = _make_tournament()
        _write_json(
            tmp_path,
            t,
            "md001.json",
            _match_json(
                match_id="MD001",
                is_doubles=True,
                p1_id="A853",
                p2_id="B123",
                p1_partner_id="R513",
                p2_partner_id="C456",
                winner="A853",
            ),
        )
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["match_uid"] is not None
        assert "DBL" in row["match_uid"]


class TestUniquenessAssertion:
    def test_assertion_fires_on_duplicate_match_uid(self):
        df = pl.DataFrame({
            "match_uid": ["uid1", "uid1", "uid2"],
        })
        import pytest

        with pytest.raises(ValueError, match="Duplicate primary keys"):
            MatchStatsTransformer.assert_unique(df, ["match_uid"], "match_stats")

    def test_assertion_skips_null_uids(self):
        df = pl.DataFrame({
            "match_uid": [None, None, "uid1"],
        })
        MatchStatsTransformer.assert_unique(df, ["match_uid"], "match_stats")


class TestSkipConditions:
    def test_no_json_returns_empty(self, tmp_path):
        t = _make_tournament()
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert paths == []

    def test_null_json_skipped(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", None)
        _write_json(tmp_path, t, "ms002.json", _match_json(
            match_id="MS002",
            p1_id="A111",
            p2_id="B222",
            winner="A111",
            round_long_name="Semifinals",
        ))
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        df = pl.read_parquet(paths[0])
        assert len(df) == 1
        assert df.row(0, named=True)["match_id"] == "MS002"

    def test_empty_set_scores_skipped(self, tmp_path):
        t = _make_tournament()
        data = _match_json()
        data["Match"]["PlayerTeam"]["SetScores"] = []
        data["Match"]["OpponentTeam"]["SetScores"] = []
        _write_json(tmp_path, t, "ms001.json", data)
        _write_json(tmp_path, t, "ms002.json", _match_json(
            match_id="MS002",
            p1_id="A111",
            p2_id="B222",
            winner="A111",
            round_long_name="Semifinals",
        ))
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_null_stats_skipped(self, tmp_path):
        t = _make_tournament()
        data = _match_json()
        data["Match"]["PlayerTeam"]["SetScores"][0]["Stats"] = None
        _write_json(tmp_path, t, "ms001.json", data)
        _write_json(tmp_path, t, "ms002.json", _match_json(
            match_id="MS002",
            p1_id="A111",
            p2_id="B222",
            winner="A111",
            round_long_name="Semifinals",
        ))
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_all_skipped_returns_empty(self, tmp_path):
        t = _make_tournament()
        _write_json(tmp_path, t, "ms001.json", None)
        _write_json(tmp_path, t, "ms002.json", None)
        xf = MatchStatsTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert paths == []
