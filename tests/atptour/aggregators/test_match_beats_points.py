"""Tests for MatchBeatsPointsAggregator."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from mvp.atptour.aggregators.match_beats_points import MatchBeatsPointsAggregator


def _write_stage(data_root: Path, points: pl.DataFrame, *, circuit: str = "tour", tid: str = "999", year: int = 2025) -> None:
    path = (
        data_root
        / "stage"
        / "atptour"
        / "tournaments"
        / circuit
        / tid
        / str(year)
        / "match_beats.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    points.write_parquet(path)


def _write_matches(data_root: Path, rows: list[dict]) -> None:
    path = data_root / "aggregate" / "atptour" / "matches.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _match_meta_row(
    *,
    tournament_id: str = "999",
    year: int = 2025,
    match_id: str = "MS001",
    match_uid: str = "uid-1",
    best_of: int = 3,
) -> dict:
    return {
        "tournament_id": tournament_id,
        "year": year,
        "match_id": match_id,
        "match_uid": match_uid,
        "circuit": "tour",
        "surface": "hard",
        "round": "R32",
        "effective_match_date": date(2025, 3, 10),
        "best_of": best_of,
    }


def _point_row(
    *,
    point_num: int,
    set_num: int = 1,
    game_num: int = 1,
    server: str = "1",
    scorer: str = "1",
    serve: int = 1,
    p1_game_score: str = "0",
    p2_game_score: str = "0",
    game_winner: str | None = None,
    set_winner: str | None = None,
    is_tiebreak: bool = False,
    is_break_point: bool = False,
    is_doubles: bool = False,
) -> dict:
    return {
        "tournament_id": "999",
        "year": 2025,
        "match_id": "MS001",
        "is_doubles": is_doubles,
        "p1_id": "PLAYER1",
        "p2_id": "PLAYER2",
        "set_num": set_num,
        "set_winner": set_winner,
        "game_num": game_num,
        "game_winner": game_winner,
        "game_duration": 120,
        "easy_hold": False,
        "difficult_hold": False,
        "multiple_deuces": False,
        "is_tiebreak": is_tiebreak,
        "point_num": point_num,
        "point_id": f"P{point_num}",
        "result": None,
        "scorer": scorer,
        "server": server,
        "serve": serve,
        "serve_speed": None,
        "fault_serve_speed": None,
        "p1_rally_shots": 0,
        "p2_rally_shots": 0,
        "rally_length_missing": True,
        "is_break_point": is_break_point,
        "break_points_in_game": 0,
        "break_points_lost": 0,
        "is_crucial_point": False,
        "p1_game_score": p1_game_score,
        "p2_game_score": p2_game_score,
        "match_duration_at_point": point_num * 60,
        "source_file": "test.json",
        "parsed_at": None,
        "schema_hash": "test",
    }


def _df(points: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(points)


class TestMatchBeatsPointsAggregator:
    def test_requires_matches_parquet(self, tmp_path):
        _write_stage(tmp_path, _df([_point_row(point_num=1, server="1", scorer="1")]))
        with pytest.raises(FileNotFoundError):
            MatchBeatsPointsAggregator(data_root=tmp_path).run()

    def test_filters_doubles(self, tmp_path):
        _write_stage(tmp_path, _df([_point_row(point_num=1, is_doubles=True)]))
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        assert result is None

    def test_server_perspective_mapping(self, tmp_path):
        # Raw game_score cols are POST-point. Point 2's pre-state = point 1's post-state.
        # Single game, P1 serving. Point 1 post = 15-0, point 2 post = 30-0. Point 2 pre = 15-0.
        _write_stage(
            tmp_path,
            _df([
                _point_row(point_num=1, server="1", scorer="1", p1_game_score="15", p2_game_score="0"),
                _point_row(point_num=2, server="1", scorer="1", p1_game_score="30", p2_game_score="0"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        assert result is not None

        # Point 1: first point of game, pre-state = 0-0
        row1 = result.filter(pl.col("point_num") == 1).to_dicts()[0]
        assert row1["server_id"] == "PLAYER1"
        assert row1["returner_id"] == "PLAYER2"
        assert row1["game_score_server"] == "0"
        assert row1["game_score_returner"] == "0"

        # Point 2: pre-state inherited from point 1 post-state (15-0)
        row2 = result.filter(pl.col("point_num") == 2).to_dicts()[0]
        assert row2["game_score_server"] == "15"
        assert row2["game_score_returner"] == "0"

    def test_point_won_by_server(self, tmp_path):
        _write_stage(
            tmp_path,
            _df([
                _point_row(point_num=1, server="1", scorer="1"),
                _point_row(point_num=2, server="1", scorer="2"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        rows = result.sort("point_num").to_dicts()
        assert rows[0]["point_won_by_server"] is True
        assert rows[1]["point_won_by_server"] is False

    def test_cumulative_set_scores(self, tmp_path):
        # Game 1: P1 wins (game_winner="1"). Game 2: P2 wins (game_winner="2"). Game 3 in progress.
        _write_stage(
            tmp_path,
            _df([
                _point_row(point_num=1, game_num=1, server="1", scorer="1", game_winner="1"),
                _point_row(point_num=1, game_num=2, server="2", scorer="2", game_winner="2"),
                _point_row(point_num=1, game_num=3, server="1", scorer="1"),  # game in progress
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        rows = result.sort(["game_num"]).to_dicts()

        # Game 1 (pre-game): 0-0 set score
        assert rows[0]["set_score_server_games"] == 0
        assert rows[0]["set_score_returner_games"] == 0

        # Game 2 (pre-game): P1 has 1, P2 has 0. P2 is serving, so server=P2=0, returner=P1=1.
        assert rows[1]["set_score_server_games"] == 0
        assert rows[1]["set_score_returner_games"] == 1

        # Game 3 (pre-game): 1-1. P1 serving.
        assert rows[2]["set_score_server_games"] == 1
        assert rows[2]["set_score_returner_games"] == 1

    def test_cumulative_sets_won(self, tmp_path):
        # Set 1: P1 wins (set_winner="1"). Set 2 in progress.
        _write_stage(
            tmp_path,
            _df([
                _point_row(point_num=1, set_num=1, game_num=1, server="1", scorer="1", game_winner="1", set_winner="1"),
                _point_row(point_num=1, set_num=2, game_num=1, server="1", scorer="1"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        rows = result.sort("set_num").to_dicts()

        assert rows[0]["sets_won_server"] == 0
        assert rows[0]["sets_won_returner"] == 0
        # Set 2: P1 has 1 set, P2 has 0. Server=P1.
        assert rows[1]["sets_won_server"] == 1
        assert rows[1]["sets_won_returner"] == 0

    def test_set_point_detected_at_5_5_40_love(self, tmp_path):
        # Set score 5-5, P1 serving game 11. Establish pre-state 40-0 at point 4
        # by having points 1-3 build up to post-state 40-0.
        _write_stage(
            tmp_path,
            _df([
                *[
                    _point_row(point_num=1, set_num=1, game_num=g, server="1" if g % 2 else "2",
                               scorer="1" if g % 2 else "2",
                               game_winner="1" if g % 2 else "2")
                    for g in range(1, 11)
                ],
                # Game 11: P1 serving, 3 points to reach post=40-0, then a 4th to test.
                _point_row(point_num=1, set_num=1, game_num=11, server="1", scorer="1", p1_game_score="15", p2_game_score="0"),
                _point_row(point_num=2, set_num=1, game_num=11, server="1", scorer="1", p1_game_score="30", p2_game_score="0"),
                _point_row(point_num=3, set_num=1, game_num=11, server="1", scorer="1", p1_game_score="40", p2_game_score="0"),
                _point_row(point_num=4, set_num=1, game_num=11, server="1", scorer="1", p1_game_score="GAME", p2_game_score="0"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        # Point 4 is the actual game point — pre-state 40-0. But winning → 6-5, not a set win.
        row = result.filter((pl.col("game_num") == 11) & (pl.col("point_num") == 4)).to_dicts()[0]
        assert row["set_score_server_games"] == 5
        assert row["set_score_returner_games"] == 5
        assert row["game_score_server"] == "40"
        assert row["is_set_point"] is False

    def test_set_point_detected_at_5_4_server_40_love(self, tmp_path):
        # Pre-point: P1 has 5 games, P2 has 4. P1 serving at 40-0 on game point.
        _write_stage(
            tmp_path,
            _df([
                *[_point_row(point_num=1, set_num=1, game_num=g, server="1" if g % 2 else "2",
                             scorer="1" if g % 2 else "2",
                             game_winner="1" if g % 2 else "2")
                  for g in range(1, 9)],  # 4-4
                _point_row(point_num=1, set_num=1, game_num=9, server="1", scorer="1", game_winner="1"),  # 5-4
                # Game 10: points build to pre-state 40-0 at point 4
                _point_row(point_num=1, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="15", p2_game_score="0"),
                _point_row(point_num=2, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="30", p2_game_score="0"),
                _point_row(point_num=3, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="40", p2_game_score="0"),
                _point_row(point_num=4, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="GAME", p2_game_score="0"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        row = result.filter((pl.col("game_num") == 10) & (pl.col("point_num") == 4)).to_dicts()[0]
        assert row["set_score_server_games"] == 5
        assert row["set_score_returner_games"] == 4
        assert row["game_score_server"] == "40"
        assert row["is_set_point"] is True

    def test_match_point_detected_bo3(self, tmp_path):
        # Pre-point: server won set 1, now leading 5-4 in set 2, at 40-0 (game point).
        # BO3: winning this game wins the set and the match.
        _write_stage(
            tmp_path,
            _df([
                # Set 1: P1 wins 6-0. set_winner filled on every point in real data.
                *[_point_row(point_num=1, set_num=1, game_num=g, server="1" if g % 2 else "2",
                             scorer="1", game_winner="1", set_winner="1")
                  for g in range(1, 7)],
                # Set 2 to 4-4
                *[_point_row(point_num=1, set_num=2, game_num=g, server="1" if g % 2 else "2",
                             scorer="1" if g % 2 else "2",
                             game_winner="1" if g % 2 else "2")
                  for g in range(1, 9)],
                _point_row(point_num=1, set_num=2, game_num=9, server="1", scorer="1", game_winner="1"),  # 5-4
                # Game 10: P1 serving, build to pre-state 40-0 at point 4
                _point_row(point_num=1, set_num=2, game_num=10, server="1", scorer="1", p1_game_score="15", p2_game_score="0"),
                _point_row(point_num=2, set_num=2, game_num=10, server="1", scorer="1", p1_game_score="30", p2_game_score="0"),
                _point_row(point_num=3, set_num=2, game_num=10, server="1", scorer="1", p1_game_score="40", p2_game_score="0"),
                _point_row(point_num=4, set_num=2, game_num=10, server="1", scorer="1", p1_game_score="GAME", p2_game_score="0"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row(best_of=3)])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        row = result.filter(
            (pl.col("set_num") == 2) & (pl.col("game_num") == 10) & (pl.col("point_num") == 4)
        ).to_dicts()[0]
        assert row["sets_won_server"] == 1
        assert row["set_score_server_games"] == 5
        assert row["game_score_server"] == "40"
        assert row["is_match_point"] is True
        assert row["is_set_point"] is True

    def test_tiebreak_suppresses_set_and_match_point(self, tmp_path):
        _write_stage(
            tmp_path,
            _df([
                # Tiebreak point with pre-state that would otherwise trigger set-point.
                _point_row(point_num=1, set_num=1, game_num=13, server="1", scorer="1",
                           p1_game_score="40", p2_game_score="0", is_tiebreak=True),
                _point_row(point_num=2, set_num=1, game_num=13, server="1", scorer="1",
                           p1_game_score="GAME", p2_game_score="0", is_tiebreak=True),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        # Point 2: pre-state 40-0, in tiebreak → flags suppressed
        row = result.filter(pl.col("point_num") == 2).to_dicts()[0]
        assert row["is_tiebreak"] is True
        assert row["is_set_point"] is False
        assert row["is_match_point"] is False

    def test_ad_score_is_game_point(self, tmp_path):
        # P1 serving at AD, P2 at 40. Set score 5-4. Should be set point.
        _write_stage(
            tmp_path,
            _df([
                *[_point_row(point_num=1, set_num=1, game_num=g, server="1" if g % 2 else "2",
                             scorer="1" if g % 2 else "2",
                             game_winner="1" if g % 2 else "2")
                  for g in range(1, 9)],
                _point_row(point_num=1, set_num=1, game_num=9, server="1", scorer="1", game_winner="1"),  # 5-4
                # Game 10: build to pre-state AD-40 via deuce path
                # Sequence: 15-0, 15-15, 30-15, 30-30, 30-40, 40-40 (D), AD-40, then next point tests.
                _point_row(point_num=1, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="15", p2_game_score="0"),
                _point_row(point_num=2, set_num=1, game_num=10, server="1", scorer="2", p1_game_score="15", p2_game_score="15"),
                _point_row(point_num=3, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="30", p2_game_score="15"),
                _point_row(point_num=4, set_num=1, game_num=10, server="1", scorer="2", p1_game_score="30", p2_game_score="30"),
                _point_row(point_num=5, set_num=1, game_num=10, server="1", scorer="2", p1_game_score="30", p2_game_score="40"),
                _point_row(point_num=6, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="40", p2_game_score="40"),
                _point_row(point_num=7, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="AD", p2_game_score="40"),
                # Point 8: pre-state AD-40 → set point (winning → 6-4).
                _point_row(point_num=8, set_num=1, game_num=10, server="1", scorer="1", p1_game_score="GAME", p2_game_score="40"),
            ]),
        )
        _write_matches(tmp_path, [_match_meta_row()])
        result = MatchBeatsPointsAggregator(data_root=tmp_path).run()
        row = result.filter((pl.col("game_num") == 10) & (pl.col("point_num") == 8)).to_dicts()[0]
        assert row["game_score_server"] == "AD"
        assert row["is_set_point"] is True
