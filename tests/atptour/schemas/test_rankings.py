"""Tests for Rankings staged schema."""

from datetime import date, datetime

from mvp.atptour.schemas.rankings import (
    SCHEMA_HASH,
    RankingsRecord,
)

PARSED_AT = datetime(2026, 2, 24)
SOURCE_FILE = "rankings/rankings_singles_20260216.html"


def _base_ranking(**overrides) -> dict:
    """Minimal valid rankings record."""
    data = {
        "ranking_date": date(2026, 2, 16),
        "rank": 1,
        "player_id": "a0e2",
        "player_name": "Carlos Alcaraz",
        "nationality": "esp",
        "age": 22,
        "points": 13150,
        "rank_move": None,
        "points_move": None,
        "tournaments_played": 18,
        "points_dropping": 100,
        "next_best": None,
        "source_file": SOURCE_FILE,
        "parsed_at": PARSED_AT,
    }
    data.update(overrides)
    return data


class TestValidRecords:
    def test_basic_record(self):
        record = RankingsRecord(**_base_ranking())
        assert record.rank == 1
        assert record.player_name == "Carlos Alcaraz"
        assert record.points == 13150

    def test_all_nullable_fields_none(self):
        record = RankingsRecord(**_base_ranking(
            rank_move=None,
            points_move=None,
            points_dropping=None,
            next_best=None,
        ))
        assert record.rank_move is None
        assert record.points_move is None
        assert record.points_dropping is None
        assert record.next_best is None

    def test_all_nullable_fields_populated(self):
        record = RankingsRecord(**_base_ranking(
            rank_move=3,
            points_move=150,
            points_dropping=200,
            next_best=50,
        ))
        assert record.rank_move == 3
        assert record.points_move == 150
        assert record.points_dropping == 200
        assert record.next_best == 50

    def test_negative_rank_move(self):
        record = RankingsRecord(**_base_ranking(rank_move=-5))
        assert record.rank_move == -5


class TestFieldValidation:
    def test_player_id_uppercased(self):
        record = RankingsRecord(**_base_ranking(player_id="a0e2"))
        assert record.player_id == "A0E2"

    def test_nationality_uppercased(self):
        record = RankingsRecord(**_base_ranking(nationality="esp"))
        assert record.nationality == "ESP"

    def test_player_id_already_upper(self):
        record = RankingsRecord(**_base_ranking(player_id="A0E2"))
        assert record.player_id == "A0E2"


class TestFieldCount:
    def test_field_count(self):
        assert len(RankingsRecord.model_fields) == 14


class TestSchemaHash:
    def test_schema_hash_is_hex_string(self):
        assert len(SCHEMA_HASH) == 16
        int(SCHEMA_HASH, 16)
