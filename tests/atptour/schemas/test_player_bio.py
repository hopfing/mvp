"""Tests for PlayerBioRecord schema."""

from datetime import date, datetime

import pytest

from mvp.atptour.schemas.player_bio import PlayerBioRecord


class TestPlayerBioRecord:
    def test_minimal_valid(self):
        record = PlayerBioRecord(
            player_id="n409",
            first_name="Rafael",
            last_name="Nadal",
            is_active=False,
            is_dbl_specialist=False,
            source_file="players/n409.json",
            parsed_at=datetime(2026, 2, 24),
        )
        assert record.player_id == "N409"  # uppercased

    def test_full_record(self):
        record = PlayerBioRecord(
            player_id="s0ag",
            first_name="Carlos",
            last_name="Alcaraz",
            birth_date=date(2003, 5, 5),
            birth_city="El Palmar",
            nationality="esp",
            natl_id="e123",
            height_cm=183,
            weight_kg=74,
            right_handed="R",
            twohand_backhand="2",
            pro_year=2018,
            is_active="A",
            is_dbl_specialist=False,
            source_file="players/s0ag.json",
            parsed_at=datetime(2026, 2, 24),
        )
        assert record.right_handed is True
        assert record.twohand_backhand is True
        assert record.is_active is True
        assert record.nationality == "ESP"

    def test_right_handed_left(self):
        r = PlayerBioRecord(
            player_id="n409",
            first_name="Rafael",
            last_name="Nadal",
            right_handed="L",
            is_active=False,
            is_dbl_specialist=False,
            source_file="test",
            parsed_at=datetime(2026, 1, 1),
        )
        assert r.right_handed is False

    def test_right_handed_unknown(self):
        for v in (None, "", "U", "A"):
            r = PlayerBioRecord(
                player_id="n409",
                first_name="Rafael",
                last_name="Nadal",
                right_handed=v,
                is_active=False,
                is_dbl_specialist=False,
                source_file="test",
                parsed_at=datetime(2026, 1, 1),
            )
            assert r.right_handed is None

    def test_twohand_backhand_values(self):
        cases = [
            ("2", True),
            ("1", False),
            ("0", None),
            ("U", None),
            (None, None),
            ("", None),
        ]
        for raw, expected in cases:
            r = PlayerBioRecord(
                player_id="x",
                first_name="A",
                last_name="B",
                twohand_backhand=raw,
                is_active=False,
                is_dbl_specialist=False,
                source_file="test",
                parsed_at=datetime(2026, 1, 1),
            )
            assert r.twohand_backhand is expected, f"Failed for {raw!r}"

    def test_is_active_parsing(self):
        cases = [("A", True), ("I", False), ("D", False), (True, True), (False, False)]
        for raw, expected in cases:
            r = PlayerBioRecord(
                player_id="x",
                first_name="A",
                last_name="B",
                is_active=raw,
                is_dbl_specialist=False,
                source_file="test",
                parsed_at=datetime(2026, 1, 1),
            )
            assert r.is_active is expected, f"Failed for {raw!r}"

    def test_strip_birth_city(self):
        r = PlayerBioRecord(
            player_id="x",
            first_name="A",
            last_name="B",
            birth_city="  El Palmar  ",
            is_active=False,
            is_dbl_specialist=False,
            source_file="test",
            parsed_at=datetime(2026, 1, 1),
        )
        assert r.birth_city == "El Palmar"

    def test_empty_birth_city(self):
        r = PlayerBioRecord(
            player_id="x",
            first_name="A",
            last_name="B",
            birth_city="",
            is_active=False,
            is_dbl_specialist=False,
            source_file="test",
            parsed_at=datetime(2026, 1, 1),
        )
        assert r.birth_city is None

    def test_schema_version_and_hash(self):
        assert PlayerBioRecord.SCHEMA_VERSION == "1.0.0"
        assert hasattr(PlayerBioRecord, "SCHEMA_HASH")
        assert isinstance(PlayerBioRecord.SCHEMA_HASH, str)

    def test_unexpected_right_handed_raises(self):
        with pytest.raises(ValueError):
            PlayerBioRecord(
                player_id="x",
                first_name="A",
                last_name="B",
                right_handed="X",
                is_active=False,
                is_dbl_specialist=False,
                source_file="test",
                parsed_at=datetime(2026, 1, 1),
            )
