"""Tests for OverviewTransformer — raw JSON to staged parquet."""

import json
from pathlib import Path

import polars as pl

from mvp.atptour.tournament import Tournament
from mvp.atptour.transformers.overview import OverviewTransformer
from mvp.common.enums import Circuit


def _overview_json(**overrides) -> dict:
    """Build a complete overview JSON matching raw data structure."""
    data = {
        "SponsorTitle": "Barcelona Open Banc Sabadell",
        "Bio": "Founded in 1953...",
        "SinglesDrawSize": 48,
        "DoublesDrawSize": 16,
        "Surface": "Clay",
        "Prize": "€2,722,480",
        "TotalFinancialCommitment": "€2,997,200",
        "Location": "Barcelona, Spain",
        "FlagUrl": "/-/media/images/flags/esp.svg",
        "Website": "barcelonaopenbancsabadell.com",
        "WebsiteUrl": "https://www.barcelonaopenbancsabadell.com",
        "InOutdoor": "O",
        "SurfaceSubCat": "Red Clay",
        "EventType": "500",
        "FbLink": "https://www.facebook.com/BarcelonaOpenBS",
        "TwLink": "https://twitter.com/bcnopenbs",
        "IgLink": "https://www.instagram.com/bcnopenbs",
        "VixletUrl": "",
        "EventTypeDetail": 0,
    }
    data.update(overrides)
    return data


def _make_tournament(**kwargs) -> Tournament:
    defaults = {
        "tournament_id": "404",
        "year": 2025,
        "circuit": Circuit.tour,
        "location": "Barcelona, Spain",
    }
    defaults.update(kwargs)
    return Tournament(**defaults)


def _write_overview(tmp_path: Path, tournament: Tournament, data: dict) -> None:
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path
    raw_dir.mkdir(parents=True, exist_ok=True)
    with (raw_dir / "overview.json").open("w", encoding="utf-8") as f:
        json.dump(data, f)


class TestTransformOverview:
    def test_produces_parquet(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json())
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        assert paths[0].exists()
        assert paths[0].name == "overview.parquet"

    def test_single_row(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json())
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_identity_fields(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json())
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["tournament_id"] == "404"
        assert row["year"] == 2025
        assert row["tournament_name"] == "Barcelona"
        assert row["city"] == "Barcelona"
        assert row["country"] == "Spain"
        assert row["circuit"] == "tour"

    def test_overview_fields(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json())
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["sponsor_title"] == "Barcelona Open Banc Sabadell"
        assert row["event_type"] == "500"
        assert row["event_type_detail"] == 0
        assert row["singles_draw_size"] == 48
        assert row["doubles_draw_size"] == 16
        assert row["surface"] == "Clay"
        assert row["surface_detail"] == "Red Clay"
        assert row["indoor"] is False
        assert row["prize"] == "€2,722,480"
        assert row["total_financial_commitment"] == "€2,997,200"
        assert row["location"] == "Barcelona, Spain"

    def test_marketing_fields_not_in_output(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json())
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        cols = set(df.columns)
        for excluded in ("bio", "flag_url", "website", "website_url",
                         "fb_link", "tw_link", "ig_link", "vixlet_url"):
            assert excluded not in cols

    def test_source_file_recorded(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json())
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert "overview.json" in row["source_file"]

    def test_grand_slam_name(self, tmp_path):
        t = _make_tournament(
            tournament_id="580",
            location="Melbourne, Australia",
        )
        _write_overview(tmp_path, t, _overview_json(
            Location="Melbourne, Australia",
            EventType="GS",
        ))
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["tournament_name"] == "Australian Open"
        assert row["city"] == "Melbourne"
        assert row["country"] == "Australia"


class TestUniquenessAssertion:
    def test_assertion_fires_on_duplicate_pk(self):
        import pytest

        df = pl.DataFrame({
            "tournament_id": ["404", "404"],
            "year": [2025, 2025],
        })
        with pytest.raises(ValueError, match="Duplicate primary keys"):
            OverviewTransformer.assert_unique(df, ["tournament_id", "year"], "overview")

    def test_assertion_passes_unique(self):
        df = pl.DataFrame({
            "tournament_id": ["404", "404"],
            "year": [2025, 2024],
        })
        OverviewTransformer.assert_unique(df, ["tournament_id", "year"], "overview")


class TestEdgeCases:
    def test_missing_file_returns_empty(self, tmp_path):
        t = _make_tournament()
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        assert paths == []

    def test_null_surface(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json(Surface=""))
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["surface"] is None

    def test_null_surface_detail(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json(SurfaceSubCat=None))
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["surface_detail"] is None

    def test_location_trailing_comma(self, tmp_path):
        t = _make_tournament(location="Fes, ")
        _write_overview(tmp_path, t, _overview_json(Location="Fes, "))
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["city"] == "Fes"
        assert row["country"] is None

    def test_indoor_event(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json(InOutdoor="I"))
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["indoor"] is True

    def test_empty_sponsor_title(self, tmp_path):
        t = _make_tournament()
        _write_overview(tmp_path, t, _overview_json(SponsorTitle=""))
        xf = OverviewTransformer(tournament=t, data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["sponsor_title"] is None
