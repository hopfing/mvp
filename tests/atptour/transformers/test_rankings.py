"""Tests for RankingsTransformer — raw HTML to staged parquet."""

from datetime import date
from pathlib import Path

import polars as pl

from mvp.atptour.transformers.rankings import (
    RankingsTransformer,
    _dash_to_none,
    _parse_date_from_stem,
)

# --- Helper tests ---


class TestParseDateFromStem:
    def test_basic(self):
        assert _parse_date_from_stem("rankings_singles_20260216") == date(2026, 2, 16)

    def test_early_date(self):
        assert _parse_date_from_stem("rankings_singles_20060102") == date(2006, 1, 2)


class TestDashToNone:
    def test_dash_returns_none(self):
        assert _dash_to_none("-") is None

    def test_integer(self):
        assert _dash_to_none("100") == 100

    def test_comma_separated(self):
        assert _dash_to_none("13,150") == 13150

    def test_whitespace(self):
        assert _dash_to_none("  - ") is None

    def test_whitespace_number(self):
        assert _dash_to_none(" 250 ") == 250


# --- Sample HTML snippets ---

DESKTOP_TABLE_HEADER = """\
<table class="mega-table desktop-table non-live">
<thead><tr class="header-row"><th>Rank</th></tr></thead>
<tbody>
"""

DESKTOP_TABLE_FOOTER = "</tbody></table>"


def _row_html(
    rank="1",
    name="Carlos Alcaraz",
    player_href="/en/players/carlos-alcaraz/a0e2/overview",
    flag_code="esp",
    age="22",
    points="13,150",
    points_move="-",
    tourns="18",
    drop="100",
    best="-",
    rank_move_html="",
) -> str:
    return f"""\
<tr class="lower-row">
    <td class="rank bold heavy tiny-cell" colspan="1">{rank}</td>
    <td class="player bold heavy large-cell" colspan="7">
        <ul class="player-stats">
            <li class="rank">{rank_move_html}</li>
            <li class="avatar">
                <svg class="atp-flag flag"><use
                    href="/assets/atptour/assets/flags.svg#flag-{flag_code}"
                /></svg>
            </li>
            <li class="name center">
                <a href="{player_href}"><span>{name}</span></a>
            </li>
        </ul>
    </td>
    <td class="age small-cell" colspan="2">{age}</td>
    <td class="points center bold extrabold small-cell" colspan="2">
        <a href="/en/players/carlos-alcaraz/a0e2/rankings-breakdown?team=singles">
            {points}
        </a>
    </td>
    <td class="small-cell pointsMove center" colspan="2">{points_move}</td>
    <td class="tourns center small-cell" colspan="2">{tourns}</td>
    <td class="drop center small-cell" colspan="2">{drop}</td>
    <td class="best center small-cell" colspan="2">{best}</td>
</tr>"""


def _make_html(*rows) -> str:
    """Wrap row HTML in a full desktop table."""
    return DESKTOP_TABLE_HEADER + "\n".join(rows) + DESKTOP_TABLE_FOOTER


def _write_rankings_html(tmp_path: Path, filename: str, html: str) -> None:
    raw_dir = tmp_path / "raw" / "atptour" / "rankings"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / filename).write_text(html, encoding="utf-8")


# --- Transformer tests ---


class TestTransformRankings:
    def test_single_row(self, tmp_path):
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 1
        df = pl.read_parquet(paths[0])
        assert len(df) == 1

    def test_field_values(self, tmp_path):
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        row = df.row(0, named=True)
        assert row["ranking_date"] == date(2026, 2, 16)
        assert row["rank"] == 1
        assert row["player_id"] == "A0E2"
        assert row["player_name"] == "Carlos Alcaraz"
        assert row["nationality"] == "ESP"
        assert row["age"] == 22
        assert row["points"] == 13150
        assert row["rank_move"] is None
        assert row["points_move"] is None
        assert row["tournaments_played"] == 18
        assert row["points_dropping"] == 100
        assert row["next_best"] is None

    def test_multiple_rows(self, tmp_path):
        html = _make_html(
            _row_html(
                rank="1",
                name="Carlos Alcaraz",
                player_href="/en/players/carlos-alcaraz/a0e2/overview",
            ),
            _row_html(
                rank="2",
                name="Jannik Sinner",
                player_href="/en/players/jannik-sinner/s0ag/overview",
                flag_code="ita",
                age="24",
                points="10,300",
            ),
        )
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 2

    def test_multiple_files(self, tmp_path):
        html1 = _make_html(_row_html())
        html2 = _make_html(_row_html(rank="1", points="12,000"))
        _write_rankings_html(tmp_path, "rankings_singles_20260209.html", html1)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html2)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        assert len(paths) == 2

    def test_tied_rank(self, tmp_path):
        html = _make_html(_row_html(rank="15T"))
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["rank"] == 15

    def test_rank_move_up(self, tmp_path):
        html = _make_html(
            _row_html(
                rank_move_html='<span class="rank-up">3</span>',
            )
        )
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["rank_move"] == 3

    def test_rank_move_down(self, tmp_path):
        html = _make_html(
            _row_html(
                rank_move_html='<span class="rank-down">2</span>',
            )
        )
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["rank_move"] == -2

    def test_rank_move_stable(self, tmp_path):
        html = _make_html(_row_html(rank_move_html=""))
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert df.row(0, named=True)["rank_move"] is None

    def test_source_file_recorded(self, tmp_path):
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert "rankings_singles_20260216.html" in df.row(0, named=True)["source_file"]

    def test_no_files_returns_empty(self, tmp_path):
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        assert paths == []


class TestMissingDesktopTable:
    def test_missing_table_returns_empty(self, tmp_path):
        html = "<html><body><p>No table here</p></body></html>"
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        assert paths == []


class TestAdRowSkipping:
    def test_ad_row_skipped(self, tmp_path):
        ad_row = (
            '<tr class="lower-row">'
            '<td class="ad-cell" colspan="20">Ad content</td>'
            "</tr>"
        )
        html = _make_html(_row_html(), ad_row)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        paths = xf.run()
        df = pl.read_parquet(paths[0])
        assert len(df) == 1


class TestConsolidate:
    def test_merges_multiple_dates(self, tmp_path):
        html1 = _make_html(_row_html(points="13,150"))
        html2 = _make_html(_row_html(points="12,000"))
        _write_rankings_html(tmp_path, "rankings_singles_20260209.html", html1)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html2)
        xf = RankingsTransformer(data_root=tmp_path)
        xf.run()
        result = xf.consolidate()
        assert result is not None
        df = pl.read_parquet(result)
        assert len(df) == 2
        assert result.name == "rankings_singles.parquet"

    def test_no_parquets_returns_none(self, tmp_path):
        xf = RankingsTransformer(data_root=tmp_path)
        result = xf.consolidate()
        assert result is None

    def test_excludes_consolidated_file_from_input(self, tmp_path):
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        xf.run()
        # Run consolidate twice — should not double-count
        xf.consolidate()
        result = xf.consolidate()
        df = pl.read_parquet(result)
        assert len(df) == 1


class TestSkipLogic:
    def test_skips_already_staged(self, tmp_path):
        """Files with existing staged parquets are not reprocessed."""
        html1 = _make_html(_row_html(points="13,150"))
        html2 = _make_html(_row_html(points="12,000"))
        _write_rankings_html(tmp_path, "rankings_singles_20260209.html", html1)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html2)
        xf = RankingsTransformer(data_root=tmp_path)

        # First run: both files processed
        paths = xf.run()
        assert len(paths) == 2

        # Second run: both already staged, nothing to process
        paths = xf.run()
        assert len(paths) == 0

    def test_processes_only_new_files(self, tmp_path):
        """Only unstaged files are processed on incremental runs."""
        html1 = _make_html(_row_html(points="13,150"))
        _write_rankings_html(tmp_path, "rankings_singles_20260209.html", html1)
        xf = RankingsTransformer(data_root=tmp_path)

        # First run: one file
        paths = xf.run()
        assert len(paths) == 1

        # Add a second raw file
        html2 = _make_html(_row_html(points="12,000"))
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html2)

        # Second run: only the new file
        paths = xf.run()
        assert len(paths) == 1
        assert paths[0].stem == "rankings_singles_20260216"

    def test_consolidated_file_not_counted_as_existing(self, tmp_path):
        """The merged rankings_singles.parquet should not block processing."""
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)
        xf.run()
        xf.consolidate()

        # Add a new raw file
        html2 = _make_html(_row_html(points="12,000"))
        _write_rankings_html(tmp_path, "rankings_singles_20260209.html", html2)

        # Run again — should process the new file (not blocked by consolidated parquet)
        paths = xf.run()
        assert len(paths) == 1
        assert paths[0].stem == "rankings_singles_20260209"


class TestUniquenessAssertion:
    def test_assertion_fires_on_duplicate_pk(self):
        import pytest

        df = pl.DataFrame({
            "player_id": ["A0E2", "A0E2"],
        })
        with pytest.raises(ValueError, match="Duplicate primary keys"):
            RankingsTransformer.assert_unique(df, ["player_id"], "rankings")

    def test_consolidated_assertion_fires(self):
        import pytest

        df = pl.DataFrame({
            "ranking_date": [date(2026, 2, 16), date(2026, 2, 16)],
            "player_id": ["A0E2", "A0E2"],
        })
        with pytest.raises(ValueError, match="Duplicate primary keys"):
            RankingsTransformer.assert_unique(
                df, ["ranking_date", "player_id"], "rankings"
            )


class TestStartYearFilter:
    def test_filters_by_start_year(self, tmp_path):
        """Only HTML files with year >= start_year are processed."""
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20240106.html", html)
        _write_rankings_html(tmp_path, "rankings_singles_20250106.html", html)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)

        paths = xf.run(start_year=2025)
        assert len(paths) == 2
        stems = {p.stem for p in paths}
        assert "rankings_singles_20250106" in stems
        assert "rankings_singles_20260216" in stems
        assert "rankings_singles_20240106" not in stems

    def test_no_start_year_processes_all(self, tmp_path):
        """Without start_year, all files are processed."""
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20240106.html", html)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)

        paths = xf.run()
        assert len(paths) == 2

    def test_start_year_combined_with_skip(self, tmp_path):
        """start_year and skip logic work together."""
        html = _make_html(_row_html())
        _write_rankings_html(tmp_path, "rankings_singles_20240106.html", html)
        _write_rankings_html(tmp_path, "rankings_singles_20250106.html", html)
        _write_rankings_html(tmp_path, "rankings_singles_20260216.html", html)
        xf = RankingsTransformer(data_root=tmp_path)

        # First run with start_year=2025: processes 2025 and 2026
        paths = xf.run(start_year=2025)
        assert len(paths) == 2

        # Second run with start_year=2025: both already staged
        paths = xf.run(start_year=2025)
        assert len(paths) == 0
