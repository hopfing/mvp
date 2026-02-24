"""Transform raw rankings HTML into staged parquet via RankingsRecord schema."""

import datetime as dt
import logging
from datetime import date
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

from mvp.atptour.schemas.rankings import RankingsRecord
from mvp.common.base_job import BaseJob
from mvp.common.utils import polars_schema_overrides

logger = logging.getLogger(__name__)


def _parse_date_from_stem(stem: str) -> date:
    """Parse YYYYMMDD from filename stem like 'rankings_singles_20260216'."""
    d = stem.replace("rankings_singles_", "")
    return date(int(d[:4]), int(d[4:6]), int(d[6:8]))


def _dash_to_none(text: str) -> int | None:
    """Parse an integer from text, treating '-' as None."""
    text = text.strip()
    if text == "-":
        return None
    return int(text.replace(",", ""))


class RankingsTransformer(BaseJob):
    """Parse rankings HTML pages into per-date parquet files."""

    def __init__(self, data_root: Path | None = None):
        super().__init__(domain="atptour", data_root=data_root)

    def run(self, start_year: int | None = None) -> list[Path]:
        """Process rankings HTML files into per-date parquets.

        Args:
            start_year: If provided, only process files with year >= start_year.

        Returns list of parquet paths written.
        """
        raw_dir = self.build_path("raw", "rankings")
        html_files = self.list_files(raw_dir, "rankings_singles_*.html")
        if not html_files:
            logger.info("No rankings HTML files to transform")
            return []

        if start_year is not None:
            html_files = [
                f
                for f in html_files
                if _parse_date_from_stem(f.stem).year >= start_year
            ]

        stage_dir = self.build_path("stage", "rankings")
        existing = {
            p.stem
            for p in self.list_files(stage_dir, "*.parquet")
            if p.stem != "rankings_singles"
        }
        to_process = [f for f in html_files if f.stem not in existing]

        if not to_process:
            logger.info(
                "Rankings transform: all %d files already staged",
                len(html_files),
            )
            return []

        parsed_at = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        paths: list[Path] = []

        for html_path in to_process:
            ranking_date = _parse_date_from_stem(html_path.stem)
            html = self.read_html(html_path)
            source_file = str(self._display_path(html_path))

            records = self._parse_rankings_page(
                html, ranking_date, source_file, parsed_at
            )
            if not records:
                continue

            rows = [r.model_dump() for r in records]
            overrides = polars_schema_overrides(RankingsRecord)
            df = pl.DataFrame(rows, schema_overrides=overrides)

            out_path = self.build_path(
                "stage", "rankings", f"{html_path.stem}.parquet"
            )
            result = self.save_parquet(df, out_path)
            if result is not None:
                paths.append(result)

        logger.info(
            "Rankings transform: %d to process (%d skipped), wrote %d parquets",
            len(to_process),
            len(html_files) - len(to_process),
            len(paths),
        )
        return paths

    def consolidate(self) -> Path | None:
        """Merge all per-date parquets into a single rankings_singles.parquet.

        Returns the consolidated file path, or None if no files to merge.
        """
        stage_dir = self.build_path("stage", "rankings")
        parquet_files = self.list_files(stage_dir, "rankings_singles_*.parquet")

        # Exclude any existing consolidated file
        parquet_files = [
            f for f in parquet_files if f.stem != "rankings_singles"
        ]

        if not parquet_files:
            logger.info("No per-date parquets to consolidate")
            return None

        dfs = [pl.read_parquet(f) for f in parquet_files]
        combined = pl.concat(dfs)

        out_path = self.build_path("stage", "rankings", "rankings_singles.parquet")
        result = self.save_parquet(combined, out_path)

        logger.info(
            "Rankings consolidate: merged %d files, %d total rows",
            len(parquet_files),
            len(combined),
        )
        return result

    def _parse_rankings_page(
        self,
        html: str,
        ranking_date: date,
        source_file: str,
        parsed_at: dt.datetime,
    ) -> list[RankingsRecord]:
        """Parse one rankings HTML page into validated records."""
        soup = BeautifulSoup(html, "lxml")
        table = soup.select_one("table.mega-table.desktop-table.non-live")
        if table is None:
            logger.warning(
                "No desktop table in rankings HTML for %s — skipping",
                ranking_date,
            )
            return []

        rows = table.select("tbody tr")
        records = []
        for tr in rows:
            rank_td = tr.select_one("td.rank")
            if rank_td is None:
                continue

            rank_text = rank_td.get_text(strip=True)
            rank = int(rank_text.rstrip("T"))

            player_cell = tr.select_one("td.player")
            link = player_cell.select_one("li.name a")
            href = link["href"]
            player_id = href.split("/")[-2]
            player_name = link.get_text(strip=True)

            flag_svg = player_cell.select_one("svg use")
            flag_href = flag_svg["href"]
            nationality = flag_href.split("#flag-")[-1]

            age = int(tr.select_one("td.age").get_text(strip=True))

            points_text = tr.select_one("td.points").get_text(strip=True)
            points = int(points_text.replace(",", ""))

            rank_li = player_cell.select_one("li.rank")
            rank_up = rank_li.select_one("span.rank-up")
            rank_down = rank_li.select_one("span.rank-down")
            if rank_up:
                rank_move = int(rank_up.get_text(strip=True))
            elif rank_down:
                rank_move = -int(rank_down.get_text(strip=True))
            else:
                rank_move = None

            points_move = _dash_to_none(
                tr.select_one("td.pointsMove").get_text(strip=True)
            )
            tournaments_played = int(
                tr.select_one("td.tourns").get_text(strip=True)
            )
            points_dropping = _dash_to_none(
                tr.select_one("td.drop").get_text(strip=True)
            )
            next_best = _dash_to_none(
                tr.select_one("td.best").get_text(strip=True)
            )

            records.append(
                RankingsRecord(
                    ranking_date=ranking_date,
                    rank=rank,
                    player_id=player_id,
                    player_name=player_name,
                    nationality=nationality,
                    age=age,
                    points=points,
                    rank_move=rank_move,
                    points_move=points_move,
                    tournaments_played=tournaments_played,
                    points_dropping=points_dropping,
                    next_best=next_best,
                    source_file=source_file,
                    parsed_at=parsed_at,
                )
            )

        return records
