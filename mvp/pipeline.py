import argparse
from datetime import date, datetime, timedelta
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from config import LEAGUE_MONTHS, PROJECT_ROOT
from mvp.action_network.extractor import ActionNetworkExtractor
from mvp.action_network.stager import ActionNetworkStager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


def valid_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{s} is not a valid date (YYYY-MM-DD)"
        )


def expected_leagues(month: int) -> list[str]:

    leagues = []

    for league, months in LEAGUE_MONTHS.items():
        if month in months:
            leagues.append(league)

    return leagues


def pretty_paths(paths) -> str:
    lines = (p.relative_to(PROJECT_ROOT).as_posix() for p in paths)
    return "\n    - " + "\n    - ".join(lines)


def parse_args():

    parser = argparse.ArgumentParser(
        description="Run Project MVP ETL Pipeline."
    )

    parser.add_argument(
        "--league",
        required=False,
        help="League abbreviation (case-insensitive), e.g. MLB, NBA, etc."
    )

    parser.add_argument(
        "--start_date",
        required=False,
        type=valid_date,
        help="Start of date range in YYYY-MM-DD format."
    )

    parser.add_argument(
        "--end_date",
        required=False,
        type=valid_date,
        help="End of date range (inclusive) in YYYY-MM-DD format. "
             "Defaults to start if not provided`."
    )

    args = parser.parse_args()

    if args.start_date is None:
        args.start_date = datetime.now(ZoneInfo("America/Chicago")).date()
    if args.end_date is None:
        args.end_date = args.start_date

    return args


def main():
    logger.info("Starting Project MVP ETL pipeline.")
    args = parse_args()

    dates = [
        args.start_date + timedelta(days=i)
        for i in range((args.end_date - args.start_date).days + 1)
    ]

    logger.info(
        "Processing %s date(s) beginning %s",
        len(dates), args.start_date.strftime("%Y-%m-%d")
    )

    for game_date in dates:
        date_str = game_date.strftime("%Y-%m-%d")
        leagues = expected_leagues(game_date.month)
        logger.info("Processing date: %s", date_str)
        logger.info("Expected leagues: %s", ", ".join(leagues))
        for league in leagues:
            logger.info("Running ActionNetwork extractor for league: %s",
                        league.upper())
            extractor = ActionNetworkExtractor(
                league=league,
                game_date=date_str
            )
            raw_manifest = extractor.run()
            raw_files = [
                Path(item["uri"]) for item in raw_manifest.get("items", [])
            ]
            logger.info(
                "%s file(s) retrieved for %s: %s",
                len(raw_files), extractor.league.upper(),
                pretty_paths(raw_files)
            )
            stager = ActionNetworkStager(
                league=league,
                game_date=date_str
            )
            stager.run(manifest=raw_manifest)


if __name__ == "__main__":
    main()
