"""Parse results HTML from atptour.com into raw match dicts.

Standalone parser with no schema or BaseJob dependencies. Returns lists of
plain dicts; schema validation is the transformer's responsibility.
"""

import logging
import re
from datetime import date

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_DAY_SUFFIX_RE = re.compile(r"\s+Day\s+\d+$", re.IGNORECASE)

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Same-month: "2-8 May, 2022" or single day "8 May, 2022"
_DATE_RANGE_SAME_MONTH_RE = re.compile(
    r"(\d{1,2})(?:\s*-\s*(\d{1,2}))?\s+([A-Za-z]{3}),?\s+(\d{4})"
)

# Cross-month: "18 Jan - 1 Feb, 2026" or "28 Dec - 3 Jan, 2022"
_DATE_RANGE_CROSS_MONTH_RE = re.compile(
    r"(\d{1,2})\s+([A-Za-z]{3})\s*-\s*(\d{1,2})\s+([A-Za-z]{3}),?\s+(\d{4})"
)


class ResultsParser:
    """Parse results HTML (singles and doubles) from atptour.com."""

    def parse_singles(self, html: str) -> list[dict]:
        """Parse singles results HTML into raw match dicts.

        Returns one dict per match with keys: round_text, match_id,
        duration_text, player_id, player_name, player_seed_entry,
        player_country, player_won, opp_id, opp_name, opp_seed_entry,
        opp_country, player_scores, opp_scores, player_tiebreaks,
        opp_tiebreaks, result_type, tournament_start_date, tournament_end_date.
        """
        soup = BeautifulSoup(html, "lxml")
        start_date, end_date = self._parse_tournament_dates(soup)
        matches = []

        for match_div in soup.find_all("div", class_="match"):
            header = match_div.find("div", class_="match-header")
            if not header:
                logger.warning("Skipping match div: missing match-header")
                continue

            strong = header.find("strong")
            if not strong:
                logger.warning("Skipping match div: missing strong tag in header")
                continue

            round_text = self._parse_round_text(strong)
            duration_text = self._parse_duration_text(header)

            stats_items = match_div.find_all("div", class_="stats-item")
            if len(stats_items) < 2:
                logger.warning(
                    "Skipping match div: fewer than 2 stats-items (round=%s)",
                    round_text,
                )
                continue

            player = self._parse_player(stats_items[0])
            opp = self._parse_player(stats_items[1])
            if player is None:
                logger.warning(
                    "Skipping match div: missing player link (round=%s)", round_text
                )
                continue
            if opp is None:
                logger.warning(
                    "Skipping match div: missing opponent link (round=%s)", round_text
                )
                continue

            player_scores, player_raw_tb = self._parse_scores(stats_items[0])
            opp_scores, opp_raw_tb = self._parse_scores(stats_items[1])

            player_tiebreaks, opp_tiebreaks = self._assign_tiebreaks(
                player_scores, player_raw_tb, opp_scores, opp_raw_tb
            )

            footer = match_div.find("div", class_="match-footer")
            match_id = self._parse_match_id(footer) if footer else None

            result_type = self._derive_result_type(player_scores, opp_scores)

            matches.append(
                {
                    "round_text": round_text,
                    "match_id": match_id,
                    "duration_text": duration_text,
                    "player_id": player["id"],
                    "player_name": player["name"],
                    "player_seed_entry": player["seed_entry"],
                    "player_country": player["country"],
                    "player_won": player["is_winner"],
                    "opp_id": opp["id"],
                    "opp_name": opp["name"],
                    "opp_seed_entry": opp["seed_entry"],
                    "opp_country": opp["country"],
                    "player_scores": player_scores,
                    "opp_scores": opp_scores,
                    "player_tiebreaks": player_tiebreaks,
                    "opp_tiebreaks": opp_tiebreaks,
                    "result_type": result_type,
                    "tournament_start_date": start_date,
                    "tournament_end_date": end_date,
                }
            )

        return matches

    def parse_doubles(self, html: str) -> list[dict]:
        """Parse doubles results HTML into raw match dicts.

        Returns one dict per match. Same keys as parse_singles plus:
        partner_id, partner_name, partner_country, opp_partner_id,
        opp_partner_name, opp_partner_country.
        """
        soup = BeautifulSoup(html, "lxml")
        start_date, end_date = self._parse_tournament_dates(soup)
        matches = []

        for match_div in soup.find_all("div", class_="match"):
            header = match_div.find("div", class_="match-header")
            if not header:
                logger.warning("Skipping match div: missing match-header")
                continue

            strong = header.find("strong")
            if not strong:
                logger.warning("Skipping match div: missing strong tag in header")
                continue

            round_text = self._parse_round_text(strong)
            duration_text = self._parse_duration_text(header)

            stats_items = match_div.find_all("div", class_="stats-item")
            if len(stats_items) < 2:
                logger.warning(
                    "Skipping match div: fewer than 2 stats-items (round=%s)",
                    round_text,
                )
                continue

            team1 = self._parse_team(stats_items[0])
            team2 = self._parse_team(stats_items[1])
            if team1 is None:
                logger.warning(
                    "Skipping match div: missing team1 data (round=%s)", round_text
                )
                continue
            if team2 is None:
                logger.warning(
                    "Skipping match div: missing team2 data (round=%s)", round_text
                )
                continue

            player_scores, player_raw_tb = self._parse_scores(stats_items[0])
            opp_scores, opp_raw_tb = self._parse_scores(stats_items[1])

            player_tiebreaks, opp_tiebreaks = self._assign_tiebreaks(
                player_scores, player_raw_tb, opp_scores, opp_raw_tb
            )

            footer = match_div.find("div", class_="match-footer")
            match_id = self._parse_match_id(footer) if footer else None

            result_type = self._derive_result_type(player_scores, opp_scores)

            matches.append(
                {
                    "round_text": round_text,
                    "match_id": match_id,
                    "duration_text": duration_text,
                    "player_id": team1["players"][0]["id"],
                    "player_name": team1["players"][0]["name"],
                    "partner_id": team1["players"][1]["id"],
                    "partner_name": team1["players"][1]["name"],
                    "player_country": team1["players"][0]["country"],
                    "partner_country": team1["players"][1]["country"],
                    "player_seed_entry": team1["seed_entry"],
                    "player_won": team1["is_winner"],
                    "opp_id": team2["players"][0]["id"],
                    "opp_name": team2["players"][0]["name"],
                    "opp_partner_id": team2["players"][1]["id"],
                    "opp_partner_name": team2["players"][1]["name"],
                    "opp_country": team2["players"][0]["country"],
                    "opp_partner_country": team2["players"][1]["country"],
                    "opp_seed_entry": team2["seed_entry"],
                    "player_scores": player_scores,
                    "opp_scores": opp_scores,
                    "player_tiebreaks": player_tiebreaks,
                    "opp_tiebreaks": opp_tiebreaks,
                    "result_type": result_type,
                    "tournament_start_date": start_date,
                    "tournament_end_date": end_date,
                }
            )

        return matches

    def _parse_tournament_dates(
        self, soup: BeautifulSoup
    ) -> tuple[date | None, date | None]:
        """Extract tournament start and end dates from date-location div.

        Parses formats:
        - Same-month: "2-8 May, 2022" or "8 May, 2022"
        - Cross-month: "18 Jan - 1 Feb, 2026"
        - Cross-year: "28 Dec - 3 Jan, 2022" (year applies to end date)

        Returns (start_date, end_date), either may be None if parsing fails.

        Note: Some pages have template placeholders ({{tournament.FormattedDate}})
        in the first date-location div; we skip those and look for real data.
        """
        for date_loc in soup.find_all("div", class_="date-location"):
            spans = date_loc.find_all("span")
            if len(spans) < 2:
                continue

            date_text = spans[1].get_text(strip=True)
            if not date_text or "{{" in date_text:
                continue

            # Try cross-month pattern first (more specific)
            match = _DATE_RANGE_CROSS_MONTH_RE.search(date_text)
            if match:
                start_day = int(match.group(1))
                start_month_str = match.group(2).lower()
                end_day = int(match.group(3))
                end_month_str = match.group(4).lower()
                year = int(match.group(5))

                start_month = _MONTH_MAP.get(start_month_str)
                end_month = _MONTH_MAP.get(end_month_str)
                if start_month is None or end_month is None:
                    logger.warning("Unknown month in tournament date: %s", date_text)
                    continue

                try:
                    # Year in text applies to end date; start may be previous year
                    start_year = year - 1 if start_month > end_month else year
                    start_date = date(start_year, start_month, start_day)
                    end_date = date(year, end_month, end_day)
                    return start_date, end_date
                except ValueError as e:
                    logger.warning("Invalid tournament date: %s (%s)", date_text, e)
                    continue

            # Try same-month pattern
            match = _DATE_RANGE_SAME_MONTH_RE.search(date_text)
            if match:
                start_day = int(match.group(1))
                end_day = int(match.group(2)) if match.group(2) else start_day
                month_str = match.group(3).lower()
                year = int(match.group(4))

                month = _MONTH_MAP.get(month_str)
                if month is None:
                    logger.warning("Unknown month in tournament date: %s", month_str)
                    continue

                try:
                    start_date = date(year, month, start_day)
                    end_date = date(year, month, end_day)
                    return start_date, end_date
                except ValueError as e:
                    logger.warning("Invalid tournament date: %s (%s)", date_text, e)
                    continue

            logger.warning("Could not parse tournament date: %s", date_text)

        return None, None

    @staticmethod
    def _parse_round_text(strong_tag) -> str:
        """Extract round name from the strong tag in a match header.

        Strips " - Venue" suffixes, trailing dashes, and "Day N" suffixes.
        """
        text = strong_tag.get_text(strip=True)
        text = text.split(" - ")[0].strip().rstrip("-").strip()
        text = _DAY_SUFFIX_RE.sub("", text)
        return text

    @staticmethod
    def _parse_player(stats_item_div) -> dict | None:
        """Parse player info from a singles stats-item div.

        Returns None if the player has no ATP profile link (e.g., WTA
        players in mixed-team events like the United Cup).
        """
        player_info = stats_item_div.find("div", class_="player-info")
        if player_info is None:
            return None

        name_div = player_info.find("div", class_="name")
        if name_div is None:
            return None
        a_tag = name_div.find("a")
        if a_tag is None:
            return None
        name = a_tag.get_text(strip=True)

        img = player_info.find("img", class_="player-image")
        alt = img["alt"]
        player_id = alt.replace("Player-Photo-", "")

        seed_span = name_div.find("span")
        seed_entry = seed_span.get_text(strip=True) if seed_span else ""

        country = ""
        use_tag = player_info.find("use")
        if use_tag:
            href = use_tag.get("href", "")
            if "#flag-" in href:
                country = href.split("#flag-")[1]

        is_winner = player_info.find("div", class_="winner") is not None

        return {
            "id": player_id,
            "name": name,
            "seed_entry": seed_entry,
            "country": country,
            "is_winner": is_winner,
        }

    @staticmethod
    def _parse_team(stats_item_div) -> dict | None:
        """Parse team info from a doubles stats-item div.

        Returns None if no profiles div, fewer than 2 player images,
        or fewer than 2 player links.
        """
        player_info = stats_item_div.find("div", class_="player-info")
        if player_info is None:
            return None

        profiles = player_info.find("div", class_="profiles")
        if profiles is None:
            return None

        imgs = profiles.find_all("img", class_="player-image")
        if len(imgs) < 2:
            return None
        ids = [img["alt"].replace("Player-Photo-", "") for img in imgs[:2]]

        names_div = player_info.find("div", class_="names")
        if names_div is None:
            return None
        a_tags = names_div.find_all("a")
        if len(a_tags) < 2:
            return None
        names = [a.get_text(strip=True) for a in a_tags[:2]]

        countries = []
        countries_div = player_info.find("div", class_="countries")
        if countries_div:
            for country_div in countries_div.find_all("div", class_="country"):
                use_tag = country_div.find("use")
                if use_tag:
                    href = use_tag.get("href", "")
                    if "#flag-" in href:
                        countries.append(href.split("#flag-")[1])
                    else:
                        countries.append("")
                else:
                    countries.append("")
        while len(countries) < 2:
            countries.append("")

        name_divs = names_div.find_all("div", class_="name")
        seed_entry = ""
        if name_divs:
            seed_span = name_divs[0].find("span")
            seed_entry = seed_span.get_text(strip=True) if seed_span else ""

        is_winner = player_info.find("div", class_="winner") is not None

        return {
            "players": [
                {"id": ids[0], "name": names[0], "country": countries[0]},
                {"id": ids[1], "name": names[1], "country": countries[1]},
            ],
            "seed_entry": seed_entry,
            "is_winner": is_winner,
        }

    @staticmethod
    def _parse_scores(stats_item_div) -> tuple[list[int], list[int | None]]:
        """Parse score-items into (games_per_set, raw_tiebreak_per_set).

        Skips empty score-items (no spans). For each non-empty item:
        - One span: (games, None)
        - Two spans: (games, tiebreak_points)
        """
        scores_div = stats_item_div.find("div", class_="scores")
        if not scores_div:
            return [], []

        score_items = scores_div.find_all("div", class_="score-item")
        games = []
        tiebreaks = []

        for item in score_items:
            spans = item.find_all("span")
            if not spans:
                continue
            games.append(int(spans[0].get_text(strip=True)))
            if len(spans) >= 2:
                tiebreaks.append(int(spans[1].get_text(strip=True)))
            else:
                tiebreaks.append(None)

        return games, tiebreaks

    @staticmethod
    def _assign_tiebreaks(
        player_games: list[int],
        player_raw_tb: list[int | None],
        opp_games: list[int],
        opp_raw_tb: list[int | None],
    ) -> tuple[list[int | None], list[int | None]]:
        """Derive tiebreak scores for both sides from raw HTML values.

        The HTML puts the tiebreak value on the set loser (the player
        who finished with 6 games). The winner's TB score is derived as
        max(7, loser_tb + 2).
        """
        player_tb: list[int | None] = []
        opp_tb: list[int | None] = []

        for i in range(len(player_games)):
            p_tb = player_raw_tb[i] if i < len(player_raw_tb) else None
            o_tb = opp_raw_tb[i] if i < len(opp_raw_tb) else None

            if p_tb is None and o_tb is None:
                player_tb.append(None)
                opp_tb.append(None)
            elif p_tb is not None and o_tb is None:
                player_tb.append(p_tb)
                opp_tb.append(max(7, p_tb + 2))
            elif o_tb is not None and p_tb is None:
                player_tb.append(max(7, o_tb + 2))
                opp_tb.append(o_tb)
            else:
                logger.warning(
                    "Both players have tiebreak values in set %d: %s vs %s",
                    i + 1,
                    p_tb,
                    o_tb,
                )
                player_tb.append(p_tb)
                opp_tb.append(o_tb)

        return player_tb, opp_tb

    @staticmethod
    def _parse_match_id(footer_div) -> str | None:
        """Extract match ID from the footer's stats link."""
        cta = footer_div.find("div", class_="match-cta")
        if not cta:
            return None
        for a_tag in cta.find_all("a", href=True):
            href = a_tag["href"]
            if "match-stats" in href or "stats-centre" in href:
                return href.rstrip("/").split("/")[-1]
        return None

    @staticmethod
    def _parse_duration_text(header_div) -> str | None:
        """Extract duration text from the second span in the match header."""
        spans = header_div.find_all("span")
        if len(spans) >= 2:
            text = spans[1].get_text(strip=True)
            return text if text else None
        return None

    @staticmethod
    def _derive_result_type(
        player_scores: list[int], opp_scores: list[int]
    ) -> str:
        """Derive result type from score data.

        Empty scores = walkover, last set max < 6 = retirement, else completed.
        """
        if not player_scores and not opp_scores:
            return "walkover"
        if not player_scores or not opp_scores:
            return "retirement"
        last_set_max = max(player_scores[-1], opp_scores[-1])
        if last_set_max < 6:
            return "retirement"
        return "completed"
