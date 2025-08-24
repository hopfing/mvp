import logging

import pandas as pd

from mvp.action_network.job import ActionNetworkJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


class ActionNetworkStager(ActionNetworkJob):
    """
    Class used to parse JSON data into tabular format for downstream analytics.
    """

    def __init__(self, league, game_date):
        super().__init__(game_date=game_date, league=league)

    def _parse_events(self, json_data, record_path):
        """
        :param json_data: JSON object with a games-equivalent record path.
        :return: Dataframe containing 1 row per event.
        """
        events = json_data.get(record_path)
        event_dicts = []

        if not events:
            logger.warning("No events found in JSON data.")

        for event in events:
            boxscore = event.get("boxscore", {})
            if self.league == "mlb":
                away_box = boxscore.get("stats", {}).get("away", {})
                home_box = boxscore.get("stats", {}).get("home", {})
                box_dict = {
                    "away_score": away_box.get("runs"),
                    "away_hits": away_box.get("hits"),
                    "away_errors": away_box.get("errors"),
                    "home_score": home_box.get("runs"),
                    "home_hits": home_box.get("hits"),
                    "home_errors": home_box.get("errors"),
                }
            else:
                box_dict = {
                    "away_score": boxscore.get("total_away_points"),
                    "home_score": boxscore.get("total_home_points")
                }
            event_dict = {
                "id": event.get("id"),
                "league_id": event.get("league_id"),
                "league_name": event.get("league_name"),
                "status": event.get("status"),
                "real_status": event.get("real_status"),
                "status_display": event.get("status_display"),
                "start_time": event.get("start_time"),
                "away_team_id": event.get("away_team_id"),
                "home_team_id": event.get("home_team_id"),
                "winning_team_id": event.get("winning_team_id"),
                "type": event.get("type"),
                "season": event.get("season"),
                "week": event.get("week"),
                "attendance": event.get("attendance"),
                "num_bets": event.get("num_bets"),
                **box_dict
            }
            event_dicts.append(event_dict)

        df = pd.DataFrame(event_dicts)

        return df

    def _parse_teams(self, json_data, record_path):

        df = pd.json_normalize(
            data=json_data,
            record_path=[record_path, "teams"],
            meta=[
                [record_path, 'id'],
            ],
            sep="_"
        )

        return df

    def _parse_markets(self, json_data, record_path):

        events = json_data.get(record_path)
        market_dfs = []

        if not events:
            logger.warning("No events found in JSON data.")

        for event in events:
            markets_json = event.get("markets")
            if not markets_json:
                logger.warning("No markets found for %s",
                               event.get("id"))
                continue
            for book_id, segments in markets_json.items():
                for segment, markets in segments.items():
                    for market, value in markets.items():
                        market_df = pd.json_normalize(
                            value,
                            sep='_'
                        )
                        market_df['book_id'] = book_id
                        market_df['book_name'] = self.SPORTSBOOKS.get(book_id)
                        market_df['period'] = segment
                        market_dfs.append(market_df)

        markets_df = pd.concat(market_dfs, ignore_index=True)

        return markets_df

    def _parse_edge_projections(self, json_data, record_path):

        events = json_data.get(record_path)
        projection_dicts = []

        for event in events:
            edge_projections = event.get("edge_projections")
            if not edge_projections:
                logger.warning("No edge projections found for %s",
                               event.get("id"))
                continue
            for segment, projections in edge_projections.items():
                projection_dict = {
                    "event_id": event.get("id"),
                    "segment": segment,
                    **projections
                }
                projection_dicts.append(projection_dict)

        return pd.DataFrame(projection_dicts)

    def parse_projections(self, json_data):

        datasets = ["events", "teams", "edge_projections", "markets"]

        for dataset in datasets:
            parser = getattr(self, f"_parse_{dataset}", None)
            if parser:
                df = parser(json_data, self.league_config.get("record_path"))
                file_path = self.build_file_path(
                    dir_path=self.staged_dir,
                    file_name=f"projections_{dataset}",
                    file_type="csv"
                )
                self.save_df_to_csv(
                    df=df,
                    path=file_path
                )

    def run(self, manifest=None):

        if manifest:
            items = manifest.get("items", [])
        else:
            items = []

        if len(items) < 0:
            logger.warning(
                "No raw JSON files found for %s.",
                self.league.upper()
            )
            return None

        for item in items:
            item_parser = getattr(self, f"parse_{item["endpoint"]}", None)
            if item_parser:
                json_data = self.read_json(item["uri"])
                item_parser(json_data)
