from enum import StrEnum

from mvp.base_job import BaseJob


class Endpoint(StrEnum):
    SCOREBOARD = "scoreboard"
    PRO_REPORT = "pro_report"
    PROJECTIONS = "projections"
    PUBLIC = "public"
    PICKS = "picks"


class ActionNetworkJob(BaseJob):
    """
    Base class for pipeline tasks that fetch and transform data from Action
    Network API.
    """
    SPORTSBOOKS = {
        "15": "Consensus",
        "30": "Open",
        "3915": "Bet365",
        "279": "Caesars",
        "1538": "DraftKings",
        "1797": "BET ESPN",
        "2990": "Fanatics",
        "270": "FanDuel",
        "282": "BetMGM",
        "262": "BetRivers",
    }

    ENDPOINTS = {
        Endpoint.SCOREBOARD: "v2/scoreboard/",
        Endpoint.PRO_REPORT: "v2/scoreboard/proreport/",
        Endpoint.PROJECTIONS: "v2/scoreboard/gameprojections/",
        Endpoint.PUBLIC: "v2/scoreboard/publicbetting/",
        Endpoint.PICKS: "v2/scoreboard/picks/"
    }

    GLOBAL_ENDPOINTS: tuple[Endpoint, ...] = (
        Endpoint.SCOREBOARD, Endpoint.PUBLIC
    )
    GLOBAL_PERIODS:   tuple[str, ...] = ("event",)

    _HALVES: tuple[str, ...] = ("firsthalf", "secondhalf")
    _QUARTERS: tuple[str, ...] = (
        "firstquarter", "secondquarter", "thirdquarter", "fourthquarter"
    )

    LEAGUES = {
        "atp": {
        },
        "mlb": {
            "periods": ["firstinning", "firstfiveinnings"],
            "endpoints": [
                Endpoint.PRO_REPORT, Endpoint.PROJECTIONS, Endpoint.PICKS,
            ],
        },
        "nba": {
            "periods": [*_HALVES, *_QUARTERS],
            "endpoints": [
                 Endpoint.PRO_REPORT, Endpoint.PROJECTIONS, Endpoint.PICKS,
            ]
        },
        "ncaab": {
            "periods": [*_HALVES],
            "endpoints": [
                 Endpoint.PRO_REPORT, Endpoint.PROJECTIONS, Endpoint.PICKS,
            ]
        },
        "ncaaw": {
            "periods": [*_HALVES, *_QUARTERS],
            "endpoints": [
                 Endpoint.PRO_REPORT, Endpoint.PROJECTIONS, Endpoint.PICKS,
            ]
        },
        "nhl": {
            "periods": ["firstperiod", "secondperiod", "thirdperiod"],
            "endpoints": [
                Endpoint.PRO_REPORT, Endpoint.PROJECTIONS, Endpoint.PICKS,
            ]
        },
        "soccer": {
            "periods": [*_HALVES],
            "endpoints": [Endpoint.PICKS]
        },
        "wta": {

        }
    }

    def __init__(self, league, game_date):
        super().__init__(league=league, game_date=game_date)

    @property
    def source(self) -> str:
        return 'action_network'

    @property
    def league_config(self):
        """
        Get the league configuration for the current league.
        :return: Dictionary with league-specific configurations.
        """
        league_config = self.LEAGUES.get(self.league, {})
        if not league_config.get('record_path'):
            league_config['record_path'] = 'games'

        return league_config
