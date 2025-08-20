from mvp.base_job import BaseJob


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
        "scoreboard": "v2/scoreboard/",
        "pro_report": "v2/scoreboard/proreport/",
        "projections": "v2/scoreboard/gameprojections/",
        "public": "v2/scoreboard/publicbetting/",
        "picks": "v2/scoreboard/picks/"
    }

    LEAGUES = {
        "atp": {
            "periods": ["event",],
            "endpoints": ["public",],
            "records_key": "competitions"
        },
        "mlb": {
            "periods": ["event", "firstinning", "firstfiveinnings"],
            "endpoints": [
                "pro_report", "projections", "public", "picks",
            ]
        },
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
        return self.LEAGUES.get(self.league, {})
