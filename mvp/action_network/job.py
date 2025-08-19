from mvp.base_job import BaseJob


class ActionNetworkJob(BaseJob):
    """
    Base class for pipeline tasks that fetch and transform data from Action
    Network API.
    """

    def __init__(self, league, game_date):
        super().__init__(league=league, game_date=game_date)
