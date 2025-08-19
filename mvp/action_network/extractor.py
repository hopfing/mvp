import logging

from mvp.action_network.job import ActionNetworkJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


class ActionNetworkExtractor(ActionNetworkJob):
    """
        Class used to send requests to ActionNetwork API and store output JSON.
        """
    BASE_URL = "https://api.actionnetwork.com/web/"

    def __init__(self, league, game_date):
        super().__init__(game_date=game_date, league=league)
