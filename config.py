import os
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SECRETS = Path(PROJECT_ROOT) / 'secrets.yaml'

LEAGUE_MONTHS = {
    'ATP': list(range(1, 13)),
    'MLB': list(range(3, 12)),
    'NBA': [10, 11, 12, 1, 2, 3, 4, 5, 6],
    'NCAAB': [11, 12, 1, 2, 3, 4],
    'NCAAW': [11, 12, 1, 2, 3, 4],
    'NHL': [10, 11, 12, 1, 2, 3, 4, 5, 6],
    'SOCCER': list(range(1, 13)),
    'WNBA': [5, 6, 7, 8, 9, 10, 11],
    'WTA': list(range(1, 13)),
}
