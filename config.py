import os
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SECRETS = Path(PROJECT_ROOT) / 'secrets.yaml'
