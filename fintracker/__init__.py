import os
from fintracker.utils import setup_logging
if not "DISABLE_LOG_CONFIG" in os.environ:
    setup_logging()