import os

ENVIRONMENT_DEBUG = os.environ.get("DEBUG", default=False)
DATABASE_URL = os.environ.get("DATABASE_URL").strip()
