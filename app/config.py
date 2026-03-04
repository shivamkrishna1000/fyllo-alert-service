"""
Configuration loader module.
"""

import os
from dotenv import load_dotenv


def load_environment() -> None:
    """Load environment variables from .env file."""
    load_dotenv()


def get_fyllo_base_url() -> str:
    """Return the Fyllo base URL."""
    return os.environ.get("FYLLO_BASE_URL")


def get_fyllo_token() -> str:
    """Return the Fyllo API token."""
    return os.environ.get("FYLLO_TOKEN")


def get_database_url() -> str:
    """Return SQLite database path."""
    return os.environ.get("DATABASE_URL")

def get_farm_user_id() -> str:
    """Retrieve Farm User ID from environment variables."""
    return os.environ.get("FARM_USER_ID")