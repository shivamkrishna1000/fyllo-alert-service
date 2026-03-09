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

def get_database_url() -> str:
    """Return Neon database path."""
    return os.environ.get("DATABASE_URL")

def get_farm_user_id() -> str:
    """Retrieve Farm User ID from environment variables."""
    return os.environ.get("FARM_USER_ID")

def get_fyllo_password() -> str:
    """Retrieve Fyllo password from environment."""
    return os.environ.get("FYLLO_PASSWORD")

def get_wati_base_url() -> str | None:
    """Retrieve Wati base URL from environment"""
    return os.environ.get("WATI_BASE_URL")


def get_wati_tenant_id() -> str | None:
    """Retrieve Wati tenant ID from environment."""
    return os.environ.get("WATI_TENANT_ID")


def get_wati_api_token() -> str | None:
    """Retrieve Wati API Token from environment."""
    return os.environ.get("WATI_API_TOKEN")


def get_wati_test_number() -> str | None:
    """Retrieve Test Mobile No. from environment."""
    return os.environ.get("WATI_TEST_NUMBER")


def get_wati_template_name() -> str | None:
    """Retrieve Wati Template Name from environment."""
    return os.environ.get("WATI_TEMPLATE_NAME")