import os
import sqlite3
from typing import Optional

from .config import STAGING_DB_PATH


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def get_staging_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Return a sqlite3.Connection to the staging DB.

    Ensures the parent directory exists before opening the DB so scripts can create the file.

    Args:
        db_path: optional override for the staging DB path. If None, uses config.STAGING_DB_PATH.

    Returns:
        sqlite3.Connection
    """
    path = db_path or STAGING_DB_PATH
    ensure_parent_dir(path)
    conn = sqlite3.connect(path)
    return conn
