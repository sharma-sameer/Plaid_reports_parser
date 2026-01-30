"""
Returns the directory path where the json reports are stored.
"""

from pathlib import Path


def get_directory_path() -> Path:
    """
    Returns the directory path for the location of the plaid reports.
    Args:
        None
    Returns:
        Path to the directory where reports are stored.
    """
    return Path.cwd() / "reports"
