"""
Code to run a snowflake query and return the results as a pandas DataFrame.
"""

import snowflake.connector
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import json
from typing import Connection

load_dotenv()


def get_connector() -> Connection:
    """
    Function to establish connection with snowflake and return the connection object.

    Args:
        None
    Returns:
        ctx (Connection): A connection object to the snowflake datanase.
    """
    # . Establish connection with the snowflake database.
    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account="onemainfinancial-prod",
        database="EDS",
        warehouse="ANALYSIS_01",
        role="SF_RG_SCH_MODEL_DATA_SERVICES_RO",
        # if encountered "250006 (08001): The authentication failed for XXX@omf.com"
        # try set authenticator = 'externalbrowser', lead you to Okta SSO webpage
        # authenticator="https://springleaf.okta.com",
        authenticator="externalbrowser",
    )
    # Return the connection object.
    return ctx


def get_reports() -> pd.DataFrame:
    """
    1. Establish connection to snowflake.
    2. Execute the query and store the data as a pandas DataFrame.
    3. Return the DataFrame.
    Args:
        None
    Returns:
        reports_df (pd.DataFrame): A pandas DataFrame with the report data and the acap_key
    """
    # Establish connection to snowflake.
    ctx = get_connector()
    # Get the cursor
    cursor = ctx.cursor()
    # Open the sql file to the executed.
    with open(Path.cwd() / "sql" / "fetch_reports.sql", "r") as query:
        # Run the query and save results as a pandas DataFrame.
        reports_df = cursor.execute(query.read()).fetch_pandas_all()

    # Return the resulting DataFrame
    return reports_df
