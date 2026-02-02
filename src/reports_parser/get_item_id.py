"""
This code pulls item_id from each json file provided by plaid containing bank data for our customers.
the path to item_id is: items[0] >> item_id.
Record this in a dictionary to avoid duplicates and then return as a csv.
dictionary structure: {acap_key_1 : item_id_1, acap_key_2 : item_id_2, ...}
"""

import pandas as pd
from typing import List
import json
from .get_directory_path import get_directory_path
from .fetch_reports import get_reports


def get_itemid(row: pd.DataFrame) -> List:
    """
    A Lambda that would consume a dataframe row and return item_id for that row.

    Args:
        row (pd.DataFrame): A row of the pandas DataFrame
    Returns:
        List: A list of all item_ids associated with that row.
    """
    report = json.loads(row["report_data"])

    return [item["item_id"] for item in report["items"]]


def extract_itemid():
    """
    1. Pull the reports from snowflake for the period Jan 2025 - Sep 2025.
    2. Parse the json report and extract the item_id(s) corresponding to each report.
    3. Store the [acap_id, item_id(s)] pair as a csv.

    Args:
        None.
    Returns:
        None
    """
    # Get the reports
    reports_df = get_reports()
    # Extract the item_id
    reports_df["item_id"] = reports_df.apply(get_itemid, axis=1)
    # Create a subset of the dataframe with just [acap_id, item_id(s)] pair.
    output_df = reports_df[["acap_refr_id", "item_id"]]

    # Save the [acap_id, item_id(s)] pair as a csv.
    output_df.to_csv("acapid_itemid_map_plaid.csv", index=False)
