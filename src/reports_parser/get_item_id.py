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
from .fetch_reports import get_reports, logger
from pathlib import Path
import numpy as np


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
    logger.info(
        "Run the query to fetch the dataframe with [acap_id, item_id(s)] pairs."
    )
    reports_df = get_reports()
    logger.info("Deduping the records for getting unique pairs.")

    reports_df = reports_df.drop_duplicates()
    # Extract the item_id
    # reports_df["item_id"] = reports_df.apply(get_itemid, axis=1)
    if reports_df is not None:
        logger.info(f"Successfully fetched {len(reports_df)} pairs.")
    else:
        logger.info("Exiting the process.")
        return

    # Save the [acap_id, item_id(s)] pair as a csv.
    logger.info("Saving the [acap_id, item_id(s)] pair as a csv.")
    
    chunk_size = 100000
    # Calculate the number of splits needed
    num_splits = len(reports_df) // chunk_size + (1 if len(reports_df) % chunk_size else 0)

    # Split the DataFrame into a list of smaller DataFrames
    df_list = np.array_split(reports_df, num_splits)

    # Iterate through the list and save each smaller DataFrame to a new file
    for i, chunk in enumerate(df_list):
        output_filename = Path.cwd() / "output" / f'output_part_{i+1}.csv'
        chunk.to_csv(output_filename, index=False)
        logger.info(f"Saved {output_filename}")
    logger.info(f"Saved the data at path {Path.cwd() / "output"}.")
