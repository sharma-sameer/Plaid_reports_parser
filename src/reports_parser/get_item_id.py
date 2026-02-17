"""
This code pulls item_id from each json file provided by plaid containing bank data for our customers.
the path to item_id is: items[0] >> item_id.
Record this in a dictionary to avoid duplicates and then return as a csv.
dictionary structure: {acap_key_1 : item_id_1, acap_key_2 : item_id_2, ...}
"""

import polars as pl
from typing import List
import json
from .get_directory_path import get_directory_path
from .fetch_reports import get_reports, logger
from pathlib import Path
import numpy as np


def get_itemid(row: pl.DataFrame) -> List:
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

    reports_df = reports_df.unique()
    if reports_df is not None:
        logger.info(f"Successfully fetched {len(reports_df)} pairs.")
    else:
        logger.info("Exiting the process.")
        return

    # Save the [acap_id, item_id(s)] pair as a csv.
    logger.info("Saving the [acap_id, item_id(s)] pair as a csv.")

    cross_verification_df = pl.read_csv(
        Path.cwd() / "output" / "cross_verification_list.csv"
    )
    cross_verification_df = cross_verification_df.with_columns(
        pl.col("cross_validation_set").cast(pl.Int64)
    )
    reports_df = reports_df.with_columns(pl.col("ACAP_REFR_ID").cast(pl.Int64))
    is_in_reports_df = (
        cross_verification_df["cross_validation_set"]
        .unique()
        .is_in(reports_df["ACAP_REFR_ID"])
        .rename("is_in_reports_df")
    )
    # logger.info(is_in_reports_df)
    cv_df = cross_verification_df.with_columns(is_in_reports_df)
    logger.info(
        f"ACAP_REFR_ID's from Ryland's list not in the reports_df: {cv_df.filter(cv_df["is_in_reports_df"] == False)}"
    )
    cv_df.filter(cv_df["is_in_reports_df"] == False).write_csv(
        Path.cwd() / "output" / "Missing_ACAP_KEYS.csv"
    )

    is_in_cv_df = (
        reports_df["ACAP_REFR_ID"]
        .unique()
        .is_in(cross_verification_df["cross_validation_set"].unique())
        .rename("is_in_cv_df")
    )
    reports_df_all = reports_df.with_columns(is_in_cv_df)
    logger.info(
        f"ACAP_REFR_ID's from reports_df not in the Ryland's list: {reports_df_all.filter(reports_df_all["is_in_cv_df"] == False)}"
    )
    reports_df_all.filter(reports_df_all["is_in_cv_df"] == False).write_csv(
        Path.cwd() / "output" / "Additional_ACAP_KEYS.csv"
    )
    # chunk_size = 100000

    # # Iterate through the list and save each smaller DataFrame to a new file
    # for i, chunk in enumerate(reports_df.iter_slices(n_rows=chunk_size)):
    #     output_filename = Path.cwd() / "output" / f'output_part_{i+1}.csv'
    #     chunk.write_csv(output_filename)
    #     logger.info(f"Saved {output_filename}")
    # logger.info(f"Saved the data at path {Path.cwd() / "output"}.")
    output_filename = Path.cwd() / "output" / f"acap_itemid_pairs.csv"
    reports_df.write_csv(output_filename)
    logger.info(f"Saved the data at path {Path.cwd() / "output"}.")
