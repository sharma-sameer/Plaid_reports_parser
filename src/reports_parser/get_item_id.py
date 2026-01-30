"""
This code pulls item_id frm each json file provided by plaid containing bank data for our customers.
the path to item_id is: items[0] >> item_id.
Record this in a dictionary to avoid duplicates and then return as a csv.
dictionary structure: {acap_key_1 : item_id_1, acap_key_2 : item_id_2, ...}
"""

from typing import Dict, List
import json
from .get_directory_path import get_directory_path


def get_item_id(reports: List[str]) -> Dict:
    """
    Retruns a dictionary with the structure {acap_key_1 : item_id_1, acap_key_2 : item_id_2, ...}

    Args:
        reports: List of the bank transaction report names present in the directory.
        type: List[str]
    Returns:
        Dict: Dictionary mapping acap_key to the item_id
    """
    # Initialize the dictionary where the resulting map will be stored.
    acapKeyItemIdMap = {}
    # Parse through the reports and extract acap_key and item_id(s) for each.
    for report in reports:
        # Build the filepath for the current report
        current_report_path = str(get_directory_path() / report)
        try:
            # Try accessing the report.
            with open(current_report_path, "r") as json_file:
                this_report = json.load(json_file)
        except Exception:
            print("Failed to open file.")

        try:
            # Extract the acap_key.
            acap_key = this_report[
                "client_report_id"
            ]  # This is my guess for the location of acap_id on the report.
        except Exception:
            print("Failed to extract Acap_key")
        item_id = []
        try:
            # Extract the item_id(s)
            for item in this_report["items"]:
                item_id.append(item["item_id"])
        except:
            print("Failed to find item_id")
        acapKeyItemIdMap[acap_key] = item_id

    return acapKeyItemIdMap
