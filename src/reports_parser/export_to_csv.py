import csv
from typing import Dict
from pathlib import Path


def export_to_csv(acapKeyItemIdMap: Dict, output_path: Path) -> None:
    """
    Export the map to a csv file

    Args:
        acapKeyItemIdMap: Map for acap_key : Item_id
        type:  Dict
        output_path: Path where the csv file is to be stored
        type: Path
    Returns:
        None
    """
    try:
        # Try opening the file to which we write this data.
        with open(output_path, "w", newline="") as output_file:
            # Get a writer to the csv file
            writer = csv.writer(output_file)
            # Add a header
            writer.writerow(["ACAP_KEY", "ITEM_ID"])
            # Write contents of the dictionary one row at a time.
            for key, value in acapKeyItemIdMap.items():
                writer.writerow([key, *value])
    except Exception as e:
        print(e)
