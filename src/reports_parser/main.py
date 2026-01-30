from .get_directory_path import get_directory_path
from .get_report_list import get_report_list
from .get_item_id import get_item_id
from .export_to_csv import export_to_csv
from pathlib import Path

# Get Directory Path
directory = get_directory_path()

# get the list of all report names
reports = get_report_list(directory)

# get the map for acap_key and item_id
acapKeyItemIdMap = get_item_id(reports)

# save the acapKeyItemIdMap in a csv format.
filename = "acapid_itemid_map_plaid.csv"
export_to_csv(acapKeyItemIdMap, output_path=Path.cwd() / "output" / filename)
