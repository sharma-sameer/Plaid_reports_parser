from .get_item_id import extract_itemid
import logging

# Configure the root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Use in your module
logger = logging.getLogger(__name__)

# Run the code to create csv with [acap_id, item_id(s)] pairs.
logger.info("Run the code to create csv with [acap_id, item_id(s)] pairs.")
extract_itemid()
