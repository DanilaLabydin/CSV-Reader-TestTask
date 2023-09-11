import os
import logging


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    # change handler name
    handlers=[
        logging.FileHandler("/tmp/csv_reader_logger.log"),
        logging.StreamHandler(),
    ],
)


LOGGER = logging.getLogger(__name__)
SERVER_PATH = "./data/server.csv"
CLIENT_PATH = "./data/client.csv"
DB_PATH = "./data/cheaters.db"
SERVER_COLUMNS = ["timestamp", "event_id", "error_id", "description"]
CLIENT_COLUMNS = ["timestamp", "error_id", "player_id", "description"]


if not os.path.isfile(SERVER_PATH):
    raise Exception(f"No {SERVER_PATH} file found")

if not os.path.isfile(CLIENT_PATH):
    raise Exception(f"No {CLIENT_PATH} file found")

if not os.path.isfile(DB_PATH):
    raise Exception(f"No {DB_PATH} file found")
