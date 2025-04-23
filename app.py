import logging
import sys

from util import get_tables, load_db_details
from read import read_table
from write import load_table

# Setup logging as before
LOG_FILE = "dataPipeline.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def validate_arguments():
    if len(sys.argv) < 3:
        logging.error("Usage: python app.py <env> <table_list> (or 'all')")
        sys.exit(1)


def main():
    validate_arguments()

    env = sys.argv[1]
    table_arg = sys.argv[2]

    logging.info("Loading DB configuration...")
    db_config = load_db_details(env)
    if not db_config:
        logging.error("Failed to load DB configuration.")
        sys.exit(1)

    source_db = db_config.get("SOURCE_DB")
    target_db = db_config.get("TARGET_DB")
    if not (source_db and target_db):
        logging.error("Missing SOURCE_DB or TARGET_DB configuration.")
        sys.exit(1)

    logging.info("Retrieving table list...")
    tables_df = get_tables("tables_list", table_arg)
    if tables_df is None or tables_df.empty:
        logging.error("No tables found to process.")
        sys.exit(1)

    for table_name in tables_df["table_name"]:
        logging.info(f"Reading data from table: {table_name}")
        data, column_names = read_table(source_db, table_name)
        if data is None or len(data) == 0:
            logging.warning(f"No data found in table: {table_name}")
            continue

        logging.info(f"Loading data into target for table: {table_name}")
        load_table(db_config, data, column_names, table_name)

    logging.info("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
