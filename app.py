import logging
import sys
import pandas as pd

from util import get_db_config
from read import read_table
from write import write_df_to_file

# Set up logging with both file and console output
LOG_FILE = "dataPipeline.log"

logging.basicConfig(
    filename="dataPipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)

logging.info("Logging setup complete.")

def validate_arguments():
    """Ensure correct script usage and required arguments."""
    if len(sys.argv) < 4:
        logging.error("Usage: python script.py <env> <database> <table>")
        sys.exit(1)

def main():
    """
    Main function to read data from a database table and write it to a file.
    Handles errors gracefully.
    """
    validate_arguments()

    env, a_database, a_table = sys.argv[1], sys.argv[2], sys.argv[3]

    try:
        db_config = get_db_config(env)
        if not db_config:
            logging.error(f"Invalid environment: {env}")
            sys.exit(1)

        if a_database not in db_config:
            logging.error(f"Database '{a_database}' not found in config.")
            sys.exit(1)

        db_details = db_config[a_database]
        logging.info(f"Fetching data for table '{a_table}' from database '{a_database}'.")

        data, column_names = read_table(db_details, a_table)
        if not data:
            logging.warning(f"No data retrieved for table '{a_table}'.")
            sys.exit(1)

        df = pd.DataFrame(data, columns=column_names)

        output_path = "/tmp"
        write_df_to_file(output_path, table_name=a_table, df=df)
        logging.info(f"Data successfully written to {output_path}/{a_table}.csv")

    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

