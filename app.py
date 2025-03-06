# import logging
# import sys
# from util import get_tables, get_db_config
# from read import read_table
# from write import load_table

#
# logging.basicConfig(
#     filename="dataPipeline.log",
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     filemode="a"
# )
#
# logging.info("Logging setup complete.")
#
# def main():
#     env = sys.argv[1]
#     a_tables = sys.argv[2]
#     db_details = get_db_config(env)
#     tables = get_tables('tables_list', a_tables)
#     for table_name in tables['table_name']:
#         logging.info(f'reading data for {table_name}')
#         data, column_names = read_table(db_details, table_name)
#         logging.info(f'loading data for {table_name}')
#         load_table(db_details, data, column_names, table_name)
#
# if __name__=='__main__':
#     main()

# import logging
# import sys
# import os
#
# import pandas as pd
#
# from util import get_tables, get_db_config
# from read import read_table
# from write import write_df_to_file
#
# # Debug the log file path and working directory
# print("Current working directory:", os.getcwd())
# print("Log file path:", os.path.abspath("dataPipeline.log"))
#
# logging.basicConfig(
#     filename="dataPipeline.log",
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     filemode="a",
#     force=True
# )
#
# logging.info("Logging setup complete.")
#
# def main():
#     env = sys.argv[1]
#     a_database = sys.argv[2]
#     a_table = sys.argv[3]
#     db_details = get_db_config(env)[a_database]
#     logging.info(f'reading data for {a_table}')
#     data, column_names = read_table(db_details, a_table)
#     df = pd.DataFrame(data, columns=column_names)
#     write_df_to_file('/tmp', table_name=a_table, df=df)
#
# if __name__=='__main__':
#     main()

import logging
import sys
import pandas as pd

from util import get_db_config
from read import read_table
from write import write_df_to_file

# Set up logging with both file and console output
LOG_FILE = "dataPipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler(sys.stdout)  # Logs to console as well
    ]
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

