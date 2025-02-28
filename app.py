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

import logging
import sys
import os
from util import get_tables, get_db_config
from read import read_table
from write import load_table

# Debug the log file path and working directory
print("Current working directory:", os.getcwd())
print("Log file path:", os.path.abspath("dataPipeline.log"))

logging.basicConfig(
    filename="dataPipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True  # Reset logging config (Python 3.8+)
)

logging.info("Logging setup complete.")

def main():
    try:
        env = sys.argv[1]
        a_tables = sys.argv[2]
        db_details = get_db_config(env)
        tables = get_tables('tables_list', a_tables)
        logging.info(f"Tables to process: {tables}")

        if tables['table_name'].empty:
            logging.warning("No tables to process!")
            return

        for table_name in tables['table_name']:
            try:
                logging.info(f'Reading data for {table_name}')
                data, column_names = read_table(db_details, table_name)
                logging.info(f'Loading data for {table_name}')
                load_table(db_details, data, column_names, table_name)
            except Exception as e:
                logging.error(f"Error processing {table_name}: {str(e)}", exc_info=True)
    except IndexError:
        logging.error("Missing arguments. Usage: script.py <env> <a_tables>")
    except Exception as e:
        logging.error(f"Critical error: {str(e)}", exc_info=True)

if __name__ == '__main__':
    main()
    # Ensure all logs are flushed
    logging.shutdown()