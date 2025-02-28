import logging
import sys
from util import get_tables, get_db_config
from read import read_table
from write import load_table

# Configure logging
logging.basicConfig(filename="dataPipeline.info", level=logging.INFO, format="%(asctime)s - %(levelname)s - %([message])s")


def main():
    env = sys.argv[1]
    a_tables = sys.argv[2]
    db_details = get_db_config(env)
    tables = get_tables('tables_list', a_tables)
    for table_name in tables['table_name']:
        logging.info(f'reading data for {table_name}')
        data, column_names = read_table(db_details, table_name)
        logging.info(f'loading data for {table_name}')
        load_table(db_details, data, column_names, table_name)


if __name__=='__main__':
    main()