import logging
import sys
from util import get_tables, get_db_config
from read import read_table
from write import load_table


def main():
    env = sys.argv[1]
    db_details = get_db_config(env)
    tables = get_tables('tables_list')
    table_name = 'departments'
    data, column_names = read_table(db_details, table_name)
    for table_name in tables['table_name']:
        logging.info(f'reading data for {table_name}')
        data, column_names = read_table(db_details, table_name)
        logging.info(f'loading data for {table_name}')
        load_table(db_details, data, column_names, table_name)


if __name__=='__main__':
    main()