import pandas as pd
from mysql import connector as mc
from config import DB_DETAILS
from mysql.connector import errorcode as ec
import psycopg2

import logging
logging.basicConfig(filename="dataPipeline.error", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")



# def load_db_details(env):
#     """Fetch database connection details for the given environment."""
#     return DB_DETAILS.get(env)
def get_db_config(env='dev'):
    """Fetch database connection details for the given environment."""
    return DB_DETAILS.get(env, {})



def get_mysql_connection(db_host, db_name, db_user, db_pass):
    """Establish a connection to a MySQL database."""
    try:
        connection = mc.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            database=db_name
        )
        return connection
    except mc.Error as error:
        logging.error(f"MySQL Connection Error: {error}")
    return None


def get_pg_connection(db_host, db_name, db_user, db_pass):
    """Establish a connection to a PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host=db_host,
            password=db_pass
        )
        return connection
    except psycopg2.Error as error:
        print(f"PostgreSQL Connection Error: {error}")
    return None



DB_CONNECTIONS = {
    'mysql': get_mysql_connection,
    'postgres': get_pg_connection
}

def get_connection(db_type, db_host, db_name, db_user, db_pass):
    """Get a database connection based on the database type."""
    connection_func = DB_CONNECTIONS.get(db_type)
    if connection_func:
        return connection_func(db_host, db_name, db_user, db_pass)
    logging.error(f"Unsupported database type: {db_type}")
    return None


def get_tables(path, table_list):
    try:
        tables = pd.read_csv(path, sep=",")

        if table_list == "all":
            return tables.query('to_be_loaded == "yes"')

        # Convert table_list (comma-separated string) into a DataFrame
        table_list_df = pd.DataFrame({'table_name': table_list.split(',')})

        # Merge instead of join for proper filtering
        filtered_tables = tables.merge(table_list_df, on='table_name', how='inner')

        return filtered_tables.query('to_be_loaded == "yes"')

    except Exception as e:
        logging.error(f"Error reading tables file: {e}")
        return None
