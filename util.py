import pandas as pd
from mysql import connector as mc
from mysql.connector import errorcode as ec
import psycopg2
import logging
from config import DB_DETAILS


# Setup logging
logging.basicConfig(
    filename="dataPipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_db_details(env):
    return DB_DETAILS[env]


def get_mysql_connection(db_host, db_name, db_user, db_pass):
    """Add explicit error handling"""
    try:
        connection = mc.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            database=db_name
        )
        logging.info("MySQL connection successful")
        return connection
    except mc.Error as e:
        if e.errno == ec.ER_ACCESS_DENIED_ERROR:
            logging.error(f"MySQL access denied. Verify credentials for {db_user}@{db_host}")
        else:
            logging.error(f"MySQL connection error: {str(e)}")
        return None

def get_pg_connection(db_host, db_name, db_user, db_pass):
    try:
        return psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host=db_host,
            password=db_pass
        )
    except psycopg2.Error as error:
        logging.error(f"PostgreSQL Connection Error: {error}")
    return None


def get_connection(db_type, db_host, db_name, db_user, db_pass):
    if db_type == 'mysql':
        return get_mysql_connection(db_host, db_name, db_user, db_pass)
    elif db_type == 'postgres':
        return get_pg_connection(db_host, db_name, db_user, db_pass)
    else:
        logging.error(f"Unsupported database type: {db_type}")
        return None


def get_tables(path, table_list):
    try:
        tables = pd.read_csv(path, sep=',')
        if table_list == 'all':
            return tables.query('to_be_loaded == "yes"')
        else:
            table_list_df = pd.DataFrame(table_list.split(','), columns=['table_name'])
            return tables.join(
                table_list_df.set_index('table_name'),
                on='table_name',
                how='inner'
            ).query('to_be_loaded == "yes"')
    except Exception as e:
        logging.error(f"Error reading tables file: {e}")
        return None
