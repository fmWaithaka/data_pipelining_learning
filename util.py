import pandas as pd
from mysql import connector as mc
from config import DB_DETAILS
from mysql.connector import errorcode as ec
import psycopg2


def load_db_details(env):
    """Fetch database connection details for the given environment."""
    return DB_DETAILS.get(env)


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
        if error.errno == ec.ER_ACCESS_DENIED_ERROR:
            print("Invalid Credentials for MySQL")
        else:
            print(f"MySQL Connection Error: {error}")
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


def get_connection(db_type, db_host, db_name, db_user, db_pass):
    """Get a database connection based on the database type."""
    connection = None
    if db_type == 'mysql':
        connection = get_mysql_connection(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_pass=db_pass
        )
    elif db_type == 'postgres':
        connection = get_pg_connection(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_pass=db_pass
        )
    else:
        print(f"Unsupported database type: {db_type}")
    return connection



def get_tables(path, sep=":"):
    try:
        tables = pd.read_csv(path, sep=sep)
        return tables.query('to_be_loaded == "yes"')
    except Exception as e:
        print(f"Error reading tables file: {e}")
        return None