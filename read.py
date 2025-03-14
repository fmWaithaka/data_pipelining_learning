import logging
from util import get_connection

def get_column_names(cursor, db_type):
    """
    Retrieve column names from a database cursor.

    Args:
        cursor: Database cursor object.
        db_type (str): Type of database ('mysql' or 'postgres').

    Returns:
        list: Column names from the table.
    """
    try:
        if db_type == "mysql":
            return cursor.column_names
        elif db_type == "postgres":
            return [column[0] for column in cursor.description]
        else:
            logging.error(f"Unsupported database type: {db_type}")
            return []
    except Exception as e:
        logging.error(f"Error retrieving column names: {e}", exc_info=True)
        return []

def fetch_data(cursor, query):
    """
    Execute a SQL query and fetch results.

    Args:
        cursor: Database cursor object.
        query (str): SQL query to execute.

    Returns:
        tuple: (data, column_names)
    """
    try:
        logging.info(f"Executing query: {query}")
        cursor.execute(query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        logging.error(f"Error executing query: {e}", exc_info=True)
        return None

def read_table(db_details, table_name, limit=0):
    """
    Read data from a specified table in the database.

    Args:
        db_details (dict): Database connection details.
        table_name (str): Name of the table to read from.
        limit (int, optional): Number of rows to fetch (default: all rows).

    Returns:
        tuple: (data, column_names), or (None, None) if an error occurs.
    """
    db_type = db_details.get("DB_TYPE")
    db_host = db_details.get("DB_HOST")
    db_name = db_details.get("DB_NAME")
    db_user = db_details.get("DB_USER")
    db_pass = db_details.get("DB_PASS")

    if not all([db_type, db_host, db_name, db_user, db_pass]):
        logging.error("Missing database configuration details.")
        return None, None

    connection = None
    try:
        connection = get_connection(db_type, db_host, db_name, db_user, db_pass)
        if not connection:
            logging.error("Database connection failed.")
            return None, None

        cursor = connection.cursor()
        query = f"SELECT * FROM {table_name}" if limit == 0 else f"SELECT * FROM {table_name} LIMIT {limit}"

        data = fetch_data(cursor, query)
        if data is None:
            return None, None

        column_names = get_column_names(cursor, db_type)
        return data, column_names

    except Exception as e:
        logging.error(f"Unexpected error in read_table: {e}", exc_info=True)
        return None, None

    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed.")
