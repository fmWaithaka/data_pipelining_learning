from numpy.distutils.fcompiler.gnu import TARGET_R

from util import get_connection

# def build_insert_query(table_name, column_names):
#     column_values = tuple(map(lambda column: column.replace(column, '%s'), column_names))
#
#     query = (f'''
#     INSERT INTO P={table_name} {column_names} VALUES ({column_values}
#     ''')
#
#     return query

def build_insert_query(table_name, column_names):
    """Generate an SQL INSERT query for the given tables and columns"""
    # Join column names into a comma-separated string
    column_names_str = ', '.join(column_names)

    # Create a comma-separated string of placeholders for values
    column_values_str = ', '.join(['%s'] * len(column_names))

    # Construct the SQL query
    query = f'''
    INSERT INTO {table_name} ({column_names_str}) VALUES ({column_values_str});
    '''

    return query


def insert_data(connection, cursor, query, data, batch_size=100):
    """Insert data into the database in batches."""
    recs = []
    count = 1
    try:
        for rec in data:
            recs.append(rec)
            if count % batch_size == 0:
                cursor.executemany(query, recs)
                connection.commit()
                recs = []
            count += 1
        if recs:
            cursor.executemany(query, recs)
            connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error occurred while inserting data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()

def load_table(db_details, data, column_names, table_name):
    """Load data into a table in the database."""
    TARGET_DB = db_details.get('TARGET_DB')
    if not TARGET_DB:
        raise ValueError("Database details are missing or incomplete.")

    try:
        # Establish the connection
        connection = get_connection(
            db_type=TARGET_DB['DB_TYPE'],
            db_user=TARGET_DB['DB_USER'],
            db_pass=TARGET_DB['DB_PASS'],
            db_host=TARGET_DB['DB_HOST'],
            db_name=TARGET_DB['DB_NAME']
        )
        cursor = connection.cursor()

        # Build the query
        query = build_insert_query(table_name, column_names)

        # Insert the data
        insert_data(connection, cursor, query, data)
    except Exception as e:
        print(f"Error occurred: {e}")
        raise
    finally:
        if connection:
            connection.close()
