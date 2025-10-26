import sqlite3
import os


def get_base_file():
    """Name of the SQLite DB file"""
    return os.path.splitext(os.path.basename(__file__))[0]


def connect_db():
    """
    Create and return an SQLite database connection.

    Args:
        db_name (str): Name of the database file to create/connect to

    Returns:
        sqlite3.Connection: SQLite database connection object
    """
    try:
        dbfile = get_base_file() + ".db"
        conn = sqlite3.connect(dbfile, timeout=2)
        print(f"Successfully created SQLite instance: {dbfile}")
        return conn
    except sqlite3.Error as e:
        print(f"Error creating SQLite instance: {e}")
        return None


def query_database(conn, query, args):
    """
    Query the master database (sqlite_master table) for the SQLite instance.

    Args:
        conn (sqlite3.Connection): SQLite database connection object
        query (str): SQL query to execute
        args (tuple): Arguments to pass to the query

    Returns:
        boolean TRUE or FALSE
    """
    result = False

    if conn is None:
        print("Error: No database connection provided")
        return result

    try:
        cursor = conn.cursor()
        cursor.execute(query, args)
        rows = cursor.fetchall()
        numrows = len(list(rows))
        if numrows > 0:
            result = True
    except sqlite3.Error as e:
        print(f"Error querying master database: {e}")
        if cursor != None:
            cursor.close()
    finally:
        if cursor != None:
            cursor.close()
    return result


def check_table(table):
    """Checks if a SQLite DB Table exists"""
    result = False
    conn = connect_db()
    try:
        if not conn is None:
            query = "SELECT * FROM sqlite_master WHERE type='table' AND name=?"
            args = (table,)
            result = query_database(conn, query, args)
            if conn != None:
                conn.close()
    except sqlite3.Error as e:
        print(f"Error querying master database: {e}")
        if conn is not None:
            conn.close()
    return result


# Example usage
if __name__ == "__main__":

    # Check if the table exists
    result = check_table("users")
    print("Table exists:", result is not None)
