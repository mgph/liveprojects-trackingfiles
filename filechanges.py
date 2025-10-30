import sqlite3
import os
import hashlib
from datetime import datetime
import sys


def get_base_file():
    """Get the same base filename"""
    return os.path.splitext(os.path.basename(__file__))[0]


def connect_db():
    """Connect or Create to a database"""
    try:
        dbfile = get_base_file() + ".db"
        conn = sqlite3.connect(dbfile, timeout=2)
        print(f"Successfully connected SQLite instance: {dbfile}")
        return conn
    except sqlite3.Error as e:
        print(f"Error creating SQLite instance: {e}")
        return None


def query_database(query, args="", table="files"):
    """Query the database for changes"""
    conn = connect_db()
    if conn is None:
        print("Error: No database connection provided")
        return False
    try:
        cursor = conn.cursor()
        if check_table(table):
            cursor.execute(query, args)
    except sqlite3.Error as e:
        print(f"Error querying master database: {e}")
        if cursor != None:
            cursor.close()
    finally:
        conn.commit()
        if cursor != None:
            cursor.close()


def fetch_database(query, args=""):
    """Fetch results to display"""
    conn = connect_db()
    if conn == None:
        print("Error: No database connection provided")
        return False
    try:
        cursor = conn.cursor()
        result = cursor.execute(query, args).fetchall()
    except sqlite3.Error as e:
        print(f"Error querying master database: {e}")
        if cursor != None:
            cursor.close()
    finally:
        if cursor != None:
            cursor.close()
        return result


def list_tables():
    """Show all tables"""
    query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    tables = fetch_database(query)
    if len(tables) > 1:
        print("Tables in database:")
        print("-" * 40)
        for table in tables:
            table_name = table[0]
            if not table_name.startswith("sqlite_"):
                print(f"  - {table_name}")

        # return [table[0] for table in tables if not table[0].startswith("sqlite_")]
    else:
        print("There are no tables")


def check_table(table):
    """Checks if a SQLite DB Table exists"""
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return len(fetch_database(query, (table,)))


def get_column_names(table_name="files"):
    """Check the columns for the table"""
    query = f"PRAGMA table_info({table_name})"
    columns = fetch_database(query)
    return [col[1] for col in columns]


def delete_table(table):
    """Deletes a SQLite DB Table"""
    query = f"DROP TABLE IF EXISTS {table}"
    query_database(query)


def create_hashtable(table="files"):
    """Creates a SQLite DB Table"""
    query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
           file TEXT,
           md5 TEXT
    )"""
    query_database(query)


def create_hashtable_idx(table="files"):
    """Creates a SQLite DB Table Index"""
    query = f"""
    CREATE INDEX IF NOT EXISTS idx_{table} 
    ON {table} (file, md5)"""
    query_database(query)


def update_hashtable(file, md5, table="files"):
    """Update MD5 hash for a file"""
    query = f"UPDATE {table} SET md5=? WHERE file=?"
    args = (md5, file)
    query_database(query, args)


def insert_hashtable(file, md5, table="files"):
    """Insert file into the Files table"""
    query = f"INSERT INTO {table} (file, md5) VALUES (?, ?)"
    args = (file, md5)
    query_database(query, args)


def setup_hashtable(file, md5, table="files"):
    """Setup the Files table"""
    create_hashtable(table)
    create_hashtable_idx(table)
    insert_hashtable(file, md5, table)


def md5indb(file, table="files"):
    """Check md5 hash exists"""
    query = f"SELECT md5 FROM {table} WHERE file={file}"
    return fetch_database(query)


def haschanged(file, md5, table="files"):
    """Check if file has changed"""
    current_md5 = md5indb(file, table)
    if current_md5 != md5:
        update_hashtable(file, md5, table)
    else:
        insert_hashtable(file, md5, table)


def get_fileext(file):
    """Get the file extension"""
    return os.path.splitext(file)[1]


def get_moddate(file):
    """Get the file modified date"""
    try:
        mtime = os.path.getmtime(file)
        # return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
    except OSError as e:
        print(f"Error getting modified date for {file}: {e}")
        mtime = 0
    return mtime


def md5short(file):
    """Get md5 file hash tag"""
    return hashlib.md5(str(file + "|" + get_moddate(file)).encode("utf-8")).hexdigest()


def check_filechanges(folder, exclude, ws):
    changed = False
    for subdir, dirs, files in os.walk(folder):
        for file in files:
            origin = os.path.join(subdir, file)
            if os.path.isfile(origin):
                file_ext = get_fileext(file)
                if not file_ext in exclude:
                    md5 = md5short(file)
                    # later
                    if haschanged(file, md5):
                        # later
                        print(f"{origin} has changed")
                        changed = True
    return changed


def load_folders():
    folders = []
    extensions = []
    config = get_base_file() + ".ini"
    if os.path.isfile(config):
        cfile = open(config, "r")
        for line in cfile.readlines():
            if "|" in line:
                folders.append(line.split("|")[0])
                exts = line.split("|")[1]
                if len(exts) > 0:
                    extl = exts.split(",")
                    extensions.append(extl)
            else:
                folders.append(line)
                extensions.append([])
        cfile.close()
    return folders, extensions


def run_filechanges(ws):
    changed = False
    folders_exts = load_folders()
    for i, folder in enumerate(folders_exts[0]):
        exts = folders_exts[1]
        changed = check_filechanges(folder, exts[i], ws)
    return changed


def execute(args):
    changes = 0
    if len(args) > 1:
        if args[1].lower() == "--loop":
            # later
            try:
                while True:
                    ws = None
                    if run_filechanges(ws):
                        changes += 1
            except KeyboardInterrupt:
                print("Stopped...")
                if changes > 0:
                    print(changes)
    else:
        ws = None
        if run_filechanges(ws):
            print(changes)


if __name__ == "__main__":
    execute(sys.argv)
