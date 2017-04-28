import sqlite3


def create_sqlite_tables(sqlite_file):
    """
    Create sqllite tables to insert address data
    :param sqlite_file:
    :return:
    """
    # Connecting to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Creating a new SQLite table with 1 column
    c.execute("""
    CREATE TABLE IF NOT EXISTS AddressMerged (
        IsUpdated    INTEGER,
        HouseStreet  TEXT,
        CityProvince TEXT,
        PostalCode   TEXT,
        Latitude     REAL,
        Longitude    REAL,
        PRIMARY KEY (
            HouseStreet,
            CityProvince,
            PostalCode
        )
    );
    """)
    # Committing changes and closing the connection to the database file
    conn.commit()
    conn.close()

if __name__ == "__main__":
    SQLLITEFILE = 'latlongadd_db.sqlite'
    ROOT_PATH = "/home/jay/BigData/SFUCourses/CMPT733/ProjectData/"
    sqlite_file = ROOT_PATH + SQLLITEFILE  # name of the sqlite database file
    create_sqlite_tables(sqlite_file)
