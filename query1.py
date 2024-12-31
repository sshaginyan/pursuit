import sqlite3
import pandas as pd

connection = sqlite3.connect("data/pursuit.db")
cursor = connection.cursor()

query = """
    SELECT DISTINCT places.*
    FROM places
    INNER JOIN contacts ON places.contact_place_id = contacts.place_id
    WHERE contacts.title LIKE '%finance%';
"""

query_df = pd.read_sql_query(query, connection)

print(query_df)

connection.close()