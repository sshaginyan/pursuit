import sqlite3
import pandas as pd

connection = sqlite3.connect("data/pursuit.db")
cursor = connection.cursor()

query = """
    SELECT DISTINCT c.*
    FROM contacts c
    JOIN techstacks t ON c.place_id = t.place_id
    JOIN places p ON c.place_id = p.contact_place_id
    WHERE c.emails LIKE '%bob%'
        AND t.name = 'Accela'
        AND p.pop_estimate_2022 > 10000;
"""

query_df = pd.read_sql_query(query, connection)

print(query_df)

connection.close()