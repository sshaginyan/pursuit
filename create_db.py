import sqlite3
import pandas as pd

connection = sqlite3.connect("data/pursuit.db")
cursor = connection.cursor()

# This will override some of my manually fixed records in places
# places_df = pd.read_csv('data/csv/enriched_places/part-00000-5342b3a6-325a-4aa7-a3c9-533fa2dc0ee1-c000.csv')
# places_df.to_sql("places", connection, if_exists='append', index=False)

cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_place_contact_place_id ON places(contact_place_id);
""")

contacts_df = pd.read_csv('data/csv/contacts.csv')
contacts_df.to_sql("contacts", connection, if_exists='replace', index=False)

cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_contacts_place_id ON contacts(place_id);
""")

techstacks_df = pd.read_csv('data/csv/techstacks.csv')
techstacks_df.to_sql("techstacks", connection, if_exists='replace', index=False)

cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_techstacks_place_id ON techstacks(place_id);
""")

cursor.execute("""
CREATE VIEW IF NOT EXISTS flat AS
SELECT 
    contacts.*,
    techstacks.*,
    places.*
FROM 
    contacts
JOIN 
    techstacks
ON 
    contacts.place_id = techstacks.place_id
JOIN 
    places
ON 
    places.contact_place_id = techstacks.place_id
""")

connection.close()