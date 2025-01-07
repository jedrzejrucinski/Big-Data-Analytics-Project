from clients.mysql_client import MySQLClient
from dotenv import load_dotenv
import os
from config import EnvConfig
import numpy as np

load_dotenv()
config = EnvConfig(os.environ)

mysql_client = MySQLClient(
    config.mysql_host,
    "weather_admin",
    config.mysql_password,
    "weather_db",
)

def initialize_tables():
    with mysql_client as db:
        db.execute("SET FOREIGN_KEY_CHECKS = 0")
        # Truncate the cloud_cover_forecasts table to remove all rows and reset auto-increment
        db.execute("TRUNCATE TABLE cloud_cover_forecasts")

        # Truncate the locations table to remove all rows and reset auto-increment
        db.execute("TRUNCATE TABLE locations")
        db.execute("SET FOREIGN_KEY_CHECKS = 1")

        # Insert new rows into the locations table using a double loop
        insert_query = "INSERT INTO locations (latitude, longitude) VALUES (%s, %s)"
        
        for lat in np.arange(49.1, 55, 0.2):  
            for lon in np.arange(14.1, 24, 0.2): 
                values = (lat, lon)
                db.insert(insert_query, values)

        location_ids = db.read("SELECT id FROM locations")
        insert_query = """
INSERT INTO cloud_cover_forecasts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
        for location in location_ids:
            values = (location['id'],) + (0,) * 24
            db.insert(insert_query, values)

if __name__ == "__main__":
    initialize_tables()

