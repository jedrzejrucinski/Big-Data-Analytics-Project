from clients.mysql_client import MySQLClient
from dotenv import load_dotenv
import os
from config import EnvConfig

load_dotenv()
config = EnvConfig(os.environ)

mysql_client = MySQLClient(
    config.mysql_host,
    "weather_admin",
    config.mysql_password,
    "weather_db",
)
def update_forecasts():
    with mysql_client as db:
        # Get all IDs from the locations table
        location_ids = db.read("SELECT id FROM locations")

        # Remove all rows from the cloud_cover_forecasts table
        db.execute("TRUNCATE TABLE cloud_cover_forecasts")

        # Insert new rows with default values
        insert_query = """
INSERT INTO cloud_cover_forecasts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

        for location in location_ids:
            values = (location['id'],) + (0,) * 24
            db.insert(insert_query, values)

if __name__ == "__main__":
    update_forecasts()

