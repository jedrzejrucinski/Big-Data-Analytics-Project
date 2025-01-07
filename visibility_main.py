from fastapi import FastAPI, HTTPException
import pandas as pd
from retry_requests import retry
from config import EnvConfig
from dotenv import load_dotenv
from clients.mysql_client import MySQLClient
import os
from satellite_visibility.satellites import Satellite, SatelliteTrajectory

load_dotenv()

config = EnvConfig(os.environ)

app = FastAPI()
mysql_client = MySQLClient(
    config.mysql_host,
    "satellite_admin",
    config.mysql_password,
    "satellite_db",
)


def get_satellite_trajectory(satellite: Satellite) -> SatelliteTrajectory:
    """
    Get satellite trajectory data.
    Args:
        satellite (Satellite): Satellite object.
    Returns:
        SatelliteTrajectory: Satellite trajectory data.
    """
    query = "SELECT satellite_id, startUTC, endUTC, startAz, endAz FROM trajectories WHERE satid=%s"
    values = (satellite.satid,)
    with mysql_client as db:
        data = db.read(query, values)
    if not data:
        raise HTTPException(status_code=404, detail="Satellite not found")
    return SatelliteTrajectory(**data[0])


@app.get("/visibility_at_location", tags=["visibility"])
def get_visibility_at_location(lat: float, lon: float):
    """
    Get visibility at a specific location.

    Args:
        lat (float): Latitude.
        lon (float): Longitude.

    Returns:
        dict: Visibility data as JSON.
    """
    # Get visibility data from the Open-Meteo API

    pass


if __name__ == "__main__":
    satellite = Satellite(satid=2, name="SPUTNIK 1")
    trajectory = get_satellite_trajectory(satellite)
    print(trajectory)
    # import uvicorn

    # uvicorn.run(app, host="0.0.0.0", port=8000)
