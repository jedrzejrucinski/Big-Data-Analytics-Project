from fastapi import FastAPI, HTTPException
import pandas as pd
from typing import List
from retry_requests import retry
from config import EnvConfig
from dotenv import load_dotenv
from clients.mysql_client import MySQLClient
from clients.cosmos_db import CosmosDBClient
import os
from models.satellites import Satellite, SatelliteTrajectory, SatelliteVisibility
from models.weather import Location, WeatherForecast
import time
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

config = EnvConfig(os.environ)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://13.74.48.118:3000"],
    allow_credentials=True,
    allow_methods=["*"],  # Zezwalaj na wszystkie metody HTTP (GET, POST itp.)
    allow_headers=["*"],  # Zezwalaj na wszystkie nagłówki
)

satellite_mysql_client = MySQLClient(
    config.mysql_host,
    "satellite_admin",
    config.mysql_password,
    "satellite_db",
)

weather_mysql_client = MySQLClient(
    config.mysql_host,
    "weather_admin",
    config.mysql_password,
    "weather_db",
)
cosmos_db_client_1 = CosmosDBClient(
    config=config, container_name="satellite_visibility_1"
)
cosmos_db_client_2 = CosmosDBClient(
    config=config, container_name="satellite_visibility_2"
)


def get_satellite_trajectory(
    satellite: Satellite, startUTC: int, endUTC: int
) -> List[SatelliteTrajectory]:
    """
    Get satellite trajectory data.
    Args:
        satellite (Satellite): Satellite object.
    Returns:
        SatelliteTrajectory: Satellite trajectory data.
    """
    query = """
        SELECT * FROM (
        SELECT satid, startUTC, endUTC, startAz, endAz FROM trajectories
        WHERE endUTC > %s AND endUTC < (%s + 1200)
    ) AS candidates
    WHERE startUTC < %s AND satid = %s;
    """
    values = (
        startUTC,
        endUTC,
        endUTC,
        satellite.id,
    )
    with satellite_mysql_client as db:
        data = db.read(query, values)
    if not data:
        raise HTTPException(status_code=404, detail="Satellite not found")
    return [SatelliteTrajectory(**item) for item in data]


def get_satellites_in_time_range(start_time: int, end_time: int) -> list[Satellite]:
    """
    Get satellites in time range.
    Args:
        start_time (int): Start time.
        end_time (int): End time.
    Returns:
        list[Satellite]: List of satellites.
    """
    query = """
    SELECT * FROM (
        SELECT satid, startUTC, endUTC, startAz, endAz FROM trajectories
        WHERE endUTC > %s AND endUTC < (%s + 600)
    ) AS candidates
    WHERE startUTC < %s;
    """
    values = (start_time, end_time, end_time)
    with satellite_mysql_client as db:
        data = db.read(query, values)
    if not data:
        raise HTTPException(status_code=404, detail="Satellites not found")
    return [Satellite(id=item["satid"]) for item in data]


def get_weather_forecast(location: Location) -> WeatherForecast:
    """
    Get weather forecast data.
    Args:
        location (Location): Location object.
    Returns:
        WeatherForecast: Weather forecast data.
    """
    query = "SELECT * from cloud_cover_forecasts WHERE location_id=%s"
    values = (location.id,)
    with weather_mysql_client as db:
        data = db.read(query, values)
    if not data:
        raise HTTPException(status_code=404, detail="Location not found")
    return WeatherForecast(**data[0])


def convert_utc_to_local(utc_time: int) -> pd.Timestamp:
    """
    Convert UTC time to local time.
    Args:
        utc_time (pd.Timestamp): UTC time.
        location (Location): Location object.
    Returns:
        pd.Timestamp: Local time.
    """
    return (
        pd.to_datetime(utc_time, unit="s")
        .tz_localize("UTC")
        .tz_convert("Europe/Warsaw")
    )


def get_forecast_value(forecast: WeatherForecast, forecast_window: int) -> float:
    attribute_name = f"forecast_hour_{forecast_window}"
    return getattr(forecast, attribute_name)


def get_visibility_of_satellite(
    satellite: Satellite, location: Location, startUTC: int, endUTC: int
) -> SatelliteVisibility:
    """
    Get visibility of satellite.
    Args:
        satellite (Satellite): Satellite object.
        location (Location): Location object.
    Returns:
        dict: Visibility of satellite.
    """
    trajectory = get_satellite_trajectory(satellite, startUTC, endUTC)
    forecast = get_weather_forecast(location)
    current_time = 1736388180
    start_forecast = (startUTC - current_time) // 3600
    end_forecast = (endUTC - current_time) // 3600

    relevant_forecast = [
        get_forecast_value(forecast, i + 1)
        for i in range(start_forecast, end_forecast + 1)
    ]

    return SatelliteVisibility(
        satellite=satellite,
        passes=trajectory,
        cloud_cover=relevant_forecast,
    )


@app.post("/visibility_of_satellite", tags=["visibility"])
def _get_visibility_of_satellite(
    satellite: Satellite, location: Location, startUTC: int, endUTC: int
) -> SatelliteVisibility:
    """
    Get visibility of satellite.
    Args:
        satellite (Satellite): Satellite object.
        location (Location): Location object.
    Returns:
        dict: Visibility of satellite.
    """

    result = get_visibility_of_satellite(satellite, location, startUTC, endUTC)
    print(result)
    cosmos_db_client_1.add_item(result.dict())

    return result


@app.post("/visibile_satellites", tags=["visibility"])
def _get_visibile_satellites(
    location: Location, start_time: int = 1736166360, end_time: int = 1736801390
) -> list[SatelliteVisibility]:
    """
    Get visibile satellites.
    Args:
        location (Location): Location object.
        start_time (int): Start time.
        end_time (int): End time.
    Returns:
        list[SatelliteVisibility]: List of visibile satellites.
    """
    satellites = get_satellites_in_time_range(start_time, end_time)
    result = [
        get_visibility_of_satellite(satellite, location, (start_time + end_time) / 2)
        for satellite in satellites
    ]
    for item in result:
        cosmos_db_client_2.add_item(item.dict())
    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
