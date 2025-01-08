from fastapi import FastAPI, HTTPException
import pandas as pd
from retry_requests import retry
from config import EnvConfig
from dotenv import load_dotenv
from clients.mysql_client import MySQLClient
from clients.cosmos_db import CosmosDBClient
import os
from models.satellites import Satellite, SatelliteTrajectory, SatelliteVisibility
from models.weather import Location, WeatherForecast

load_dotenv()

config = EnvConfig(os.environ)

app = FastAPI()
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
cosmos_db_client = CosmosDBClient(config=config)


def get_satellite_trajectory(satellite: Satellite) -> SatelliteTrajectory:
    """
    Get satellite trajectory data.
    Args:
        satellite (Satellite): Satellite object.
    Returns:
        SatelliteTrajectory: Satellite trajectory data.
    """
    query = "SELECT satid, startUTC, endUTC, startAz, endAz FROM trajectories WHERE satid=%s"
    values = (satellite.id,)
    with satellite_mysql_client as db:
        data = db.read(query, values)
    if not data:
        raise HTTPException(status_code=404, detail="Satellite not found")
    return SatelliteTrajectory(**data[0])


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
        WHERE endUTC > %s AND endUTC < (%s + INTERVAL 1 HOUR)
    ) AS candidates
    WHERE startUTC < %s;
    """
    values = (end_time, end_time, start_time)
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


@app.post("/visibility_of_satellite", tags=["visibility"])
def get_visibility_of_satellite(
    satellite: Satellite, location: Location, time: int = None
) -> SatelliteVisibility:
    """
    Get visibility of satellite.
    Args:
        satellite (Satellite): Satellite object.
        location (Location): Location object.
    Returns:
        dict: Visibility of satellite.
    """
    trajectory = get_satellite_trajectory(satellite)
    forecast = get_weather_forecast(location)
    if not time:
        time = pd.Timestamp.now("UTC").tz_convert("Europe/Warsaw")
    else:
        time = convert_utc_to_local(time)
    start_time = convert_utc_to_local(trajectory.startUTC)
    end_time = convert_utc_to_local(trajectory.endUTC)
    if time < start_time or time > end_time:
        raise HTTPException(status_code=404, detail="Satellite not visible")
    if start_time.day != time.day:
        forecast_window = (24 - start_time.hour) + time.hour
    else:
        forecast_window = time.hour - start_time.hour + 1

    result = SatelliteVisibility(
        satellite=satellite,
        startUTC=trajectory.startUTC,
        endUTC=trajectory.endUTC,
        visibility=get_forecast_value(forecast, forecast_window),
    )

    cosmos_db_client.add_item(result.dict())

    return result


if __name__ == "__main__":
    # import uvicorn

    # uvicorn.run(app, host="0.0.0.0", port=8000)
    satellites = get_satellites_in_time_range(
        start_time=1622505600, end_time=1622592000
    )
    for satellite in satellites:
        print(satellite)
