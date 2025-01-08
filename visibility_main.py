from fastapi import FastAPI, HTTPException
import pandas as pd
from retry_requests import retry
from config import EnvConfig
from dotenv import load_dotenv
from clients.mysql_client import MySQLClient
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


@app.post("/visibility_of_satellite", tags=["visibility"])
def get_visibility_of_satellite(
    satellite: Satellite, location: Location
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
    current_time = pd.Timestamp.now("UTC").tz_convert("Europe/Warsaw")
    start_time = convert_utc_to_local(trajectory.startUTC)
    end_time = convert_utc_to_local(trajectory.endUTC)
    if current_time < start_time or current_time > end_time:
        raise HTTPException(status_code=404, detail="Satellite not visible")
    if start_time.day != current_time.day:
        forecast_window = (24 - start_time.hour) + current_time.hour
    else:
        forecast_window = current_time.hour - start_time.hour

    return SatelliteVisibility(
        satellite=satellite,
        startUTC=trajectory.startUTC,
        endUTC=trajectory.endUTC,
        visibility=forecast.forecast[forecast_window],
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
