from fastapi import FastAPI, HTTPException
from models import WeatherRequest, SatelliteRequest
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import requests
import os
from config import EnvConfig
from dotenv import load_dotenv

load_dotenv()

config = EnvConfig(os.environ)

# Setup the Open-Meteo API client with cache and retry on error


app = FastAPI()


def send_to_nifi(data, endpoint):
    # Convert Timestamps to strings
    for record in data:
        if "date" in record:
            record["date"] = record["date"].isoformat()
    headers = {"Content-Type": "application/json"}
    response = requests.post(endpoint, json=data, headers=headers)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)


@app.post("/openmeteo", tags=["weather"])
def get_openmeteo_data(request: WeatherRequest):
    """
    Fetch weather data for a specific location.

    Args:
        request (WeatherRequest): WeatherRequest object.

    Returns:
        dict: Weather data as JSON.
    """
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = config.openmeteo_api_url
    params = {
        "latitude": request.latitude,
        "longitude": request.longitude,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "cloud_cover",
            "surface_pressure",
            "wind_speed_10m",
            "wind_direction_10m",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)

    response = responses[0]
    # Current values. The order of variables needs to be the same as requested.
    current = response.Current()

    current_temperature_2m = current.Variables(0).Value()
    current_relative_humidity_2m = current.Variables(1).Value()
    current_precipitation = current.Variables(2).Value()
    current_cloud_cover = current.Variables(3).Value()
    current_surface_pressure = current.Variables(4).Value()
    current_wind_speed_10m = current.Variables(5).Value()
    current_wind_direction_10m = current.Variables(6).Value()
    data = [
        {
            "latitude": request.latitude,
            "longitude": request.longitude,
            "current_temperature_2m": current_temperature_2m,
            "current_relative_humidity_2m": current_relative_humidity_2m,
            "current_precipitation": current_precipitation,
            "current_cloud_cover": current_cloud_cover,
            "current_surface_pressure": current_surface_pressure,
            "current_wind_speed_10m": current_wind_speed_10m,
            "current_wind_direction_10m": current_wind_direction_10m,
        }
    ]

    port = "8082"
    send_to_nifi(data, config.nifi_base_url + f":{port}/openmeteo")

    return {"message": "Weather data sent to NiFi successfully"}


@app.post("/openweather", tags=["weather"])
@app.post("/openweather", tags=["weather"])
def get_openweather_data(request: WeatherRequest):
    """
    Fetch weather data for a specific location.

    Args:
        request (WeatherRequest): WeatherRequest object.

    Returns:
        dict: Weather data as JSON.
    """
    params = {
        "lat": request.latitude,
        "lon": request.longitude,
        "appid": config.openweather_api_key,
        "units": "metric",
    }

    response = requests.get(config.openweather_api_url, params=params)
    if response.status_code == 200:
        weather_data = response.json()
        data = [
            {
                "latitude": request.latitude,
                "longitude": request.longitude,
                "current_temperature_2m": weather_data["main"]["temp"],
                "current_relative_humidity_2m": weather_data["main"]["humidity"],
                "current_precipitation": weather_data.get("rain", {}).get("1h", 0),
                "current_cloud_cover": weather_data["clouds"]["all"],
                "current_surface_pressure": weather_data["main"]["pressure"],
                "current_wind_speed_10m": weather_data["wind"]["speed"],
                "current_wind_direction_10m": weather_data["wind"]["deg"],
            }
        ]

        port = "8083"
        send_to_nifi(data, config.nifi_base_url + f":{port}/openweather")
        return {"message": "Weather data sent to NiFi successfully"}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.json())


@app.post("/weatherapi", tags=["weather"])
def get_weather_data(request: WeatherRequest):
    """
    Fetch weather data for a specific location.

    Args:
        request (WeatherRequest): WeatherRequest object.

    Returns:
        dict: Weather data as JSON.
    """
    endpoint = f"{config.weather_api_url}/current.json"
    params = {
        "q": f"{request.latitude},{request.longitude}",
        "key": config.weather_api_key,
        "aqi": "no",
    }

    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        weather_data = response.json()
        current = weather_data["current"]
        data = [
            {
                "latitude": request.latitude,
                "longitude": request.longitude,
                "current_temperature_2m": current["temp_c"],
                "current_relative_humidity_2m": current["humidity"],
                "current_precipitation": current["precip_mm"],
                "current_cloud_cover": current["cloud"],
                "current_surface_pressure": current["pressure_mb"],
                "current_wind_speed_10m": current["wind_kph"],
                "current_wind_direction_10m": current["wind_degree"],
            }
        ]

        port = "8084"
        send_to_nifi(data, config.nifi_base_url + f":{port}/weatherapi")
        return {"message": "Weather data sent to NiFi successfully"}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.json())


@app.post("/satellite", tags=["satellite"])
def get_satellite_data(request: SatelliteRequest):
    """
    Endpoint to get satellite data and send it to NiFi.

    Args:
        request (SatelliteRequest): The request object containing observer's latitude, longitude, altitude, number of days, and minimum visibility.

    Returns:
        dict: A message indicating whether the satellite data was sent to NiFi successfully.

    Raises:
        HTTPException: If the response status code is not 200, an HTTPException is raised with the response details.
    """
    endpoint = f"{config.ny2o_api_url}/visualpasses/{request.sat_id}/{request.observer_lat}/{request.observer_lng}/{request.observer_alt}/{request.days}/{request.min_visibility}/"
    params = {"apiKey": config.ny2o_api_key}
    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        data = response.json()
        filtered_data = [
            {
                "passescount": data["info"]["passescount"],
                "sat_id": request.sat_id,
                "sat_name": data["info"]["satname"],
                "startUTC": pass_["startUTC"],
                "endUTC": pass_["endUTC"],
                "duration": pass_["duration"],
                "startAz": pass_["startAz"],
                "endAz": pass_["endAz"],
            }
            for pass_ in data["passes"]
        ]

        port = "8081"
        send_to_nifi(filtered_data, config.nifi_base_url + f":{port}/satellite")
        return {"message": "Satellite data sent to NiFi successfully"}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.json())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
