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
        "hourly": "temperature_2m",
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["temperature_2m"] = hourly_temperature_2m

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    data = hourly_dataframe.to_dict(orient="records")
    print(data)
    port = "8082"
    send_to_nifi(data, config.nifi_base_url + f":{port}/weather")
    return {"message": "Weather data sent to NiFi successfully"}


@app.post("/openweather", tags=["weather"])
def get_openweather_data(city):
    """
    Fetch weather data for a given city.

    Args:
        city (str): Name of the city.

    Returns:
        dict: Weather data as JSON.
    """
    params = {
        "q": city,
        "appid": config.openweather_api_key,
        "units": "metric",
    }

    response = requests.get(config.openweather_api_url, params=params)
    if response.status_code == 200:
        port = "8082"
        send_to_nifi(response.json(), config.nifi_base_url + f":{port}/weather")
        return {"message": "Weather data sent to NiFi successfully"}
    else:
        print(f"Error: {response.status_code}, {response.json()}")
        return None


@app.post("/weatherapi", tags=["weather"])
def get_weather_data(city):
    """
    Fetch weather data for a specific city.

    Args:
        city (str): Name of the city.

    Returns:
        dict: Weather data as JSON.
    """
    endpoint = f"{config.weather_api_url}/current.json"
    params = {
        "key": config.weather_api_key,
        "q": city,
        "aqi": "no",
    }

    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        port = "8082"
        send_to_nifi(response.json(), config.nifi_base_url + f":{port}/weather")
        return {"message": "Weather data sent to NiFi successfully"}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.json())


@app.post("/satellite", tags=["satellite"])
def get_satellite_data(request: SatelliteRequest):
    endpoint = f"{config.ny2o_api_url}/positions/{request.sat_id}/{request.observer_lat}/{request.observer_lon}/{request.observer_alt}/{request.seconds}/"
    params = {"apiKey": config.ny2o_api_key}
    response = requests.get(endpoint, params=params)
    if response.status_code == 200:
        data = response.json()
        port = "8081"
        send_to_nifi(data, config.nifi_base_url + f":{port}/satellite")
        return {"message": "Satellite data sent to NiFi successfully"}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.json())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
