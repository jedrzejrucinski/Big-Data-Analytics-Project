from pydantic import BaseModel


class WeatherRequest(BaseModel):
    latitude: float
    longitude: float


class SatelliteRequest(BaseModel):
    sat_id: int
    observer_lat: float
    observer_lon: float
    observer_alt: float
    seconds: int = 3600
