from pydantic import BaseModel


class WeatherRequest(BaseModel):
    latitude: float
    longitude: float


class SatelliteRequest(BaseModel):
    sat_id: int = 25544
    observer_lat: float = 51.759445
    observer_lng: float = 19.457216
    observer_alt: float = 100.0
    days: int = 7
    min_visibility: int = 5
