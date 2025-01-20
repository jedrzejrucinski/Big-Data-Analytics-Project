from pydantic import BaseModel


class Location(BaseModel):
    id: int = 678
    latitude: float = 51.7
    longitude: float = 19.5


class WeatherForecast(BaseModel):
    location_id: int
    forecast_hour_1: float
    forecast_hour_2: float
    forecast_hour_3: float
    forecast_hour_4: float
    forecast_hour_5: float
    forecast_hour_6: float
    forecast_hour_7: float
    forecast_hour_8: float
    forecast_hour_9: float
    forecast_hour_10: float
    forecast_hour_11: float
    forecast_hour_12: float
    forecast_hour_13: float
    forecast_hour_14: float
    forecast_hour_15: float
    forecast_hour_16: float
    forecast_hour_17: float
    forecast_hour_18: float
    forecast_hour_19: float
    forecast_hour_20: float
    forecast_hour_21: float
    forecast_hour_22: float
    forecast_hour_23: float
    forecast_hour_24: float
