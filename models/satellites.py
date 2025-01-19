from pydantic import BaseModel, validator
from typing import List
import pandas as pd


class Satellite(BaseModel):
    id: int = 5
    name: str = "VANGUARD 1"


class SatelliteTrajectory(BaseModel):
    satid: int
    startUTC: int
    endUTC: int
    startAz: float
    endAz: float


class SatelliteVisibility(BaseModel):
    satellite: Satellite
    passes: List[SatelliteTrajectory]
    startUTC: pd.Timestamp
    endUTC: pd.Timestamp
    cloud_cover: List[int]

    @validator("startUTC", "endUTC", pre=True)
    def parse_timestamps(cls, value):
        if isinstance(value, int):
            return pd.to_datetime(value, unit="s")
        return value

    @validator("startUTC", "endUTC", each_item=True)
    def validate_timestamps(cls, value):
        if not isinstance(value, pd.Timestamp):
            raise ValueError("Invalid timestamp")
        return value
