from pydantic import BaseModel
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
