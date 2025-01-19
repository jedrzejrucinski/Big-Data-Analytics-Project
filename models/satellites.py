from pydantic import BaseModel, validator
from typing import List
import pandas as pd


class Satellite(BaseModel):
    id: int = 10395
    name: str = "DELTA 1 DEB"


class SatelliteTrajectory(BaseModel):
    satid: int
    startUTC: int
    endUTC: int
    startAz: float
    endAz: float


class SatelliteVisibility(BaseModel):
    satellite: Satellite
    passes: List[SatelliteTrajectory]
    cloud_cover: List[int]


class VisibleSatellites(BaseModel):
    satellites: List[Satellite]
    passes: List[SatelliteTrajectory]
    cloud_cover: int
