from pydantic import BaseModel


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
    startUTC: int
    endUTC: int
    visibility: float
