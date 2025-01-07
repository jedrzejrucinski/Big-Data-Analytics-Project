from pydantic import BaseModel


class Satellite(BaseModel):
    id: int
    name: str


class SatelliteTrajectory(BaseModel):
    satid: int
    startUTC: int
    endUTC: int
    startAz: float
    endAz: float
