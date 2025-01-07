from pydantic import BaseModel


class Satellite(BaseModel):
    id: int
    name: str


class SatelliteTrajectory(BaseModel):
    satid: int
    startUtC: int
    endUTC: int
    startAz: float
    endAz: float
