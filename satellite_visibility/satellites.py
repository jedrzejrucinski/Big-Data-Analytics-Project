from pydantic import BaseModel


class Satellite(BaseModel):
    id: int
    name: str


class SatelliteTrajectory(BaseModel):
    satid: int
    startUtC: str
    endUTC: str
    startAz: float
    endAz: float
