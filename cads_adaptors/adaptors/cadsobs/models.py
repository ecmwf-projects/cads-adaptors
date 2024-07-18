from datetime import datetime
from typing import List, Literal

from pydantic import BaseModel

RetrieveFormat = Literal["netCDF", "csv"]


class RetrieveParams(BaseModel, extra="ignore"):
    dataset_source: str
    stations: None | List[str] = None
    variables: List[str] | None = None
    latitude_coverage: None | tuple[float, float] = None
    longitude_coverage: None | tuple[float, float] = None
    time_coverage: None | tuple[datetime, datetime] = None
    year: None | List[int] = None
    month: None | List[int] = None
    day: None | List[int] = None
    format: RetrieveFormat = "netCDF"


class RetrieveArgs(BaseModel):
    dataset: str
    params: RetrieveParams
