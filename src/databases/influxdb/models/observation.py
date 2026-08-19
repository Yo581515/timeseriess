from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass
class Observation:
    time: datetime
    node_source: str
    node_source_id: str
    latitude: float
    longitude: float
    sensor_source: str
    sensor_source_id: str
    parameter: str
    value: float
    unit: str
    quality_codes: List[int] = field(default_factory=list)

    def quality_codes_str(self) -> str:
        return "[" + ",".join(str(q) for q in self.quality_codes) + "]"

    def __str__(self) -> str:
        return (
            f"Observation(\n"
            f"  time             = {self.time}\n"
            f"  node_source      = {self.node_source}\n"
            f"  node_source_id   = {self.node_source_id}\n"
            f"  latitude         = {self.latitude}\n"
            f"  longitude        = {self.longitude}\n"
            f"  sensor_source    = {self.sensor_source}\n"
            f"  sensor_source_id = {self.sensor_source_id}\n"
            f"  parameter        = {self.parameter}\n"
            f"  value            = {self.value}\n"
            f"  unit             = {self.unit}\n"
            f"  quality_codes    = {self.quality_codes}\n"
            f")"
        )
