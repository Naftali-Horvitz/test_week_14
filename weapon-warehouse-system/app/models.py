from pydantic import BaseModel


class Weapons(BaseModel):
    weapon_id: str
    weapon_name: str
    weapon_type: str 
    range_km: int
    weight_kg: float
    manufacturer: str | None
    origin_country: str
    storage_location: str 
    year_estimated: int