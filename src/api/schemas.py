from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, ConfigDict

# --- Trips ---
class TaxiTripBase(BaseModel):
    VendorID: Optional[int] = None
    tpep_pickup_datetime: Optional[datetime] = None
    tpep_dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[float] = None
    trip_distance: Optional[float] = None
    RatecodeID: Optional[float] = None
    store_and_fwd_flag: Optional[str] = None
    PULocationID: Optional[int] = None
    DOLocationID: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    Airport_fee: Optional[float] = None

class TaxiTripCreate(TaxiTripBase):
    pass

class TaxiTripUpdate(TaxiTripBase):
    pass

class TaxiTrip(TaxiTripBase):
    id: int
    model_config = ConfigDict(from_attributes=True)

class TaxiTripList(BaseModel):
    total: int
    trips: List[TaxiTrip]

# --- Stats ---
class Statistics(BaseModel):
    total_trips: int
    min_pickup: Optional[datetime] = None
    max_dropoff: Optional[datetime] = None
    avg_trip_distance: Optional[float] = None
    avg_total_amount: Optional[float] = None

# --- Pipeline ---
class PipelineResponse(BaseModel):
    imported_files: int
    stats: dict
    data_dir: str
