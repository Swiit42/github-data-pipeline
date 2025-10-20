from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field

class YellowTaxiTrip(SQLModel, table=True):
    __tablename__ = "yellow_taxi_trips"

    id: Optional[int] = Field(default=None, primary_key=True)
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
