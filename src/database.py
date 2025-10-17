# src/database.py
from __future__ import annotations
import os
from sqlmodel import SQLModel, create_engine, Session, Field
from datetime import datetime
from typing import Generator
from dotenv import load_dotenv

# ========= Chargement du .env ==========
load_dotenv()  # <-- charge automatiquement le fichier .env

# ========= Configuration ==========
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "nyc_taxi")

DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = create_engine(DATABASE_URL, echo=False)



# ========= Modèles ==========
class YellowTaxiTrip(SQLModel, table=True):
    __tablename__ = "yellow_taxi_trips"

    vendorid: int | None = Field(default=None)
    tpep_pickup_datetime: datetime | None = Field(default=None)
    tpep_dropoff_datetime: datetime | None = Field(default=None)
    passenger_count: float | None = Field(default=None)
    trip_distance: float | None = Field(default=None)
    ratecodeid: float | None = Field(default=None)
    store_and_fwd_flag: str | None = Field(default=None)
    pu_location_id: int | None = Field(default=None)
    do_location_id: int | None = Field(default=None)
    payment_type: int | None = Field(default=None)
    fare_amount: float | None = Field(default=None)
    extra: float | None = Field(default=None)
    mta_tax: float | None = Field(default=None)
    tip_amount: float | None = Field(default=None)
    tolls_amount: float | None = Field(default=None)
    improvement_surcharge: float | None = Field(default=None)
    total_amount: float | None = Field(default=None)
    congestion_surcharge: float | None = Field(default=None)
    airport_fee: float | None = Field(default=None)
    cbd_congestion_fee: float | None = Field(default=None)


class ImportLog(SQLModel, table=True):
    __tablename__ = "import_log"

    file_name: str = Field(primary_key=True)
    import_date: datetime | None = Field(default_factory=datetime.utcnow)
    rows_imported: int | None = Field(default=None)


# ========= Helpers ==========
def get_db() -> Generator[Session, None, None]:
    """Dépendance FastAPI ou usage contextuel."""
    with Session(engine) as session:
        yield session


def init_db() -> None:
    """Création des tables si elles n’existent pas encore."""
    SQLModel.metadata.create_all(engine)
