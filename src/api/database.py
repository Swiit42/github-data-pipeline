from sqlmodel import SQLModel, create_engine, Session
from .settings import settings

def _pg_url() -> str:
    return (
        f"postgresql://{settings.PG_USER}:{settings.PG_PASSWORD}"
        f"@{settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB}"
    )

engine = create_engine(_pg_url(), echo=False)

def init_db() -> None:
    from .models.yellow_taxi_trip import YellowTaxiTrip
    from .models.import_log import ImportLog
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session
