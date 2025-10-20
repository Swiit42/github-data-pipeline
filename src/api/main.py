from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .database import init_db
from .routes.yellow_taxi_trip_routes import router as trips_router
from .routes.import_log_routes import router as imports_router
from .routes.statistic_routes import router as stats_router

app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="CRUD PostgreSQL + statistiques + import pipeline Parquet",
    version="1.0.0"
)

# CORS (ouvert, à ajuster en production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

init_db()

app.include_router(trips_router, prefix="/api/v1")
app.include_router(imports_router, prefix="/api/v1")
app.include_router(stats_router, prefix="/api/v1")

@app.get("/", tags=["Meta"])
def root():
    return {"message": "NYC Taxi Data Pipeline API"}

@app.get("/health", tags=["Meta"])
def health():
    return {"status": "ok"}
