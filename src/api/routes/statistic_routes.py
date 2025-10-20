from fastapi import APIRouter, Depends
from sqlmodel import Session
from ..database import get_session
from ..schemas import Statistics
from ..services.yellow_taxi_trip_service import TaxiTripService

router = APIRouter(prefix="/statistics", tags=["Statistics"])

@router.get("/", response_model=Statistics)
def get_statistics(db: Session = Depends(get_session)):
    return TaxiTripService.get_statistics(db)
