from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session
from ..database import get_session
from ..schemas import TaxiTrip, TaxiTripCreate, TaxiTripUpdate, TaxiTripList
from ..services.yellow_taxi_trip_service import TaxiTripService

router = APIRouter(prefix="/trips", tags=["Trips"])

@router.get("/", response_model=TaxiTripList)
def list_trips(skip: int = Query(0), limit: int = Query(100), db: Session = Depends(get_session)):
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return {"total": total, "trips": trips}

@router.get("/{trip_id}", response_model=TaxiTrip)
def get_trip(trip_id: int, db: Session = Depends(get_session)):
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip

@router.post("/", response_model=TaxiTrip, status_code=201)
def create_trip(payload: TaxiTripCreate, db: Session = Depends(get_session)):
    return TaxiTripService.create_trip(db, payload)

@router.put("/{trip_id}", response_model=TaxiTrip)
def update_trip(trip_id: int, payload: TaxiTripUpdate, db: Session = Depends(get_session)):
    updated = TaxiTripService.update_trip(db, trip_id, payload)
    if not updated:
        raise HTTPException(status_code=404, detail="Trip not found")
    return updated

@router.delete("/{trip_id}", status_code=204)
def delete_trip(trip_id: int, db: Session = Depends(get_session)):
    ok = TaxiTripService.delete_trip(db, trip_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Trip not found")
    return None
