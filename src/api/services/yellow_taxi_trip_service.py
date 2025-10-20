from typing import Tuple, Optional, List
from sqlmodel import Session, select, func
from ..models.yellow_taxi_trip import YellowTaxiTrip
from ..schemas import TaxiTripCreate, TaxiTripUpdate, Statistics

class TaxiTripService:
    @staticmethod
    def get_trip(db: Session, trip_id: int) -> Optional[YellowTaxiTrip]:
        return db.get(YellowTaxiTrip, trip_id)

    @staticmethod
    def get_trips(db: Session, skip: int, limit: int) -> Tuple[List[YellowTaxiTrip], int]:
        total = db.exec(select(func.count()).select_from(YellowTaxiTrip)).one()
        trips = db.exec(select(YellowTaxiTrip).offset(skip).limit(limit)).all()
        return trips, total

    @staticmethod
    def create_trip(db: Session, trip: TaxiTripCreate) -> YellowTaxiTrip:
        obj = YellowTaxiTrip(**trip.model_dump())
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj

    @staticmethod
    def update_trip(db: Session, trip_id: int, trip: TaxiTripUpdate) -> Optional[YellowTaxiTrip]:
        obj = db.get(YellowTaxiTrip, trip_id)
        if not obj:
            return None
        for k, v in trip.model_dump(exclude_unset=True).items():
            setattr(obj, k, v)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj

    @staticmethod
    def delete_trip(db: Session, trip_id: int) -> bool:
        obj = db.get(YellowTaxiTrip, trip_id)
        if not obj:
            return False
        db.delete(obj)
        db.commit()
        return True

    @staticmethod
    def get_statistics(db: Session) -> Statistics:
        total = db.exec(select(func.count()).select_from(YellowTaxiTrip)).one()
        min_pickup = db.exec(select(func.min(YellowTaxiTrip.tpep_pickup_datetime))).one()
        max_dropoff = db.exec(select(func.max(YellowTaxiTrip.tpep_dropoff_datetime))).one()
        avg_dist = db.exec(select(func.avg(YellowTaxiTrip.trip_distance))).one()
        avg_total = db.exec(select(func.avg(YellowTaxiTrip.total_amount))).one()

        return Statistics(
            total_trips=total or 0,
            min_pickup=min_pickup,
            max_dropoff=max_dropoff,
            avg_trip_distance=float(avg_dist) if avg_dist else None,
            avg_total_amount=float(avg_total) if avg_total else None,
        )
