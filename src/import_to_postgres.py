# src/import_to_postgres.py
from __future__ import annotations
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import duckdb
import pandas as pd
from sqlmodel import Session
from sqlalchemy import text
from database import engine, init_db


# ===== mapping des noms de colonnes =====
SNAKECASE_MAP: Dict[str, str] = {
    "VendorID": "vendorid",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecodeid",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


class PostgresImporter:
    def __init__(self, chunksize: int = 200_000):
        self.chunksize = chunksize
        init_db()

    def _normalize_columns(self, cols: List[str]) -> List[str]:
        return [SNAKECASE_MAP.get(c, c.lower()) for c in cols]

    def is_file_imported(self, file_name: str) -> bool:
        with Session(engine) as session:
            res = session.exec(text("SELECT 1 FROM import_log WHERE file_name = :f LIMIT 1"), {"f": file_name}).first()
            return res is not None

    def log_import(self, file_name: str, rows: int) -> None:
        with Session(engine) as session:
            session.exec(
                text("""
                INSERT INTO import_log (file_name, import_date, rows_imported)
                VALUES (:f, NOW(), :r)
                ON CONFLICT (file_name) DO NOTHING
                """),
                {"f": file_name, "r": rows},
            )
            session.commit()

    def import_parquet(self, parquet_path: Path) -> int:
        parquet_path = Path(parquet_path)
        if not parquet_path.is_file():
            print(f"[ERROR] Fichier introuvable : {parquet_path}")
            return 0

        fname = parquet_path.name
        if self.is_file_imported(fname):
            print(f"[SKIP] Déjà importé (log): {fname}")
            return 0

        # Lecture du parquet avec DuckDB
        df = duckdb.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
        df.columns = self._normalize_columns(df.columns)

        # Insertion dans Postgres via SQLModel / SQLAlchemy
        df.to_sql(
            "yellow_taxi_trips",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=self.chunksize,
            method="multi",
        )

        rows = len(df)
        self.log_import(fname, rows)
        print(f"[OK] {fname} -> +{rows:,} lignes importées")
        return rows

    def import_all_parquet_files(self, data_dir: Path) -> int:
        data_dir = Path(data_dir)
        files = sorted(data_dir.rglob("*.parquet"))
        if not files:
            print(f"[INFO] Aucun fichier .parquet trouvé dans {data_dir.resolve()}")
            return 0

        imported = 0
        for f in files:
            if self.import_parquet(f):
                imported += 1
        print(f"[INFO] {imported}/{len(files)} fichiers importés.")
        return imported

    def get_statistics(self) -> None:
        with Session(engine) as session:
            total = session.exec(text("SELECT COUNT(*) FROM yellow_taxi_trips")).first() or 0
            files = session.exec(text("SELECT COUNT(*) FROM import_log")).first() or 0
            min_max = session.exec(
                text("SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips")
            ).first()

        print("\n===== STATISTIQUES POSTGRES =====")
        print(f"Total trajets  : {total:,}".replace(",", " "))
        print(f"Fichiers logués: {files}")
        if min_max and all(min_max):
            print(f"Plage de dates : {min_max[0]} → {min_max[1]}")
        print("=================================\n")


def main():
    parser = argparse.ArgumentParser(description="Import des fichiers Parquet dans PostgreSQL (via SQLModel)")
    parser.add_argument("--data-dir", type=Path, default=Path("src/data/raw"), help="Dossier contenant les Parquet")
    args = parser.parse_args()

    importer = PostgresImporter()
    importer.import_all_parquet_files(args.data_dir)
    importer.get_statistics()


if __name__ == "__main__":
    main()
