# src/import_to_postgres.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, Optional

import duckdb
import pandas as pd
from sqlalchemy import text
from sqlmodel import Session

from database import engine, init_db

# ---------- Mapping TLC -> snake_case Postgres ----------
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
    def __init__(self, chunksize_to_sql: int = 50_000):
        """
        chunksize_to_sql : sous-découpage interne utilisé par pandas.to_sql
        (chaque batch lu depuis Parquet pourra lui-même être découpé en paquets plus petits).
        """
        self.chunksize_to_sql = chunksize_to_sql
        init_db()  # crée les tables si absentes

    # ---------- Helpers SQL bruts ----------
    def is_file_imported(self, file_name: str) -> bool:
        stmt = text("SELECT 1 FROM import_log WHERE file_name = :f LIMIT 1").bindparams(f=file_name)
        with Session(engine) as s:
            return s.exec(stmt).first() is not None

    def log_import(self, file_name: str, rows: int) -> None:
        stmt = text("""
            INSERT INTO import_log (file_name, import_date, rows_imported)
            VALUES (:f, NOW(), :r)
            ON CONFLICT (file_name) DO NOTHING
        """).bindparams(f=file_name, r=rows)
        with Session(engine) as s:
            s.exec(stmt)
            s.commit()

    # ---------- Lecture Parquet par morceaux ----------
    def iter_parquet_batches(self, parquet_path: Path, batch_rows: int) -> Iterable[pd.DataFrame]:
        """
        Lit un fichier Parquet par morceaux (batch_rows lignes).
        Utilise DuckDB pour streamer en DataFrames successifs.
        """
        con = duckdb.connect()  # base temporaire en mémoire
        # Paramétrage du chemin via placeholder pour éviter les soucis de quoting
        con.execute("SELECT * FROM read_parquet(?)", [str(parquet_path)])

        while True:
            df = con.fetch_df_chunk(batch_rows)
            if df is None or df.empty:
                break
            # normaliser les noms de colonnes
            df.columns = [SNAKECASE_MAP.get(c, c.lower()) for c in df.columns]
            yield df

        con.close()

    # ---------- Import d'un fichier (batché) ----------
    def import_parquet(self, parquet_path: Path, batch_rows: int) -> int:
        from tqdm import tqdm  # import local pour ne pas charger si non utilisé ailleurs

        parquet_path = Path(parquet_path)
        if not parquet_path.is_file():
            print(f"[ERR ] Fichier introuvable: {parquet_path}")
            return 0

        fname = parquet_path.name
        if self.is_file_imported(fname):
            print(f"[SKIP] Déjà importé (log): {fname}")
            return 0

        # Estimation du total de lignes pour une barre de progression propre
        total_rows = duckdb.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(parquet_path)]
        ).fetchone()[0]

        print(f"[INFO] Import fichier: {fname} (batch_rows={batch_rows}, to_sql_chunksize={self.chunksize_to_sql})")
        imported_rows = 0

        # Barre de progression par fichier (en lignes)
        with tqdm(
            total=total_rows,
            unit="rows",
            unit_scale=True,
            desc=f"{fname}",
            ascii=True,              # plus propre dans les logs Docker
            dynamic_ncols=True,
            mininterval=0.5,
            disable=False,
        ) as pbar:
            for df in self.iter_parquet_batches(parquet_path, batch_rows=batch_rows):
                if df.empty:
                    continue

                # Insertion par batch
                df.to_sql(
                    "yellow_taxi_trips",
                    con=engine,
                    if_exists="append",
                    index=False,
                    chunksize=self.chunksize_to_sql,
                    method="multi",
                )

                imported_rows += len(df)
                pbar.update(len(df))

        # Log de fin de fichier
        self.log_import(fname, imported_rows)
        print(f"[DONE] {fname} -> total importé {imported_rows:,}".replace(",", " "))
        return imported_rows

    # ---------- Import d'un dossier ----------
    def import_all_parquet_files(
        self,
        data_dir: Path,
        batch_rows: int,
        max_files: Optional[int] = None,
        start_with: Optional[str] = None,
    ) -> int:
        from tqdm import tqdm

        data_dir = Path(data_dir)
        files = sorted(data_dir.rglob("*.parquet"))
        if start_with:
            files = [f for f in files if f.name >= start_with]
        if max_files:
            files = files[:max_files]

        print(f"[INFO] Dossier: {data_dir.resolve()}")
        print(f"[INFO] Fichiers trouvés: {len(files)}")
        if not files:
            return 0

        imported_files = 0

        # Barre de progression globale (par fichiers)
        with tqdm(
            total=len(files),
            desc="Fichiers",
            ascii=True,
            dynamic_ncols=True,
            mininterval=0.5,
        ) as filebar:
            for fp in files:
                rows = self.import_parquet(fp, batch_rows=batch_rows)
                if rows > 0:
                    imported_files += 1
                filebar.update(1)

        print(f"[RES ] Fichiers traités: {len(files)} | Nouveaux imports: {imported_files} | Déjà logués: {len(files)-imported_files}")
        return imported_files

    # ---------- Stats ----------
    def get_statistics(self) -> None:
        with Session(engine) as s:
            total = s.exec(text("SELECT COALESCE(COUNT(*),0) FROM yellow_taxi_trips")).scalar_one()
            files = s.exec(text("SELECT COALESCE(COUNT(*),0) FROM import_log")).scalar_one()
            min_dt, max_dt = s.exec(text("""
                SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips
            """)).first() or (None, None)

        print("\n===== STATISTIQUES POSTGRES =====")
        print(f"Total trajets  : {total:,}".replace(",", " "))
        print(f"Fichiers logués: {files}")
        print(f"Plage de dates : {min_dt} → {max_dt}")
        print("=================================\n")


def main():
    parser = argparse.ArgumentParser(description="Import Parquet TLC -> PostgreSQL en batches")
    parser.add_argument("--data-dir", type=Path, default=Path("src/data/raw"), help="Dossier avec les .parquet")
    parser.add_argument("--batch-rows", type=int, default=250_000, help="Taille d'un batch (lignes lues depuis Parquet)")
    parser.add_argument("--to-sql-chunksize", type=int, default=50_000, help="Sous-découpage côté pandas.to_sql")
    parser.add_argument("--max-files", type=int, default=None, help="Limiter le nombre de fichiers à importer")
    parser.add_argument("--start-with", type=str, default=None, help="Commencer à partir d'un nom de fichier (tri lexicographique)")
    args = parser.parse_args()

    importer = PostgresImporter(chunksize_to_sql=args.to_sql_chunksize)
    importer.import_all_parquet_files(
        data_dir=args.data_dir,
        batch_rows=args.batch_rows,
        max_files=args.max_files,
        start_with=args.start_with,
    )
    importer.get_statistics()


if __name__ == "__main__":
    main()
