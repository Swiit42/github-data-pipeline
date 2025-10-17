from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb


YELLOW_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
    VendorID BIGINT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag VARCHAR,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE
);
"""

IMPORT_LOG_SQL = """
CREATE TABLE IF NOT EXISTS import_log (
    file_name VARCHAR PRIMARY KEY,
    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rows_imported BIGINT
);
"""


class DuckDBImporter:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        # Connexion
        self.conn = duckdb.connect(str(self.db_path))
        # Intégrité (journal WAL activé par défaut en 0.10)
        self._initialize_database()

    def _initialize_database(self) -> None:
        self.conn.execute(YELLOW_SCHEMA_SQL)
        self.conn.execute(IMPORT_LOG_SQL)

    # ---------- Anti-doublon ----------
    def is_file_imported(self, filename: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM import_log WHERE file_name = ? LIMIT 1;", [filename]
        ).fetchone()
        return row is not None

    # ---------- Import d'un fichier ----------
    def import_parquet(self, file_path: Path) -> bool:
        file_path = Path(file_path)
        fname = file_path.name

        if self.is_file_imported(fname):
            print(f"[SKIP] {fname} déjà importé (import_log).")
            return True

        # Compte initial
        before = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[0]

        # Transaction pour une importation atomique
        try:
            self.conn.execute("BEGIN;")
            # Astuce DuckDB: BY NAME aligne les colonnes par nom et ignore les colonnes en plus.
            self.conn.execute(
                "INSERT INTO yellow_taxi_trips BY NAME SELECT * FROM read_parquet(?);",
                [str(file_path)],
            )
            # Commit
            self.conn.execute("COMMIT;")
        except Exception as e:
            self.conn.execute("ROLLBACK;")
            print(f"[ERR ] Échec import {fname}: {e}")
            return False

        after = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[0]
        rows_imported = int(after - before)

        # Log d'import (même si 0 lignes, on note le passage pour ne pas retenter)
        self.conn.execute(
            "INSERT INTO import_log(file_name, import_date, rows_imported) VALUES (?, CURRENT_TIMESTAMP, ?);",
            [fname, rows_imported],
        )

        print(f"[OK  ] {fname} -> {rows_imported} lignes")
        return True

    # ---------- Import batch ----------
    def import_all_parquet_files(self, data_dir: Path) -> int:
        data_dir = Path(data_dir)
        files = sorted(data_dir.glob("*.parquet"))
        if not files:
            print(f"[INFO] Aucun .parquet trouvé dans: {data_dir.resolve()}")
            return 0

        imported = 0
        for fp in files:
            if self.import_parquet(fp):
                imported += 1

        print(f"[INFO] Fichiers traités: {len(files)} | Importés (ou déjà logués): {imported}")
        return imported

    # ---------- Statistiques ----------
    def get_statistics(self) -> None:
        total = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[0]
        nb_files = self.conn.execute("SELECT COUNT(*) FROM import_log;").fetchone()[0]

        # Min/Max sur la colonne pickup
        # (si table vide, fetchone() renvoie (None, None))
        min_max = self.conn.execute(
            "SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips;"
        ).fetchone()
        min_dt, max_dt = min_max if min_max else (None, None)

        db_size_bytes: Optional[int] = None
        try:
            db_size_bytes = os.path.getsize(self.db_path)
        except OSError:
            pass

        def _fmt_size(n: Optional[int]) -> str:
            if n is None:
                return "n/a"
            for unit in ("B", "KB", "MB", "GB", "TB"):
                if n < 1024:
                    return f"{n:.1f} {unit}"
                n /= 1024
            return f"{n:.1f} PB"

        print("\n===== STATISTIQUES DUCKDB =====")
        print(f"Base           : {self.db_path.resolve()}")
        print(f"Total trajets  : {total:,}".replace(",", " "))
        print(f"Fichiers importés (log) : {nb_files}")
        print(f"Plage de dates : {min_dt}  →  {max_dt}")
        print(f"Taille fichier : {_fmt_size(db_size_bytes)}")
        print("================================\n")

    def close(self) -> None:
        self.conn.close()


def main():
        parser = argparse.ArgumentParser(description="Importe des fichiers Parquet Yellow Taxi dans DuckDB.")
        parser.add_argument(
            "--db",
            type=Path,
            default=Path("data/warehouse/yellow_taxi.duckdb"),
            help="Chemin du fichier DuckDB (.duckdb)",
        )
        parser.add_argument(
            "--data-dir",
            type=Path,
            default=Path("data/raw"),
            help="Dossier contenant les .parquet (ex: data/raw)",
        )
        args = parser.parse_args()

        args.db.parent.mkdir(parents=True, exist_ok=True)

        importer = DuckDBImporter(db_path=args.db)
        try:
            importer.import_all_parquet_files(args.data_dir)
            importer.get_statistics()
        finally:
            importer.close()


if __name__ == "__main__":
    main()
