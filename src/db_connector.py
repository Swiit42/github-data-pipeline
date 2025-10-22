import io
import gc
from pathlib import Path
from os import getenv

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pyarrow.parquet as pq


class PostgresParquetImporter:
    def __init__(
        self,
        host: str,
        dbname: str,
        user: str,
        password: str,
        port: int = 5432,
        batch_size: int = 200_000,
        method: str = "copy",
        dry_run: bool = False,
        verbose: bool = True,
    ):
        """
        method: "copy" (recommandé) ou "values" pour isoler un blocage éventuel de COPY.
        dry_run: si True, lit seulement 1 batch et n'insert rien (debug rapide).
        """
        print(f"🔌 Connecting to PostgreSQL database '{dbname}' on {host}:{port}")
        self.conn = psycopg2.connect(
            host=host, dbname=dbname, user=user, password=password, port=port
        )
        self.cur = self.conn.cursor()
        self.conn.autocommit = False
        self.conn.set_client_encoding("UTF8")

        assert method in {"copy", "values"}
        self.method = method
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.verbose = verbose

        # Colonnes présentes dans les fichiers Parquet (source)
        self.src_cols = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
        ]

        # Colonnes de la table Postgres créée par SQLModel (CamelCase → doivent être citées)
        self.db_cols = [
            '"VendorID"', "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
            "trip_distance", '"RatecodeID"', "store_and_fwd_flag", '"PULocationID"', '"DOLocationID"',
            "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "total_amount", "congestion_surcharge", '"Airport_fee"',
        ]

        self._initialize_db()

    def log(self, msg: str):
        if self.verbose:
            print(msg, flush=True)

    def close(self):
        try:
            if self.cur:
                self.cur.close()
        finally:
            if self.conn:
                self.conn.close()
                print("🔒 PostgreSQL connection closed.")

    def _initialize_db(self):
        """Crée les tables si elles n'existent pas (schéma aligné CamelCase)."""
        self.log("🧱 Creating tables if not exists…")
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS yellow_taxi_trips (
            id SERIAL PRIMARY KEY,
            "VendorID" BIGINT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE PRECISION,
            trip_distance DOUBLE PRECISION,
            "RatecodeID" DOUBLE PRECISION,
            store_and_fwd_flag TEXT,
            "PULocationID" BIGINT,
            "DOLocationID" BIGINT,
            payment_type BIGINT,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION,
            "Airport_fee" DOUBLE PRECISION
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS import_log (
            file_name TEXT PRIMARY KEY,
            import_date TIMESTAMP DEFAULT NOW(),
            rows_imported BIGINT
        );
        """)
        self.conn.commit()
        self.log("✅ Schema ready.")

    def is_file_imported(self, file_name: str) -> bool:
        self.cur.execute("SELECT 1 FROM import_log WHERE file_name = %s;", (file_name,))
        return self.cur.fetchone() is not None

    def _try_lock_file(self, file_name: str) -> bool:
        self.cur.execute("SELECT pg_try_advisory_lock(hashtext(%s));", (file_name,))
        ok = self.cur.fetchone()[0]
        self.conn.commit()
        return ok

    def _unlock_file(self, file_name: str):
        self.cur.execute("SELECT pg_advisory_unlock(hashtext(%s));", (file_name,))
        self.conn.commit()

    def _normalize_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie et normalise un batch de données pour correspondre 1:1 à self.src_cols."""
        for c in self.src_cols:
            if c not in df.columns:
                df[c] = None

        for c in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
            df[c] = pd.to_datetime(df[c], errors="coerce")

        num_cols = {
            "VendorID", "passenger_count", "trip_distance", "RatecodeID",
            "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra",
            "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge", "Airport_fee",
        }
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")

        # Respect de l'ordre des colonnes source
        df = df[self.src_cols]
        df = df.where(pd.notnull(df), None)
        return df

    def _copy_chunk(self, df: pd.DataFrame):
        """Insère un batch via COPY (plus rapide)."""
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        # IMPORTANT : viser les colonnes DB (citées) et non les colonnes source
        self.cur.copy_expert(
            sql=f"COPY yellow_taxi_trips ({', '.join(self.db_cols)}) FROM STDIN WITH (FORMAT CSV)",
            file=buf
        )

    def _values_chunk(self, df: pd.DataFrame, page_size: int = 10_000):
        """Fallback INSERT VALUES (plus lent, utile en debug)."""
        data = list(df.itertuples(index=False, name=None))
        execute_values(
            self.cur,
            f"INSERT INTO yellow_taxi_trips ({', '.join(self.db_cols)}) VALUES %s",
            data,
            page_size=page_size,
        )

    def import_parquet(self, file_path: Path) -> bool:
        file_name = file_path.name
        self.log(f"\n📥 Start file: {file_name}")

        if self.is_file_imported(file_name):
            self.log(f"⚠️  Already imported: {file_name} — skipping.")
            return False

        if not self._try_lock_file(file_name):
            self.log(f"🔒 Locked by another process: {file_name} — skipping.")
            return False

        total_rows = 0
        try:
            pa_file = str(file_path)
            self.log(f"🧩 Opening ParquetFile: {pa_file}")
            pf = pq.ParquetFile(pa_file)

            self.log(f"📦 Row groups: {pf.num_row_groups}")
            batch_no = 0

            for batch in pf.iter_batches(batch_size=self.batch_size):
                batch_no += 1
                self.log(f"  🔄 Batch #{batch_no} — converting to pandas …")
                df = batch.to_pandas()
                df = self._normalize_chunk(df)
                if len(df) == 0:
                    continue

                if self.dry_run:
                    self.log(f"  🧪 DRY-RUN: would insert {len(df)} rows.")
                else:
                    if self.method == "copy":
                        self._copy_chunk(df)
                    else:
                        self._values_chunk(df)
                    self.conn.commit()
                    self.log(f"  ✅ Batch #{batch_no} committed.")

                total_rows += len(df)
                del df
                gc.collect()

                if self.dry_run:
                    self.log("  🧪 DRY-RUN: stopping after first batch.")
                    break

            if not self.dry_run:
                self.cur.execute(
                    "INSERT INTO import_log (file_name, rows_imported) VALUES (%s, %s);",
                    (file_name, total_rows),
                )
                self.conn.commit()
                self.log(f"✅ File done: {file_name} — {total_rows} rows.")

            return True

        except Exception as e:
            self.conn.rollback()
            self.log(f"❌ Failed to import {file_name}: {e}")
            return False

        finally:
            try:
                self._unlock_file(file_name)
            except Exception:
                pass

    def import_all_parquet_files(self, data_dir: Path, recursive: bool = True) -> int:
        data_dir = Path(data_dir)
        self.log(f"📁 Effective data_dir = {data_dir.resolve()}")
        pattern = "**/*.parquet" if recursive else "*.parquet"
        parquet_files = sorted(data_dir.glob(pattern))
        self.log(f"🗂️ Found {len(parquet_files)} parquet files.")
        if not parquet_files:
            print(f"ℹ️ No parquet files found in {data_dir} (recursive={recursive}).")

        imported_count = 0
        for i, file_path in enumerate(parquet_files, 1):
            self.log(f"➡️ [{i}/{len(parquet_files)}] {file_path}")
            if self.import_parquet(file_path):
                imported_count += 1
        print(f"📦 Imported {imported_count}/{len(parquet_files)} parquet files.")
        return imported_count

    def get_statistics(self):
        self.cur.execute("""
            SELECT 
                COUNT(*) AS total_trips,
                AVG(trip_distance) AS avg_trip_distance,
                SUM(total_amount) AS total_revenue
            FROM yellow_taxi_trips;
        """)
        row = self.cur.fetchone()
        stats = {
            "total_trips": row[0],
            "avg_trip_distance": float(row[1]) if row[1] is not None else None,
            "total_revenue": float(row[2]) if row[2] is not None else None,
        }
        print("📊 Database Statistics:", stats)
        return stats


if __name__ == "__main__":
    # Lecture silencieuse des variables d'env (Docker Compose)
    HOST = getenv("PG_HOST")
    DB = getenv("PG_DB")
    USER = getenv("PG_USER")
    PASSWORD = getenv("PG_PASSWORD")
    PORT = int(getenv("PG_PORT"))
    DATA_DIR = Path(getenv("DATA_DIR"))

    importer = PostgresParquetImporter(
        host=HOST, dbname=DB, user=USER, password=PASSWORD, port=PORT,
        batch_size=200_000, method="copy", dry_run=False, verbose=True
    )
    try:
        print("CWD =", Path.cwd())
        print("Parquet files (sample):", list((DATA_DIR / "raw").glob("*.parquet"))[:5])
        imported = importer.import_all_parquet_files(DATA_DIR, recursive=True)
        print(f"Imported files count: {imported}")
    finally:
        importer.close()
