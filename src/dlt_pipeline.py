# src/dlt_pipeline.py
import os
from pathlib import Path
import requests
import dlt
import pandas as pd


class NYCTaxiDLTPipeline:
    def __init__(self, year: int = 2025, months=None, data_dir: str = "src/data"):
        self.year = year
        self.months = months or list(range(1, 13))
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.dataset_name = "nyc_taxi_dlt"

    def _download_if_needed(self, month: int) -> Path:
        filename = f"yellow_tripdata_{self.year}-{month:02d}.parquet"
        file_path = self.data_dir / filename
        if not file_path.exists():
            url = f"{self.base_url}/{filename}"
            print(f"Downloading {url}")
            r = requests.get(url, stream=True)
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)
            print(f"Saved {file_path}")
        return file_path

    # Générateur simple (pas décoré) pour lire les données
    def _iter_taxi_data(self):
        for month in self.months:
            file_path = self._download_if_needed(month)
            df = pd.read_parquet(file_path)  # DLT normalisera le schéma côté destination
            # On peut yield ligne par ligne ou par batch (ici, enregistrements)
            for record in df.to_dict(orient="records"):
                yield record

    # Ressource DLT fabriquée dynamiquement (referme sur self)
    def _dlt_resource(self):
        @dlt.resource(
            name="yellow_taxi_trips",
            table_name="yellow_taxi_trips",
            write_disposition="merge",
            primary_key=["vendorid", "tpep_pickup_datetime", "pulocationid", "dolocationid"],
        )
        def yellow_taxi_trips():
            # Ici on appelle notre générateur qui utilise self
            for rec in self._iter_taxi_data():
                yield rec

        return yellow_taxi_trips

    def run(self):
        # Auto-détection local vs docker (tu peux aussi surcharger via PG_* env)
        in_docker = os.path.exists("/.dockerenv")
        pg_host = os.getenv("PG_HOST", "postgres" if in_docker else "localhost")
        pg_port = int(os.getenv("PG_PORT", "5432" if in_docker else "15432"))
        pg_user = os.getenv("PG_USER", "postgres")
        pg_password = os.getenv("PG_PASSWORD", "postgres")
        pg_db = os.getenv("PG_DB", "postgres")

        # Injecte la connexion Postgres explicitement pour DLT
        os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = (
            f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        )

        pipeline = dlt.pipeline(
            pipeline_name="nyc_taxi_pipeline",
            destination="postgres",
            dataset_name=self.dataset_name,
        )

        print(f"Running DLT pipeline for {self.year} months {self.months} → postgres.{self.dataset_name}")

        resource = self._dlt_resource()
        info = pipeline.run(resource())

        print("Load complete")
        print(info)


if __name__ == "__main__":
    NYCTaxiDLTPipeline().run()

