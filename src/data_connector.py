import os
from urllib.parse import quote_plus
from sqlmodel import create_engine
from sqlalchemy import text
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
from typing import Generator
from typing import Dict

import pandas as pd

# Charger le .env une seule fois
load_dotenv()


class DataConnector:

    def __init__(self):
        # Connexion PostgreSQL
        try:
            self.pg_engine = self._get_postgres_engine()
            with self.pg_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Connexion PostgreSQL OK")
        except Exception as e:
            self.pg_engine = None
            print("Erreur PostgreSQL :", repr(e))

        # Connexion MongoDB
        try:
            self.mongo_client = MongoClient(os.getenv("MONGO_URL"))
            mongo_db_name = os.getenv("MONGO_DB")
            self.mongo_db = self.mongo_client[mongo_db_name]
            self.cleaned_trips = self.mongo_db["yellow_taxi"]
            print("Connexion MongoDB OK", mongo_db_name)
        except ConnectionFailure as e:
            self.mongo_client = None
            self.mongo_db = None
            self.cleaned_trips = None
            print("Erreur MongoDB :", repr(e))

    def _get_postgres_engine(self):
        """
        Construit l'URL PostgreSQL depuis les variables d'environnement
        et retourne le moteur SQLAlchemy.
        """
        host = os.getenv("PG_HOST")
        port = os.getenv("PG_PORT")
        db = os.getenv("PG_DB")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")

        if not all([host, port, db, user, password]):
            raise EnvironmentError("Variables d'environnement PostgreSQL manquantes")

        pg_url = f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{db}"
        return create_engine(pg_url, pool_pre_ping=True, echo=False)


    def load_data_from_postgres(
        self, table_name: str = "yellow_taxi_trips", chunksize: int = 100_000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Charge les données depuis PostgreSQL par lots successifs avec LIMIT / OFFSET.
        Avantage : plus robuste pour les gros volumes, pas de curseur long côté serveur.
        """
        if self.pg_engine is None:
            raise ConnectionError("Connexion PostgreSQL non initialisée.")

        try:
            #Compter le total de lignes valides
            count_query = text(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE passenger_count BETWEEN 1 AND 8
                AND trip_distance < 100
                AND fare_amount < 500
                AND tpep_pickup_datetime IS NOT NULL
                AND tpep_dropoff_datetime IS NOT NULL
                AND tip_amount >= 0
                AND tolls_amount >= 0
                AND total_amount >= 0
                AND congestion_surcharge >= 0;
            """)
            with self.pg_engine.connect() as conn:
                total_rows = conn.execute(count_query).scalar()
            print(f"Total de lignes valides à charger : {total_rows:,}")

            offset = 0
            chunk_index = 1
            while offset < total_rows:
                query = text(f"""
                    SELECT * FROM {table_name}
                    WHERE passenger_count BETWEEN 1 AND 8
                    AND trip_distance < 100
                    AND fare_amount < 500
                    AND tpep_pickup_datetime IS NOT NULL
                    AND tpep_dropoff_datetime IS NOT NULL
                    AND tip_amount >= 0
                    AND tolls_amount >= 0
                    AND total_amount >= 0
                    AND congestion_surcharge >= 0
                    ORDER BY tpep_pickup_datetime
                    LIMIT {chunksize} OFFSET {offset};
                """)

                df_chunk = pd.read_sql(query, con=self.pg_engine)
                if df_chunk.empty:
                    break

                print(f"Chunk {chunk_index} : {len(df_chunk):,} lignes (OFFSET={offset:,})")
                yield df_chunk

                offset += chunksize
                chunk_index += 1

            print(f"Chargement terminé ({offset:,} lignes lues).")

        except Exception as e:
            print("Erreur lors du chargement PostgreSQL :", repr(e))
            return pd.DataFrame()



    def save_to_mongodb(self, df: pd.DataFrame) -> int:
            """
            Sauvegarde un DataFrame dans MongoDB.
            - Convertit en dict
            - Convertit les Timestamp Pandas en datetime Python
            - Supprime la colonne 'id' PostgreSQL si présente
            - Supprime les anciennes données correspondantes (idempotence)
            - Insère les nouvelles données
            - Retourne le nombre de documents insérés
            """
            if df.empty:
                print("DataFrame vide, rien à insérer dans MongoDB.")
                return 0

            # 1Convertir les Timestamp Pandas → datetime Python
            df = df.copy()
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

            # Supprimer l’ID PostgreSQL si présent
            for id_col in ["id", "ID", "uuid"]:
                if id_col in df.columns:
                    df.drop(columns=[id_col], inplace=True)
                    print(f"Colonne '{id_col}' supprimée avant insertion.")

            # Convertir en liste de dictionnaires
            records = df.to_dict("records")

            # Vérifier si des données existent déjà dans MongoDB
            existing_count = self.cleaned_trips.count_documents({})
            if existing_count > 0:
                print(f"{existing_count:,} anciens documents trouvés — suppression en cours...")
                self.cleaned_trips.delete_many({})
                print("Anciennes données supprimées.")

            # Insertion des nouvelles données
            try:
                result = self.cleaned_trips.insert_many(records, ordered=False)
                inserted_count = len(result.inserted_ids)
                print(f"{inserted_count:,} documents insérés dans MongoDB.")
                return inserted_count
            except Exception as e:
                print(f"Erreur lors de l’insertion MongoDB : {e}")
                return 0

if __name__ == "__main__":

    connector = DataConnector()

    if connector.pg_engine:
        with connector.pg_engine.connect() as c:
            print(c.execute(text("SELECT NOW()")).fetchone())

    for i, batch in enumerate(
            connector.load_data_from_postgres(table_name="yellow_taxi_trips", chunksize=100_000)
        ):
            print(f"Traitement du chunk {i+1}")
            inserted = connector.save_to_mongodb(batch)
            print(f"{inserted:,} documents insérés pour le chunk {i+1}\n")
