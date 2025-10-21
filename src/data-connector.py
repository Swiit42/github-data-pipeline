import os
from urllib.parse import quote_plus
from sqlmodel import create_engine
from sqlalchemy import text
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv
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
            print("✅ Connexion PostgreSQL OK")
        except Exception as e:
            self.pg_engine = None
            print("❌ Erreur PostgreSQL :", repr(e))

        # Connexion MongoDB
        try:
            self.mongo_client = self._get_mongo_client()
            self.mongo_client.admin.command("ping")
            mongo_db_name = os.getenv("MONGO_DB")
            self.mongo_db = self.mongo_client[mongo_db_name]
            self.cleaned_trips = self.mongo_db["yellow_taxi"]
            print("✅ Connexion MongoDB OK")
        except ConnectionFailure as e:
            self.mongo_client = None
            self.mongo_db = None
            self.cleaned_trips = None
            print("❌ Erreur MongoDB :", repr(e))

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

    def _get_mongo_client(self):
        """
        Construit l'URL MongoDB depuis les variables d'environnement
        et retourne le client PyMongo.
        """
        mongo_url = os.getenv("MONGO_URL")
        if not mongo_url:
            host = os.getenv("MONGO_HOST")
            port = os.getenv("MONGO_PORT")
            user = os.getenv("MONGO_USER")
            password = os.getenv("MONGO_PASSWORD")
            auth_source = os.getenv("MONGO_AUTH_SOURCE")

            if not all([host, port, user, password, auth_source]):
                raise EnvironmentError("❌ Variables d'environnement MongoDB manquantes")

            mongo_url = f"mongodb://{user}:{quote_plus(password)}@{host}:{port}/?authSource={auth_source}"

        return MongoClient(mongo_url, serverSelectionTimeoutMS=3000)
    def load_data_from_postgres(self, table_name: str = "yellow_taxi_trips") -> pd.DataFrame:
        """
        Charge toutes les données depuis PostgreSQL dans un DataFrame.
        Utilise pd.read_sql() via l'engine SQLAlchemy.
        """
        if self.pg_engine is None:
            raise ConnectionError("❌ Connexion PostgreSQL non initialisée.")

        try:
            query = text(f'SELECT * FROM "{table_name}"')
            df = pd.read_sql(query, con=self.pg_engine)
            print(f"✅ Données chargées depuis PostgreSQL : {len(df)} lignes.")
            return df
        except Exception as e:
            print("❌ Erreur lors du chargement PostgreSQL :", repr(e))
            return pd.DataFrame()

if __name__ == "__main__":
    connector = DataConnector()
    if connector.pg_engine:
        with connector.pg_engine.connect() as c:
            print(c.execute(text("SELECT NOW()")).fetchone())

    if connector.cleaned_trips is not None:
        print("Docs cleaned_trips :", connector.cleaned_trips.count_documents({}))

    query = text(f'SELECT * FROM "yellow_taxi_trips" LIMIT 10')
    
    df = pd.read_sql(query, con=connector.pg_engine)

    print(f"Total lignes chargées : {len(df)}")

    if connector.cleaned_trips is not None and not df.empty:
        result = connector.cleaned_trips.insert_many(df.to_dict("records"))
        print("Données insérées dans MongoDB", len(result.inserted_ids), "documents.")
        print("Total documents :", connector.cleaned_trips.count_documents({}))
