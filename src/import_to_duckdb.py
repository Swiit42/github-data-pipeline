# src/import_to_duckdb.py
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional

import duckdb


class DuckDBImporter:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Crée la table de log si elle n'existe pas."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                file_name TEXT PRIMARY KEY,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_imported BIGINT
            );
        """)
        print("[DEBUG] Tables après init:", self.conn.execute("SHOW TABLES;").fetchall())

    def table_exists(self, name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1;",
            [name],
        ).fetchone()
        return row is not None

    def is_file_imported(self, filename: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM import_log WHERE file_name = ? LIMIT 1;",
            [filename],
        ).fetchone()
        return row is not None

    # ---------- Création de la table depuis le 1er parquet ----------
    def create_table_from_first_parquet(self, parquet_path: Path) -> None:
        """Crée la table principale à partir du schéma du premier parquet."""
        print(f"[INFO] Création de la table 'yellow_taxi_trips' depuis {parquet_path.name}")
        query = f"""
        CREATE TABLE yellow_taxi_trips AS
        SELECT * FROM read_parquet('{parquet_path}') LIMIT 0;
        """
        self.conn.execute(query)
        print("[DEBUG] Table 'yellow_taxi_trips' créée via read_parquet().")
        print("[DEBUG] Colonnes :", self.conn.execute("PRAGMA table_info('yellow_taxi_trips');").fetchall())

    # ---------- Import d'un fichier ----------
    def import_parquet(self, parquet_path: Path) -> int:
        parquet_path = Path(parquet_path)
        fname = parquet_path.name

        if not parquet_path.is_file():
            print(f"[ERROR] Fichier non trouvé: {parquet_path}")
            return 0

        if self.is_file_imported(fname):
            print(f"[SKIP] Déjà importé (log): {fname}")
            return 0

        # Crée la table à partir du premier parquet si absente
        if not self.table_exists("yellow_taxi_trips"):
            self.create_table_from_first_parquet(parquet_path)

        # Nombre avant import
        before = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[0]

        # Insertion BY NAME pour tolérer les différences de colonnes
        self.conn.execute(
            "INSERT INTO yellow_taxi_trips BY NAME SELECT * FROM read_parquet(?);",
            [str(parquet_path)],
        )

        # Nombre après import
        after = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[0]
        delta = int(after - before)

        # Enregistre dans le log
        self.conn.execute(
            "INSERT INTO import_log (file_name, rows_imported) VALUES (?, ?);",
            (fname, delta),
        )

        print(f"[OK] {fname} -> +{delta:,} lignes")
        return delta

    # ---------- Import batch ----------
    def import_all_parquet_files(self, data_dir: Path) -> int:
        data_dir = Path(data_dir)
        files = sorted(data_dir.glob("*.parquet"))
        if not files:
            print(f"[INFO] Aucun .parquet trouvé dans: {data_dir.resolve()}")
            return 0

        print(f"[INFO] {len(files)} fichiers trouvés.")
        imported = 0
        for fp in files:
            if self.import_parquet(fp) > 0:
                imported += 1

        print(f"[INFO] Fichiers traités: {len(files)} | Nouveaux imports: {imported} | Déjà logués: {len(files)-imported}")
        return imported

    # ---------- Statistiques ----------
    def get_statistics(self) -> None:
        if not self.table_exists("yellow_taxi_trips"):
            nb_files = self.conn.execute("SELECT COUNT(*) FROM import_log;").fetchone()[0]
            print("\n===== STATISTIQUES DUCKDB =====")
            print(f"Base : {self.db_path.resolve()}")
            print("Table 'yellow_taxi_trips' absente (aucun import effectué).")
            print(f"Fichiers logués : {nb_files}")
            print("================================\n")
            return

        total = self.conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips;").fetchone()[0]
        nb_files = self.conn.execute("SELECT COUNT(*) FROM import_log;").fetchone()[0]
        min_dt, max_dt = self.conn.execute("""
            SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime)
            FROM yellow_taxi_trips;
        """).fetchone()

        try:
            size = os.path.getsize(self.db_path)
        except OSError:
            size = None

        def fmt_size(n):
            if n is None: return "n/a"
            for unit in ("B","KB","MB","GB","TB"):
                if n < 1024: return f"{n:.1f} {unit}"
                n /= 1024
            return f"{n:.1f} PB"

        print("\n===== STATISTIQUES DUCKDB =====")
        print(f"Base : {self.db_path.resolve()}")
        print(f"Total trajets : {total:,}".replace(",", " "))
        print(f"Fichiers importés : {nb_files}")
        print(f"Plage de dates : {min_dt} → {max_dt}")
        print(f"Taille : {fmt_size(size)}")
        print("================================\n")

    def close(self) -> None:
        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Importe des fichiers Parquet Yellow Taxi dans DuckDB.")
    parser.add_argument("--db", type=Path, default=Path("data/warehouse/yellow_taxi.duckdb"))
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
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
