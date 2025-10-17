# src/import_to_duckdb.py
from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb



class DuckDBImporter:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn = duckdb.connect(str(self.db_path))
        self.conn =self._initialize_database()

    def _initialize_database(self) -> None:
        # Schéma de base + table de log
        conn = duckdb.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                file_name TEXT PRIMARY KEY,
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rows_imported INTEGER )
                     """)
    
        return conn

   # ---------- Anti-doublon ----------
    def is_file_imported(self, filename: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM import_log WHERE file_name = ? LIMIT 1;", [filename]
        ).fetchone()
        return row is not None

    # ---------- Import d'un fichier ----------
    def create_table_from_parquet(self, parquet_path: Path) -> None:
        query = f"""
        CREATE TABLE IF NOT EXISTS yellow_taxi_trips AS
        SELECT * FROM read_parquet('{parquet_path}')
        LIMIT 0;
        """

        self.conn.execute(query)
        print(f"[INFO] Table 'yellow_taxi_trips' créée (si inexistante).")

    
    def import_parquet(self, parquet_path: Path) -> int:
        parquet_path = Path(parquet_path)
        if not parquet_path.is_file():
            print(f"[ERROR] Fichier non trouvé: {parquet_path}")
            return 0

        if self.is_file_imported(parquet_path.name):
            print(f"[SKIP] Fichier déjà importé (log): {parquet_path.name}")
            return 0

        # Crée la table si elle n'existe pas encore
        self.create_table_from_parquet(parquet_path)

        # Importation des données
        import_query = f"""
        INSERT INTO yellow_taxi_trips
        SELECT * FROM read_parquet('{parquet_path}');
        """
        self.conn.execute(import_query)

        # Nombre de lignes importées
        rows_imported = self.conn.execute(
            "SELECT COUNT(*) FROM yellow_taxi_trips WHERE TRUE;"
        ).fetchone()[0]

        # Log de l'importation
        self.conn.execute(
            "INSERT INTO import_log (file_name, rows_imported) VALUES (?, ?);",
            (parquet_path.name, rows_imported),
        )

        print(f"[IMPORT] {parquet_path.name} importé avec {rows_imported} lignes.")
        return rows_imported

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

        min_dt, max_dt = self.conn.execute(
            "SELECT MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime) FROM yellow_taxi_trips;"
        ).fetchone()

        try:
            db_size_bytes: Optional[int] = os.path.getsize(self.db_path)
        except OSError:
            db_size_bytes = None

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
