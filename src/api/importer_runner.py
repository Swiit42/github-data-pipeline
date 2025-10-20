from pathlib import Path
from .settings import settings
from src.db_connector import PostgresParquetImporter

def run_import(data_dir: Path | None = None, dry_run: bool = False, method: str = "copy") -> dict:
    data_dir = data_dir or Path(settings.DATA_DIR)
    importer = PostgresParquetImporter(
        host=settings.PG_HOST,
        dbname=settings.PG_DB,
        user=settings.PG_USER,
        password=settings.PG_PASSWORD,
        port=settings.PG_PORT,
        batch_size=200_000,
        method=method,
        dry_run=dry_run,
        verbose=True
    )
    try:
        imported = importer.import_all_parquet_files(data_dir, recursive=True)
        stats = importer.get_statistics()
        return {"imported_files": imported, "stats": stats, "data_dir": str(data_dir)}
    finally:
        importer.close()
