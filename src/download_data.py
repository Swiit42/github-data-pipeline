import argparse
from pathlib import Path
from datetime import datetime
import requests
from requests.exceptions import RequestException
import sys


class NYCTaxiDataDownloader:
    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    def __init__(self, year: int, data_dir: Path, check_exists: bool = False):
        self.YEAR = year
        self.DATA_DIR = Path(data_dir)
        self.CHECK_EXISTS = check_exists
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def get_file_path(self, month: int) -> Path:
        return self.DATA_DIR / f"yellow_tripdata_{self.YEAR}-{month:02d}.parquet"

    def file_exists(self, month: int) -> bool:
        return self.get_file_path(month).is_file()

    def url_exists(self, month: int) -> bool:
        """Vérifie que le fichier distant existe (HEAD request)"""
        url = f"{self.BASE_URL}/yellow_tripdata_{self.YEAR}-{month:02d}.parquet"
        try:
            r = requests.head(url, timeout=10)
            return r.status_code == 200
        except RequestException:
            return False

    def download_month(self, month: int) -> bool:
        local_path = self.get_file_path(month)
        url = f"{self.BASE_URL}/yellow_tripdata_{self.YEAR}-{month:02d}.parquet"

        if self.file_exists(month):
            print(f"[SKIP] {local_path.name} déjà présent.")
            return True

        # Vérifie l'existence du fichier distant avant téléchargement
        if self.CHECK_EXISTS and not self.url_exists(month):
            print(f"[MISS] {url} n’existe pas (HEAD != 200)")
            return False

        tmp_path = local_path.with_suffix(".parquet.part")

        try:
            print(f"[GET ] Téléchargement : {url}")
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                chunk_size = 8192

                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            pct = downloaded * 100 // total
                            sys.stdout.write(f"\r    Progression: {pct:3d}%")
                            sys.stdout.flush()

            tmp_path.rename(local_path)
            print(f"\n[OK  ] {local_path.name}")
            return True

        except RequestException as e:
            print(f"\n[ERR ] {url}: {e}")
            if tmp_path.exists():
                tmp_path.unlink()
            return False

    def download_all_available(self):
        now = datetime.now()
        last_month = now.month if self.YEAR == now.year else 12

        downloaded, failures = 0, 0
        print(f"[INFO] Téléchargement des données TLC {self.YEAR} jusqu’au mois {last_month}")

        for m in range(1, last_month + 1):
            if self.download_month(m):
                downloaded += 1
            else:
                failures += 1

        print("\n===== RÉSUMÉ =====")
        print(f"Mois traités  : {last_month}")
        print(f"Téléchargés   : {downloaded}")
        print(f"Échecs        : {failures}")
        print("==================\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Télécharge les fichiers TLC Yellow Taxi.")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Année à télécharger (ex: 2025)")
    parser.add_argument("--data-dir", type=Path, default=Path("data") / "raw", help="Dossier de stockage")
    parser.add_argument("--check-exists", action="store_true", help="Vérifie les URLs avant téléchargement")
    args = parser.parse_args()

    downloader = NYCTaxiDataDownloader(year=args.year, data_dir=args.data_dir, check_exists=args.check_exists)
    downloader.download_all_available()
