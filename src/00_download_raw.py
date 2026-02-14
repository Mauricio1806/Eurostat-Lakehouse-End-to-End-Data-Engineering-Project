# src/00_download_raw.py
from urllib.parse import quote

from config import DATA_RAW, DATASETS, EUROSTAT_BASE
from utils import ensure_dir, download_file, gunzip_file

def build_url(dataset: str) -> str:
    # Eurostat Dissemination API (SDMX 2.1) - TSV compactado
    return f"{EUROSTAT_BASE}/{quote(dataset)}?format=TSV&compressed=true"

def main() -> None:
    ensure_dir(DATA_RAW)

    for ds in DATASETS:
        url = build_url(ds)

        gz_path = DATA_RAW / f"{ds}.tsv.gz"
        tsv_path = DATA_RAW / f"{ds}.tsv"

        print(f"Downloading {ds} ...")
        download_file(url, gz_path)

        print(f"Decompressing {gz_path.name} ...")
        gunzip_file(gz_path, tsv_path)

        print(f"Saved: {tsv_path}")

if __name__ == "__main__":
    main()

