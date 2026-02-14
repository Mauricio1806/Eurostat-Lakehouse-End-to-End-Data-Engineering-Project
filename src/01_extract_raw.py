from pathlib import Path
import gzip
import shutil

RAW_DIR = Path("data-raw")

# tenta os dois nomes
gz_file_1 = RAW_DIR / "sbs_na_ind_r2.tsv.gz"
gz_file_2 = RAW_DIR / "sbs_na_ind_r2.tsv.gz.gz"
gz_file = gz_file_1 if gz_file_1.exists() else gz_file_2

tsv_file = RAW_DIR / "sbs_na_ind_r2.tsv"

if not gz_file.exists():
    raise FileNotFoundError(f"Não achei {gz_file_1} nem {gz_file_2}. Veja o nome real em data-raw/")

with gzip.open(gz_file, "rb") as f_in:
    with open(tsv_file, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

print("Extraction finished:", tsv_file)
