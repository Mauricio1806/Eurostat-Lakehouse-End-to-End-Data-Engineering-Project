from pathlib import Path
import pandas as pd

RAW_DIR = Path("data-raw")
BRONZE_DIR = Path("data-bronze")
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

tsv_path = RAW_DIR / "sbs_na_ind_r2.tsv"
out_path = BRONZE_DIR / "sbs_na_ind_r2_bronze.parquet"

# Eurostat TSV geralmente vem com primeira coluna tipo:
# "freq,nace_r2,indic_sbs,geo\TIME_PERIOD"
# e depois colunas de anos (2010, 2011, ...)

df = pd.read_csv(tsv_path, sep="\t")

# padroniza nome da primeira coluna (fica mais fácil depois)
first_col = df.columns[0]
df = df.rename(columns={first_col: "key"})

df.to_parquet(out_path, index=False)
print("BRONZE saved:", out_path, "rows:", len(df), "cols:", len(df.columns))
