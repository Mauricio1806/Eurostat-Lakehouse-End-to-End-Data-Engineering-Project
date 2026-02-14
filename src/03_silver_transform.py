from pathlib import Path
import pandas as pd

BRONZE_DIR = Path("data-bronze")
SILVER_DIR = Path("data-silver")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

in_path = BRONZE_DIR / "sbs_na_ind_r2_bronze.parquet"
out_path = SILVER_DIR / "sbs_na_ind_r2_silver.parquet"

df = pd.read_parquet(in_path)

# split da chave: "freq,nace_r2,indic_sbs,geo\TIME_PERIOD"
# Exemplo de key: "A,NACE2,....,DE"
# O último item normalmente é geo; o resto depende do dataset.
parts = df["key"].astype(str).str.split(",", expand=True)

# garante colunas mínimas (se vier mais/menos, a gente adapta depois)
# Será assumido 4 partes (freq, nace_r2, indic_sbs, geo)
df["freq"] = parts[0]
df["nace_r2"] = parts[1]
df["indic_sbs"] = parts[2]
df["geo"] = parts[3]

df = df.drop(columns=["key"])

# colunas de anos viram linhas
value_cols = [c for c in df.columns if c not in ["freq", "nace_r2", "indic_sbs", "geo"]]

long_df = df.melt(
    id_vars=["freq", "nace_r2", "indic_sbs", "geo"],
    value_vars=value_cols,
    var_name="year",
    value_name="value_raw"
)

# limpar valores: pode ter ":" (missing) ou flags tipo "123.4 p"
# Mantém só número (quando existir)
long_df["value_raw"] = long_df["value_raw"].astype(str).str.strip()
long_df["value_num"] = (
    long_df["value_raw"]
    .str.replace(",", ".", regex=False)
    .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
)
long_df["value_num"] = pd.to_numeric(long_df["value_num"], errors="coerce")

# year numérico
long_df["year"] = pd.to_numeric(long_df["year"], errors="coerce").astype("Int64")

# remove linhas sem ano ou sem valor
long_df = long_df.dropna(subset=["year", "value_num"])

# regra simples de qualidade: value >= 0 (ajusta depois se precisar)
long_df = long_df[long_df["value_num"] >= 0]

long_df.to_parquet(out_path, index=False)
print("SILVER saved:", out_path, "rows:", len(long_df), "cols:", len(long_df.columns))
