from pathlib import Path
import pandas as pd

SILVER_DIR = Path("data-silver")
GOLD_DIR = Path("data-gold")
GOLD_DIR.mkdir(parents=True, exist_ok=True)

in_path = SILVER_DIR / "sbs_na_ind_r2_silver.parquet"

gold1 = GOLD_DIR / "gold_country_indicator_year.parquet"
gold2 = GOLD_DIR / "gold_yoy_growth.parquet"

df = pd.read_parquet(in_path)

# 1) Tabela analítica base: (geo, indic_sbs, year) com value
base = df[["geo", "indic_sbs", "year", "value_num"]].copy()
base = base.rename(columns={"value_num": "value"})
base.to_parquet(gold1, index=False)

# 2) Crescimento YoY por país e indicador
base = base.sort_values(["geo", "indic_sbs", "year"])
base["value_prev"] = base.groupby(["geo", "indic_sbs"])["value"].shift(1)
base["yoy_pct"] = (base["value"] - base["value_prev"]) / base["value_prev"] * 100

yoy = base.dropna(subset=["yoy_pct"])
yoy.to_parquet(gold2, index=False)

print("GOLD saved:", gold1)
print("GOLD saved:", gold2)
