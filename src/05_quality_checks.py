import json
from pathlib import Path
import pandas as pd

BRONZE = Path("data-bronze/sbs_na_ind_r2_bronze.parquet")
SILVER = Path("data-silver/sbs_na_ind_r2_silver.parquet")
GOLD1 = Path("data-gold/gold_country_indicator_year.parquet")
GOLD2 = Path("data-gold/gold_yoy_growth.parquet")

OUT_DIR = Path("outputs-checks")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "quality_report.json"

def pct_null(s: pd.Series) -> float:
    return float(s.isna().mean())

report = {"files": {}, "checks": {}, "status": "OK", "errors": []}

# File existence + size
for p in [BRONZE, SILVER, GOLD1, GOLD2]:
    report["files"][str(p)] = {
        "exists": p.exists(),
        "size_bytes": p.stat().st_size if p.exists() else None
    }
    if not p.exists():
        report["status"] = "FAIL"
        report["errors"].append(f"Missing file: {p}")

if report["status"] == "FAIL":
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    raise SystemExit("Quality checks failed (missing files).")

# Load
bronze = pd.read_parquet(BRONZE)
silver = pd.read_parquet(SILVER)
gold = pd.read_parquet(GOLD1)
yoy = pd.read_parquet(GOLD2)

# Bronze checks
report["checks"]["bronze"] = {
    "rows": int(len(bronze)),
    "cols": int(len(bronze.columns)),
    "has_key": "key" in bronze.columns,
}

# Silver checks
expected_cols = {"freq", "nace_r2", "indic_sbs", "geo", "year", "value_raw", "value_num"}
missing = sorted(list(expected_cols - set(silver.columns)))
report["checks"]["silver"] = {
    "rows": int(len(silver)),
    "cols": int(len(silver.columns)),
    "missing_expected_cols": missing,
    "null_rate_year": pct_null(silver["year"]),
    "null_rate_value_num": pct_null(silver["value_num"]),
    "year_min": int(silver["year"].min()) if len(silver) else None,
    "year_max": int(silver["year"].max()) if len(silver) else None,
    "value_num_min": float(silver["value_num"].min()) if len(silver) else None,
    "value_num_max": float(silver["value_num"].max()) if len(silver) else None,
}

if missing:
    report["status"] = "FAIL"
    report["errors"].append(f"Silver missing columns: {missing}")

# Gold checks
report["checks"]["gold_country_indicator_year"] = {
    "rows": int(len(gold)),
    "cols": int(len(gold.columns)),
    "null_rate_value": pct_null(gold["value"]),
}

# YOY checks
report["checks"]["gold_yoy_growth"] = {
    "rows": int(len(yoy)),
    "cols": int(len(yoy.columns)),
    "yoy_min": float(yoy["yoy_pct"].min()) if len(yoy) else None,
    "yoy_max": float(yoy["yoy_pct"].max()) if len(yoy) else None,
}

# Final status
if report["errors"]:
    report["status"] = "FAIL"

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print("Quality report saved:", OUT)

if report["status"] != "OK":
    raise SystemExit("Quality checks failed. See outputs-checks/quality_report.json")
