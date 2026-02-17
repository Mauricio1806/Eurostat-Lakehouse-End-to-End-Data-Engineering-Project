from __future__ import annotations

from pathlib import Path
import math

import numpy as np
import pandas as pd


# ----------------------------
# Paths (repo root)
# ----------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = REPO_ROOT / "data-silver" / "sbs_na_ind_r2_silver.parquet"
YOY_PATH = REPO_ROOT / "data-gold" / "gold_yoy_growth.parquet"

OUT_PARQUET = REPO_ROOT / "data-gold" / "gold_structural_metrics.parquet"
OUT_CSV = REPO_ROOT / "data-gold" / "gold_structural_metrics.csv"


# ----------------------------
# Helpers
# ----------------------------
def _safe_num(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    x = x.where(np.isfinite(x), np.nan)
    return x


def _cagr(first: float, last: float, years: int) -> float:
    """
    CAGR in percent. years means number of periods between first and last year.
    """
    if first is None or last is None:
        return np.nan
    if not np.isfinite(first) or not np.isfinite(last):
        return np.nan
    if first <= 0 or years <= 0:
        return np.nan
    return (math.pow(last / first, 1.0 / years) - 1.0) * 100.0


def main() -> None:
    # --- Load Silver
    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Silver file not found: {SILVER_PATH}")

    df = pd.read_parquet(SILVER_PATH)

    # Expect columns like:
    # freq, nace_r2, indic_sbs, geo, year, value_raw, value_num
    if "value_num" in df.columns:
        df["value"] = _safe_num(df["value_num"])
    elif "value" in df.columns:
        df["value"] = _safe_num(df["value"])
    else:
        raise ValueError("Silver parquet must have value_num or value column")

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["geo", "indic_sbs", "year", "value"]).copy()
    df["year"] = df["year"].astype(int)

    # Keep only sensible rows for growth metrics
    # (value can be 0, but CAGR requires >0; we'll handle later)
    df = df.sort_values(["indic_sbs", "geo", "year"])

    # --- Build per (geo, indic_sbs) structural metrics using first/last valid year
    grp = df.groupby(["geo", "indic_sbs"], as_index=False)

    # First/last year/value
    first_rows = grp.first()[["geo", "indic_sbs", "year", "value"]].rename(
        columns={"year": "year_first", "value": "value_first"}
    )
    last_rows = grp.last()[["geo", "indic_sbs", "year", "value"]].rename(
        columns={"year": "year_last", "value": "value_last"}
    )

    # Min/max years + n_years
    span = grp.agg(
        year_min=("year", "min"),
        year_max=("year", "max"),
        n_years=("year", "nunique"),
    )

    out = span.merge(first_rows, on=["geo", "indic_sbs"], how="left").merge(
        last_rows, on=["geo", "indic_sbs"], how="left"
    )

    # Changes
    out["abs_change"] = out["value_last"] - out["value_first"]
    out["pct_change"] = np.where(
        (out["value_first"] > 0) & np.isfinite(out["value_first"]) & np.isfinite(out["value_last"]),
        (out["value_last"] / out["value_first"] - 1.0) * 100.0,
        np.nan,
    )

    # CAGR (periods between first and last year)
    out["periods"] = (out["year_last"] - out["year_first"]).astype("int64")

    out["cagr"] = [
        _cagr(f, l, int(p))
        for f, l, p in zip(out["value_first"], out["value_last"], out["periods"])
    ]

    # --- YoY stats (from gold_yoy_growth)
    if YOY_PATH.exists():
        yoy = pd.read_parquet(YOY_PATH).copy()
        # expected: geo, indic_sbs, year, yoy_pct
        if "yoy_pct" in yoy.columns:
            yoy["yoy_pct"] = _safe_num(yoy["yoy_pct"])
        else:
            yoy["yoy_pct"] = np.nan

        yoy = yoy.dropna(subset=["geo", "indic_sbs", "year", "yoy_pct"]).copy()
        yoy["year"] = pd.to_numeric(yoy["year"], errors="coerce")
        yoy = yoy.dropna(subset=["year"]).copy()
        yoy["year"] = yoy["year"].astype(int)

        yoy_stats = (
            yoy.groupby(["geo", "indic_sbs"], as_index=False)
            .agg(
                yoy_mean=("yoy_pct", "mean"),
                yoy_volatility=("yoy_pct", "std"),
                yoy_n=("yoy_pct", "count"),
            )
        )

        out = out.merge(yoy_stats, on=["geo", "indic_sbs"], how="left")
    else:
        out["yoy_mean"] = np.nan
        out["yoy_volatility"] = np.nan
        out["yoy_n"] = 0

    # --- Ranking: first-year and last-year ranks per indicator (global)
    # We rank on each indicator's earliest available year and latest available year (overall)
    latest_year_by_indic = df.groupby("indic_sbs")["year"].max().to_dict()
    earliest_year_by_indic = df.groupby("indic_sbs")["year"].min().to_dict()

    df_latest = df.copy()
    df_latest["year_target"] = df_latest["indic_sbs"].map(latest_year_by_indic)
    df_latest = df_latest[df_latest["year"] == df_latest["year_target"]].copy()

    df_earliest = df.copy()
    df_earliest["year_target"] = df_earliest["indic_sbs"].map(earliest_year_by_indic)
    df_earliest = df_earliest[df_earliest["year"] == df_earliest["year_target"]].copy()

    # Rank descending by value (1 is top)
    df_latest["rank_last_year"] = df_latest.groupby("indic_sbs")["value"].rank(
        method="dense", ascending=False
    )
    df_earliest["rank_first_year"] = df_earliest.groupby("indic_sbs")["value"].rank(
        method="dense", ascending=False
    )

    rank_last = df_latest[["geo", "indic_sbs", "rank_last_year"]].drop_duplicates()
    rank_first = df_earliest[["geo", "indic_sbs", "rank_first_year"]].drop_duplicates()

    out = out.merge(rank_first, on=["geo", "indic_sbs"], how="left").merge(
        rank_last, on=["geo", "indic_sbs"], how="left"
    )
    out["rank_delta"] = out["rank_first_year"] - out["rank_last_year"]

    # --- Final columns and save
    cols = [
        "geo",
        "indic_sbs",
        "year_min",
        "year_max",
        "n_years",
        "year_first",
        "year_last",
        "value_first",
        "value_last",
        "abs_change",
        "pct_change",
        "cagr",
        "yoy_mean",
        "yoy_volatility",
        "yoy_n",
        "rank_first_year",
        "rank_last_year",
        "rank_delta",
    ]
    out = out[cols].sort_values(["indic_sbs", "cagr"], ascending=[True, False])

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_PARQUET, index=False)
    out.to_csv(OUT_CSV, index=False)

    print("Saved:")
    print(f"- {OUT_PARQUET}")
    print(f"- {OUT_CSV}")
    print(f"Rows: {len(out):,}".replace(",", "."))


if __name__ == "__main__":
    main()
