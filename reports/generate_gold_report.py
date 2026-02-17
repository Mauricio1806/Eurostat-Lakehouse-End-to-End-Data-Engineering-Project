from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import math
import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from jinja2 import Environment, FileSystemLoader, select_autoescape


# =========================================================
# CONFIG (AJUSTE AQUI SEM QUEBRAR O RESTO)
# =========================================================
FORCE_INDICATOR: str | None = None

# YoY explode quando value_prev ~ 0. Aumente se quiser mais conservador.
YOY_MIN_PREV_VALUE: float = 1.0

TOP_N = 10
RANK_BASE_YEAR: int | None = None

# Remove agregados tipo EU27_2020 / EA19_2020 etc.
COUNTRY_ONLY: bool = True

# Para evitar gráfico “chapado” ou dominado por outlier:
# - a tabela mostra o valor real
# - o gráfico usa valores “clipados” só para visual
YOY_CLIP_ABS_FOR_CHART: float = 200.0   # (%)
CAGR_CLIP_ABS_FOR_CHART: float = 50.0   # (%)

# Mínimo de anos para aceitar CAGR (melhora coerência)
CAGR_MIN_YEARS: int = 5


# =========================================================
# PATHS
# =========================================================
REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = REPO_ROOT / "data-gold"

OUT_DIR = Path(__file__).resolve().parent / "out"
ASSETS_DIR = OUT_DIR / "assets"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

CHECKS_DIR = REPO_ROOT / "outputs-checks"
QUALITY_REPORT_JSON = CHECKS_DIR / "quality_report.json"

GOLD_COUNTRY_INDICATOR_YEAR = GOLD_DIR / "gold_country_indicator_year.parquet"
GOLD_YOY_GROWTH = GOLD_DIR / "gold_yoy_growth.parquet"
GOLD_STRUCTURAL_METRICS = GOLD_DIR / "gold_structural_metrics.parquet"


# =========================================================
# HELPERS
# =========================================================
def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)


def safe_numeric(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.where(np.isfinite(s), pd.NA)


def fmt_year(x) -> str:
    try:
        if pd.isna(x):
            return "—"
        return str(int(x))
    except Exception:
        return "—"


def human_number(x, decimals: int = 2) -> str:
    if x is None:
        return "—"
    if isinstance(x, float) and (math.isnan(x) or not math.isfinite(x)):
        return "—"
    try:
        x = float(x)
    except Exception:
        return str(x)

    absx = abs(x)
    if absx >= 1e12:
        return f"{x/1e12:.{decimals}f}T"
    if absx >= 1e9:
        return f"{x/1e9:.{decimals}f}B"
    if absx >= 1e6:
        return f"{x/1e6:.{decimals}f}M"
    if absx >= 1e3:
        return f"{x/1e3:.{decimals}f}K"
    if absx >= 100:
        return f"{x:,.0f}".replace(",", ".")
    return f"{x:,.{decimals}f}".replace(",", ".")


def pct1(x) -> str:
    if x is None:
        return "—"
    if isinstance(x, float) and (math.isnan(x) or not math.isfinite(x)):
        return "—"
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "—"


def pct2(x) -> str:
    if x is None:
        return "—"
    if isinstance(x, float) and (math.isnan(x) or not math.isfinite(x)):
        return "—"
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "—"


def read_quality_report() -> dict | None:
    if not QUALITY_REPORT_JSON.exists():
        return None
    try:
        return json.loads(QUALITY_REPORT_JSON.read_text(encoding="utf-8"))
    except Exception:
        return None


def is_country_geo(geo: str) -> bool:
    """
    Heurística simples para remover agregados típicos:
    EU27_2020, EA19_2020, EA, EU, etc.
    Mantém códigos país (2 letras) e alguns casos comuns.
    """
    if geo is None:
        return False
    g = str(geo).strip().upper()

    if "_" in g:
        return False

    if g.startswith("EU") or g.startswith("EA"):
        return False

    if g in {"EU", "EA"}:
        return False

    if len(g) in (2, 3):
        return True

    return False


def pick_main_indicator(df_top: pd.DataFrame) -> str | None:
    if df_top.empty or "indic_sbs" not in df_top.columns:
        return None
    if FORCE_INDICATOR and (df_top["indic_sbs"] == FORCE_INDICATOR).any():
        return FORCE_INDICATOR
    return str(df_top["indic_sbs"].mode().iloc[0])


def compute_coverage(df_top: pd.DataFrame, df_yoy: pd.DataFrame, df_struct: pd.DataFrame) -> dict:
    cov: dict = {}

    cov["top_rows"] = int(len(df_top))
    cov["top_countries"] = int(df_top["geo"].nunique()) if "geo" in df_top.columns else 0
    cov["top_indicators"] = int(df_top["indic_sbs"].nunique()) if "indic_sbs" in df_top.columns else 0
    cov["top_year_min"] = int(df_top["year"].min()) if "year" in df_top.columns and len(df_top) else None
    cov["top_year_max"] = int(df_top["year"].max()) if "year" in df_top.columns and len(df_top) else None
    cov["top_missing_value_pct"] = float(df_top["value"].isna().mean() * 100.0) if "value" in df_top.columns and len(df_top) else 0.0

    cov["yoy_rows"] = int(len(df_yoy))
    cov["yoy_countries"] = int(df_yoy["geo"].nunique()) if "geo" in df_yoy.columns else 0
    cov["yoy_indicators"] = int(df_yoy["indic_sbs"].nunique()) if "indic_sbs" in df_yoy.columns else 0
    cov["yoy_year_min"] = int(df_yoy["year"].min()) if "year" in df_yoy.columns and len(df_yoy) else None
    cov["yoy_year_max"] = int(df_yoy["year"].max()) if "year" in df_yoy.columns and len(df_yoy) else None
    cov["yoy_missing_pct"] = float(df_yoy["yoy_pct"].isna().mean() * 100.0) if "yoy_pct" in df_yoy.columns and len(df_yoy) else 0.0

    cov["struct_rows"] = int(len(df_struct))
    cov["struct_has_cagr"] = bool("cagr_pct" in df_struct.columns and df_struct["cagr_pct"].notna().any())

    return cov


# ---------- THE FIX (your data has duplicate rows per (geo,indic,year)) ----------
def agg_country_year_value(df: pd.DataFrame) -> pd.DataFrame:
    needed = {"geo", "indic_sbs", "year", "value"}
    if not needed.issubset(df.columns) or df.empty:
        return df

    out = (
        df.dropna(subset=["geo", "indic_sbs", "year"])
          .groupby(["geo", "indic_sbs", "year"], as_index=False)["value"]
          .sum()
    )
    return out


def agg_country_year_yoy(df: pd.DataFrame) -> pd.DataFrame:
    needed = {"geo", "indic_sbs", "year", "value", "value_prev"}
    if not needed.issubset(df.columns) or df.empty:
        return df

    out = (
        df.dropna(subset=["geo", "indic_sbs", "year"])
          .groupby(["geo", "indic_sbs", "year"], as_index=False)[["value", "value_prev"]]
          .sum()
    )

    # recompute YoY after aggregation
    out["yoy_pct"] = (out["value"] / out["value_prev"] - 1.0) * 100.0
    out["yoy_pct"] = safe_numeric(out["yoy_pct"])
    return out


def dedupe_structural(df_struct: pd.DataFrame) -> pd.DataFrame:
    """
    Se a tabela estrutural tiver duplicados por geo/indic, pegamos a melhor linha:
    - maior n_years
    - maior year_last
    """
    if df_struct.empty:
        return df_struct
    if not {"geo", "indic_sbs"}.issubset(df_struct.columns):
        return df_struct

    tmp = df_struct

    if "n_years" in tmp.columns:
        tmp["n_years"] = safe_numeric(tmp["n_years"])
    else:
        tmp["n_years"] = pd.NA

    if "year_last" in tmp.columns:
        tmp["year_last"] = safe_numeric(tmp["year_last"])
    else:
        tmp["year_last"] = pd.NA

    tmp = tmp.sort_values(
        by=["geo", "indic_sbs", "n_years", "year_last"],
        ascending=[True, True, False, False],
        kind="mergesort",
    )
    tmp = tmp.drop_duplicates(subset=["geo", "indic_sbs"], keep="first")
    return tmp


def save_bar_chart(
    df: pd.DataFrame,
    title: str,
    outpath: Path,
    x_col: str,
    y_col: str,
    y_label: str,
    rotate_x: int = 0,
    clip_abs: float | None = None,
) -> None:
    if df.empty:
        return

    plot_df = df[[x_col, y_col]].copy()
    plot_df[x_col] = plot_df[x_col].astype(str)
    plot_df[y_col] = pd.to_numeric(plot_df[y_col], errors="coerce")
    plot_df = plot_df.dropna(subset=[y_col])

    if clip_abs is not None:
        plot_df[y_col] = plot_df[y_col].clip(lower=-abs(clip_abs), upper=abs(clip_abs))

    fig = plt.figure(figsize=(10.5, 4.8))
    ax = fig.add_subplot(111)

    ax.bar(plot_df[x_col], plot_df[y_col])
    ax.set_title(title)
    ax.set_xlabel("Geo")
    ax.set_ylabel(y_label)
    ax.tick_params(axis="x", rotation=rotate_x)

    fig.tight_layout()
    fig.savefig(outpath, dpi=170)
    plt.close(fig)


# =========================================================
# STORY/ROWS BUILDERS
# =========================================================
def build_rows_value(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, r in df.iterrows():
        rows.append({
            "geo": r.get("geo", "—"),
            "year": fmt_year(r.get("year")),
            "value": human_number(r.get("value")),
        })
    return rows


def build_rows_yoy(df: pd.DataFrame) -> list[dict]:
    """
    Aqui o pulo do gato: YoY pode arredondar para 0.0% quando é muito pequeno.
    Então além de YoY (%), mostramos Δ absoluto (value - prev) pra dar contexto.
    """
    rows: list[dict] = []
    for _, r in df.iterrows():
        value = r.get("value")
        prev = r.get("value_prev")
        delta_abs = None
        try:
            if pd.notna(value) and pd.notna(prev):
                delta_abs = float(value) - float(prev)
        except Exception:
            delta_abs = None

        rows.append({
            "geo": r.get("geo", "—"),
            "year": fmt_year(r.get("year")),
            "value": human_number(value),
            "prev": human_number(prev),
            "delta_abs": human_number(delta_abs),  # NOVO
            "yoy": pct1(r.get("yoy_pct")),         # 1 casa como você pediu
        })
    return rows


def build_rows_rank_delta(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, r in df.iterrows():
        rows.append({
            "geo": r.get("geo", "—"),
            "rank_base": int(r.get("rank_base")) if pd.notna(r.get("rank_base")) else None,
            "rank_last": int(r.get("rank_last")) if pd.notna(r.get("rank_last")) else None,
            "rank_delta": int(r.get("rank_delta")) if pd.notna(r.get("rank_delta")) else None,
            "value_base": human_number(r.get("value_base")),
            "value_last": human_number(r.get("value_last")),
            "pct_change": pct1(r.get("pct_change")),
        })
    return rows


def build_rows_cagr(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, r in df.iterrows():
        rows.append({
            "geo": r.get("geo", "—"),
            "years": f"{fmt_year(r.get('year_first'))}→{fmt_year(r.get('year_last'))}",
            "n_years": int(r.get("n_years")) if "n_years" in df.columns and pd.notna(r.get("n_years")) else None,
            "cagr": pct2(r.get("cagr_pct")),
            "pct_change": pct2(r.get("pct_change")),
            "abs_change": human_number(r.get("abs_change")),
        })
    return rows


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    ensure_dirs()

    # -------- Load
    df_top = pd.read_parquet(GOLD_COUNTRY_INDICATOR_YEAR)
    df_yoy = pd.read_parquet(GOLD_YOY_GROWTH)
    df_struct = pd.read_parquet(GOLD_STRUCTURAL_METRICS)

    # -------- Normalize numeric
    if "value" in df_top.columns:
        df_top["value"] = safe_numeric(df_top["value"])

    for c in ["value", "value_prev", "yoy_pct"]:
        if c in df_yoy.columns:
            df_yoy[c] = safe_numeric(df_yoy[c])

    # structural metrics: your file has "cagr" column (not cagr_pct)
    if "cagr" in df_struct.columns:
        df_struct["cagr_pct"] = safe_numeric(df_struct["cagr"])
    elif "cagr_pct" in df_struct.columns:
        df_struct["cagr_pct"] = safe_numeric(df_struct["cagr_pct"])
    else:
        df_struct["cagr_pct"] = pd.NA

    for c in ["abs_change", "pct_change", "yoy_mean", "yoy_volatility", "n_years", "year_first", "year_last"]:
        if c in df_struct.columns:
            df_struct[c] = safe_numeric(df_struct[c])

    # -------- FIX: aggregate duplicates to true country-year
    df_top = agg_country_year_value(df_top)
    df_yoy = agg_country_year_yoy(df_yoy)

    # -------- Optional: keep only countries (remove EU27_2020 etc.)
    # IMPORTANT: avoid .copy() here to prevent huge consolidation and RAM spikes
    if COUNTRY_ONLY and "geo" in df_top.columns:
        df_top = df_top.loc[df_top["geo"].astype(str).map(is_country_geo)]
    if COUNTRY_ONLY and "geo" in df_yoy.columns:
        df_yoy = df_yoy.loc[df_yoy["geo"].astype(str).map(is_country_geo)]
    if COUNTRY_ONLY and "geo" in df_struct.columns:
        df_struct = df_struct.loc[df_struct["geo"].astype(str).map(is_country_geo)]

    # -------- Deduplicate structural table (avoids repeated NL rows etc.)
    df_struct = dedupe_structural(df_struct)

    # -------- Coverage / quality
    coverage = compute_coverage(df_top, df_yoy, df_struct)
    quality = read_quality_report()

    # -------- Select indicator
    main_indic = pick_main_indicator(df_top)
    if main_indic is None:
        raise ValueError("Could not select main indicator (indic_sbs missing or empty).")

    # -------- Years
    if "year" not in df_top.columns or df_top.empty:
        raise ValueError("gold_country_indicator_year is missing year or is empty after filters/aggregation.")
    if "year" not in df_yoy.columns or df_yoy.empty:
        raise ValueError("gold_yoy_growth is missing year or is empty after filters/aggregation.")

    year_top = int(df_top["year"].max())
    year_yoy = int(df_yoy["year"].max())

    years_available = sorted([int(x) for x in df_top["year"].dropna().unique().tolist()])
    if RANK_BASE_YEAR is not None and RANK_BASE_YEAR in years_available:
        rank_base_year = RANK_BASE_YEAR
    else:
        candidate = year_top - 5
        rank_base_year = candidate if candidate in years_available else years_available[0]

    # -------- Filter to main indicator
    df_top_main = df_top.loc[df_top["indic_sbs"] == main_indic]
    df_yoy_main = df_yoy.loc[df_yoy["indic_sbs"] == main_indic]
    df_struct_main = df_struct.loc[df_struct["indic_sbs"] == main_indic] if "indic_sbs" in df_struct.columns else df_struct

    # -------- Top Value (latest year)
    df_top_latest = df_top_main.loc[df_top_main["year"] == year_top]
    df_top10_value = (
        df_top_latest.dropna(subset=["value"])
        .sort_values("value", ascending=False)
        .head(TOP_N)
        .loc[:, ["geo", "year", "value"]]
        .copy()
    )

    # -------- YoY (latest year) with sanity rules
    df_yoy_latest = df_yoy_main.loc[df_yoy_main["year"] == year_yoy].copy()
    df_yoy_latest = df_yoy_latest[df_yoy_latest["value_prev"].fillna(0) >= YOY_MIN_PREV_VALUE]
    df_yoy_latest = df_yoy_latest[df_yoy_latest["yoy_pct"].notna()]
    df_yoy_latest["yoy_pct"] = pd.to_numeric(df_yoy_latest["yoy_pct"], errors="coerce")
    df_yoy_latest = df_yoy_latest[np.isfinite(df_yoy_latest["yoy_pct"])]

    df_top10_yoy = (
        df_yoy_latest.sort_values("yoy_pct", ascending=False)
        .head(TOP_N)
        .loc[:, ["geo", "year", "value", "value_prev", "yoy_pct"]]
        .copy()
    )

    # -------- Rank Delta (base vs latest) - using dense ranks to handle ties
    base_rank = (
        df_top_main.loc[df_top_main["year"] == rank_base_year]
        .dropna(subset=["value"])
        .loc[:, ["geo", "value"]]
    )
    last_rank = (
        df_top_main.loc[df_top_main["year"] == year_top]
        .dropna(subset=["value"])
        .loc[:, ["geo", "value"]]
    )

    base_rank = base_rank.sort_values("value", ascending=False)
    last_rank = last_rank.sort_values("value", ascending=False)

    base_rank["rank_base"] = base_rank["value"].rank(method="dense", ascending=False).astype(int)
    last_rank["rank_last"] = last_rank["value"].rank(method="dense", ascending=False).astype(int)

    base_rank = base_rank.rename(columns={"value": "value_base"})
    last_rank = last_rank.rename(columns={"value": "value_last"})

    base_rank = base_rank.loc[:, ["geo", "value_base", "rank_base"]].copy()
    last_rank = last_rank.loc[:, ["geo", "value_last", "rank_last"]].copy()

    df_rank = base_rank.merge(last_rank, on="geo", how="inner")
    df_rank["rank_delta"] = df_rank["rank_base"] - df_rank["rank_last"]  # + means moved up
    df_rank["pct_change"] = (df_rank["value_last"] / df_rank["value_base"] - 1.0) * 100.0

    df_rank_up = df_rank.sort_values("rank_delta", ascending=False).head(TOP_N).copy()
    df_rank_down = df_rank.sort_values("rank_delta", ascending=True).head(TOP_N).copy()

    # -------- CAGR Top/Bottom (clean + min years)
    has_cagr = bool("cagr_pct" in df_struct_main.columns and df_struct_main["cagr_pct"].notna().any())
    df_top10_cagr = pd.DataFrame()
    df_bottom10_cagr = pd.DataFrame()

    if has_cagr:
        base_struct = df_struct_main.dropna(subset=["cagr_pct"]).copy()
        if "n_years" in base_struct.columns:
            base_struct = base_struct[base_struct["n_years"].fillna(0) >= CAGR_MIN_YEARS].copy()
        df_top10_cagr = base_struct.sort_values("cagr_pct", ascending=False).head(TOP_N).copy()
        df_bottom10_cagr = base_struct.sort_values("cagr_pct", ascending=True).head(TOP_N).copy()

    # -------- Charts
    chart_value = ASSETS_DIR / "top10_value.png"
    chart_yoy = ASSETS_DIR / "top10_yoy.png"
    chart_rank_up = ASSETS_DIR / "rank_movers_up.png"
    chart_rank_down = ASSETS_DIR / "rank_movers_down.png"
    chart_cagr_top = ASSETS_DIR / "top10_cagr.png"
    chart_cagr_bottom = ASSETS_DIR / "bottom10_cagr.png"

    if len(df_top10_value):
        save_bar_chart(
            df_top10_value,
            title=f"Top {TOP_N} countries by value — {main_indic} ({year_top})",
            outpath=chart_value,
            x_col="geo",
            y_col="value",
            y_label="Value",
            rotate_x=0,
            clip_abs=None,
        )

    if len(df_top10_yoy):
        save_bar_chart(
            df_top10_yoy,
            title=f"Top {TOP_N} YoY growth (%) — {main_indic} ({year_yoy}) | prev ≥ {YOY_MIN_PREV_VALUE:g}",
            outpath=chart_yoy,
            x_col="geo",
            y_col="yoy_pct",
            y_label="YoY (%)",
            rotate_x=0,
            clip_abs=YOY_CLIP_ABS_FOR_CHART,
        )

    if len(df_rank_up):
        save_bar_chart(
            df_rank_up,
            title=f"Rank movers UP (value rank) — {main_indic} ({rank_base_year}→{year_top})",
            outpath=chart_rank_up,
            x_col="geo",
            y_col="rank_delta",
            y_label="Δ rank (positive = up)",
            rotate_x=0,
            clip_abs=None,
        )

    if len(df_rank_down):
        tmp = df_rank_down.copy()
        tmp["rank_delta_abs"] = tmp["rank_delta"].abs()
        save_bar_chart(
            tmp,
            title=f"Rank movers DOWN (value rank) — {main_indic} ({rank_base_year}→{year_top})",
            outpath=chart_rank_down,
            x_col="geo",
            y_col="rank_delta_abs",
            y_label="Δ rank (magnitude)",
            rotate_x=0,
            clip_abs=None,
        )

    if has_cagr and len(df_top10_cagr):
        save_bar_chart(
            df_top10_cagr,
            title=f"Top {TOP_N} CAGR (%) — {main_indic} | min years={CAGR_MIN_YEARS}",
            outpath=chart_cagr_top,
            x_col="geo",
            y_col="cagr_pct",
            y_label="CAGR (%)",
            rotate_x=0,
            clip_abs=CAGR_CLIP_ABS_FOR_CHART,
        )

    if has_cagr and len(df_bottom10_cagr):
        save_bar_chart(
            df_bottom10_cagr,
            title=f"Bottom {TOP_N} CAGR (%) — {main_indic} | min years={CAGR_MIN_YEARS}",
            outpath=chart_cagr_bottom,
            x_col="geo",
            y_col="cagr_pct",
            y_label="CAGR (%)",
            rotate_x=0,
            clip_abs=CAGR_CLIP_ABS_FOR_CHART,
        )

    # -------- Insights (curtos, sem cara de IA)
    insights: list[dict] = []

    if len(df_top10_value):
        lead = df_top10_value.iloc[0]
        insights.append({
            "title": "Latest-year market leader",
            "text": f"In {year_top}, {lead['geo']} leads {main_indic} by total country value ({human_number(lead['value'])})."
        })

    if len(df_top10_yoy):
        best = df_top10_yoy.iloc[0]
        insights.append({
            "title": "Fastest YoY growth (cleaned)",
            "text": f"In {year_yoy}, {best['geo']} grows {pct1(best['yoy_pct'])} YoY (prev={human_number(best['value_prev'])})."
        })
        insights.append({
            "title": "Why YoY can look 'zero'",
            "text": "If Value and Prev are very close, YoY becomes a small fraction and rounds to 0.0% at 1 decimal. Use Δ abs to interpret small changes."
        })
        insights.append({
            "title": "Why YoY is filtered",
            "text": f"YoY excludes near-zero baselines (prev < {YOY_MIN_PREV_VALUE:g}) and the chart is clipped at ±{YOY_CLIP_ABS_FOR_CHART:g}% for readability."
        })

    if len(df_rank_up):
        m = df_rank_up.iloc[0]
        insights.append({
            "title": "Biggest rank gain (value rank)",
            "text": f"{m['geo']} improves by +{int(m['rank_delta'])} positions from {rank_base_year} to {year_top} (Δ%={pct1(m['pct_change'])})."
        })
        insights.append({
            "title": "How ties are handled",
            "text": "Ranks are computed with dense ranking (ties share the same rank). Rank deltas reflect changes in the ordered position, not row index."
        })

    if has_cagr and len(df_top10_cagr):
        c = df_top10_cagr.iloc[0]
        insights.append({
            "title": "Best long-run performer (CAGR)",
            "text": f"{c.get('geo')} shows the strongest CAGR: {pct2(c.get('cagr_pct'))} over {fmt_year(c.get('year_first'))}→{fmt_year(c.get('year_last'))} (min years={CAGR_MIN_YEARS})."
        })
        insights.append({
            "title": "Interpreting negative CAGR",
            "text": "Negative CAGR means the indicator decreases over the period. Whether that is good or bad depends on the indicator semantics."
        })

    # -------- Rows for HTML
    top_rows = build_rows_value(df_top10_value)
    yoy_rows = build_rows_yoy(df_top10_yoy)
    rank_up_rows = build_rows_rank_delta(df_rank_up)
    rank_down_rows = build_rows_rank_delta(df_rank_down)
    cagr_top_rows = build_rows_cagr(df_top10_cagr) if has_cagr else []
    cagr_bottom_rows = build_rows_cagr(df_bottom10_cagr) if has_cagr else []

    # -------- Render HTML
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template("gold_report.html")

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = template.render(
        title="Eurostat Lakehouse — Gold Report",
        generated_at=generated_at,
        main_indicator=main_indic,
        year_top=year_top,
        year_yoy=year_yoy,
        rank_base_year=rank_base_year,
        coverage=coverage,
        quality=quality,
        insights=insights,
        yoy_prev_threshold=YOY_MIN_PREV_VALUE,
        yoy_clip_abs=YOY_CLIP_ABS_FOR_CHART,
        cagr_clip_abs=CAGR_CLIP_ABS_FOR_CHART,
        cagr_min_years=CAGR_MIN_YEARS,
        top_n=TOP_N,
        country_only=COUNTRY_ONLY,

        chart_value=str(chart_value.relative_to(OUT_DIR)).replace("\\", "/"),
        chart_yoy=str(chart_yoy.relative_to(OUT_DIR)).replace("\\", "/"),
        chart_rank_up=str(chart_rank_up.relative_to(OUT_DIR)).replace("\\", "/"),
        chart_rank_down=str(chart_rank_down.relative_to(OUT_DIR)).replace("\\", "/"),
        chart_cagr_top=str(chart_cagr_top.relative_to(OUT_DIR)).replace("\\", "/") if has_cagr else None,
        chart_cagr_bottom=str(chart_cagr_bottom.relative_to(OUT_DIR)).replace("\\", "/") if has_cagr else None,

        top_rows=top_rows,
        yoy_rows=yoy_rows,
        rank_up_rows=rank_up_rows,
        rank_down_rows=rank_down_rows,
        cagr_top_rows=cagr_top_rows,
        cagr_bottom_rows=cagr_bottom_rows,
        has_cagr=has_cagr,
    )

    out_html = OUT_DIR / "gold_report.html"
    out_html.write_text(html, encoding="utf-8")

    print(f"Report generated: {out_html}")


if __name__ == "__main__":
    main()
