# src/config.py
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

DATA_RAW = REPO_ROOT / "data-raw"
DATA_BRONZE = REPO_ROOT / "data-bronze"
DATA_SILVER = REPO_ROOT / "data-silver"
DATA_GOLD = REPO_ROOT / "data-gold"
OUTPUTS_CHECKS = REPO_ROOT / "outputs-checks"

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"

# seus 4 datasets (ajuste se quiser)
DATASETS = [
    "estat_sbs_ovw_act",
    "estat_sbs_ovw_ieq",
    "estat_sbs_ovw_smc",
    "estat_sbs_sc_ovw",
]
