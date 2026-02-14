import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[0]  # .../src
PROJECT = BASE.parent                       # .../ (raiz do repo)

steps = [
    PROJECT / "src" / "01_extract_raw.py",
    PROJECT / "src" / "02_bronze_ingest.py",
    PROJECT / "src" / "03_silver_transform.py",
    PROJECT / "src" / "04_gold_analytics.py",
    PROJECT / "src" / "05_quality_checks.py",
]

for s in steps:
    print(f"\n=== Running {s} ===")
    r = subprocess.run([sys.executable, str(s)], cwd=str(PROJECT))
    if r.returncode != 0:
        raise SystemExit(f"Step failed: {s}")

print("\nPipeline finished OK.")
