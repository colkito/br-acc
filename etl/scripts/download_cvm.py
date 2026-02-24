#!/usr/bin/env python3
"""Download CVM PAS (Processo Administrativo Sancionador) data.

Usage:
    python etl/scripts/download_cvm.py
    python etl/scripts/download_cvm.py --output-dir ./data/cvm
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click

sys.path.insert(0, str(Path(__file__).parent))
from _download_utils import download_file, validate_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# CVM open data portal CSVs
BASE_URL = "https://dados.cvm.gov.br/dados/PAS"
DATASETS = {
    "pas_resultado": f"{BASE_URL}/RESULTADO/DADOS/pas_resultado.csv",
    "pas_processo": f"{BASE_URL}/PROCESSO/DADOS/pas_processo.csv",
}


@click.command()
@click.option("--output-dir", default="./data/cvm", help="Output directory")
@click.option("--skip-existing/--no-skip-existing", default=True, help="Skip existing files")
@click.option("--timeout", type=int, default=300, help="Download timeout in seconds")
def main(output_dir: str, skip_existing: bool, timeout: int) -> None:
    """Download CVM PAS sanctions data."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    success_count = 0
    for name, url in DATASETS.items():
        dest = out / f"{name}.csv"
        if skip_existing and dest.exists():
            logger.info("Skipping (exists): %s", dest.name)
            success_count += 1
            continue

        logger.info("=== %s ===", name)
        if download_file(url, dest, timeout=timeout):
            validate_csv(dest, encoding="utf-8", sep=",")
            success_count += 1
        else:
            logger.warning("Failed to download %s", name)

    logger.info("=== Done: %d/%d datasets downloaded ===", success_count, len(DATASETS))
    if success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
