"""Download official NYC TLC HVFHV trip Parquet files and taxi-zone lookup.

Usage:
    python scripts/download_data.py --months 1  # single-month smoke test
    python scripts/download_data.py              # full 4-month, >1GB dataset
"""
from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data' / 'raw'
DATA_BASE = 'https://d37ci6vzurychx.cloudfront.net'


def download(url: str, dest: Path, min_bytes: int) -> int:
    if dest.exists() and dest.stat().st_size >= min_bytes:
        print(f'[SKIP] {dest.name}: {dest.stat().st_size / 1e6:.1f} MB')
        return dest.stat().st_size

    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_name(dest.name + '.part')
    for attempt in range(1, 4):
        try:
            request = Request(url, headers={'User-Agent': 'week13-pyspark-class-project/1.0'})
            with urlopen(request, timeout=90) as response, partial.open('wb') as f:
                shutil.copyfileobj(response, f, length=2 * 1024 * 1024)
            size = partial.stat().st_size
            if size < min_bytes:
                raise ValueError(f'Unexpectedly small download: {size:,} bytes')
            partial.replace(dest)
            print(f'[OK] {dest.name}: {size / 1e6:.1f} MB')
            return size
        except Exception as exc:
            partial.unlink(missing_ok=True)
            print(f'[WARN] Attempt {attempt}/3 for {dest.name}: {exc}')
            if attempt == 3:
                raise RuntimeError(f'Could not download {url}') from exc
            time.sleep(attempt * 2)
    raise RuntimeError('unreachable')


def main() -> None:
    parser = argparse.ArgumentParser(description='Download official TLC Parquet files.')
    parser.add_argument('--months', type=int, default=4, choices=(1, 2, 3, 4))
    args = parser.parse_args()
    total = 0
    for month in range(1, args.months + 1):
        filename = f'fhvhv_tripdata_2024-{month:02d}.parquet'
        total += download(f'{DATA_BASE}/trip-data/{filename}', RAW / filename, 10_000_000)
    download(f'{DATA_BASE}/misc/taxi_zone_lookup.csv', RAW / 'taxi_zone_lookup.csv', 100)
    print(f'\nDownloaded trip Parquet total: {total:,} bytes = {total / 1e9:.3f} GB')
    if total >= 1_000_000_000:
        print('[PASS] At least 1 GB of raw trip Parquet data.')
    else:
        print('[NOTE] Less than 1 GB. Download all 4 months before final submission.')


if __name__ == '__main__':
    main()
