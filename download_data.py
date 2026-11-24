import argparse
import hashlib
import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

parser = argparse.ArgumentParser(description="Download and verify the public NYC TLC taxi inputs.")
parser.add_argument("--destination", default="raw")
args = parser.parse_args()
RAW = Path(args.destination).resolve()
RAW.mkdir(parents=True, exist_ok=True)
YEARS = (2023, 2024)
SOURCE = "https://d37ci6vzurychx.cloudfront.net"

manifest_path = RAW / "download_manifest.json"
previous_manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else []
previous = {row["filename"]: row for row in previous_manifest}

def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def parquet_signature(path):
    if path.stat().st_size < 12:
        return False
    with path.open("rb") as stream:
        first = stream.read(4)
        stream.seek(-4, 2)
        last = stream.read(4)
    return first == last == b"PAR1"

def download(item):
    filename, url = item
    destination = RAW / filename
    old = previous.get(filename)
    valid_existing = destination.exists() and old is not None
    if valid_existing:
        valid_existing = destination.stat().st_size == old["bytes"] and checksum(destination) == old["sha256"]
    if not valid_existing:
        partial = destination.with_suffix(destination.suffix + ".part")
        for attempt in range(3):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "IDS706-course-project/1.0"})
                with urllib.request.urlopen(request, timeout=120) as response, partial.open("wb") as stream:
                    expected = response.headers.get("Content-Length")
                    for chunk in iter(lambda: response.read(8 * 1024 * 1024), b""):
                        stream.write(chunk)
                if expected is not None:
                    assert partial.stat().st_size == int(expected), f"Incomplete download: {filename}"
                if filename.endswith(".parquet"):
                    assert parquet_signature(partial), f"Invalid Parquet file: {filename}"
                partial.replace(destination)
                break
            except Exception:
                if attempt == 2:
                    raise
    if filename.endswith(".parquet"):
        assert parquet_signature(destination)
    return dict(filename=filename, url=url, bytes=destination.stat().st_size, sha256=checksum(destination))

items = [(f"yellow_tripdata_{year}-{month:02d}.parquet",
          f"{SOURCE}/trip-data/yellow_tripdata_{year}-{month:02d}.parquet")
         for year in YEARS for month in range(1, 13)]
items.append(("taxi_zone_lookup.csv", f"{SOURCE}/misc/taxi_zone_lookup.csv"))
with ThreadPoolExecutor(max_workers=3) as pool:
    manifest = list(pool.map(download, items))
manifest_path.write_text(json.dumps(manifest, indent=2))
trip_bytes = sum(row["bytes"] for row in manifest if row["filename"].endswith(".parquet"))
assert trip_bytes >= 1024**3, f"Trip inputs are only {trip_bytes:,} bytes; add another full year before analysis."
print(f"Verified {len(items)-1} trip files; {trip_bytes/1024**3:.3f} GiB. Files: {RAW}")
