"""
Dataset Subsetting Script
Filters Retailrocket events.csv to the first 500k unique visitor IDs,
as required by the project specification (500k user subset).

Usage:
    python3 data/scripts/prepare_subset.py

Output:
    data/raw/retailrocket/events_500k.csv   <- subset used for all experiments
"""

import csv
import os

INPUT_FILE  = os.path.join(os.path.dirname(__file__), '../raw/retailrocket/events.csv')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '../raw/retailrocket/events_500k.csv')
USER_LIMIT  = 500_000

def prepare_subset():
    print("=" * 50)
    print("Retailrocket 500k User Subset Preparation")
    print("=" * 50)

    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} not found. Run download_datasets.sh first.")
        return

    if os.path.exists(OUTPUT_FILE):
        # Count existing rows
        with open(OUTPUT_FILE) as f:
            existing = sum(1 for _ in f) - 1  # minus header
        print(f"Subset already exists: {existing:,} events. Delete to regenerate.")
        return

    print(f"Reading {INPUT_FILE} ...")
    allowed_users = set()
    kept_rows = []

    with open(INPUT_FILE, newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            uid = int(row['visitorid'])
            # Collect unique users until we hit 500k
            if uid not in allowed_users:
                if len(allowed_users) < USER_LIMIT:
                    allowed_users.add(uid)
                else:
                    continue  # skip users beyond the 500k cap
            kept_rows.append(row)

    print(f"Unique users in subset : {len(allowed_users):,} (cap: {USER_LIMIT:,})")
    print(f"Events in subset       : {len(kept_rows):,}")

    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept_rows)

    size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"Written to             : {OUTPUT_FILE}  ({size_mb:.1f} MB)")
    print("Done. Use CLICKSTREAM_DATA=data/raw/retailrocket/events_500k.csv")

if __name__ == '__main__':
    prepare_subset()
