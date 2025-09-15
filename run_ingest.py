#!/usr/bin/env python3
"""
Run all bronze ingestions and commit logs to data-ingest/logs/
"""
import json, datetime, os, subprocess
from src.gov import hmlr, epc
from src.crawl import firecrawl_rm_zoopla

LOG_DIR = "logs"

def save_log(payload):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    fname = f"{LOG_DIR}/{payload['source']}_{ts}.json"
    with open(fname, "w") as f:
        json.dump(payload, f, indent=2)
    # git commit
    subprocess.run(["git", "add", fname])
    subprocess.run(["git", "commit", "-m", f"ingest log {payload['source']} {ts}"])

if __name__ == "__main__":
    os.makedirs(LOG_DIR, exist_ok=True)
    save_log(hmlr.ingest_yearly_txt())
    save_log(epc.ingest_domestic_zip())
    save_log(firecrawl_rm_zoopla.ingest_news())
    print("🎉 all bronze ingestions complete – logs committed.")
