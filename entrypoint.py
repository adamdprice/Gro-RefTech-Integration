#!/usr/bin/env python3
"""
Railway worker entrypoint.
Runs the sync immediately on startup, then every 30 minutes.
"""
import time
import run_sync_all

INTERVAL_SECONDS = 30 * 60  # 30 minutes

while True:
    run_sync_all.main()
    print(f"\nNext sync in {INTERVAL_SECONDS // 60} minutes…\n", flush=True)
    time.sleep(INTERVAL_SECONDS)
