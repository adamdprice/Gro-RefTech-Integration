#!/usr/bin/env python3
"""
Run HubSpot → RefTech sync with **verification** by HubSpot `recordid` (POST field `recordid`).

After **each** update attempt, recounts RefTech rows whose recordid/record_id matches `hs_object_id`.
If that count **increases**, that is a **failure** (duplicate registration for the same HubSpot id).
Rotates update strategies until one passes the count check, or `--max-failures` is reached.

Updates use **delegate id only** (`attendee=` on POST submission.json), not `attendee_import_id`.

Flow:
  1) Create any attendee that has no RefTech row yet (single pass), unless --skip-create.
  2) Re-read HubSpot so `reftech_delegate_id` is fresh.
  3) Loop up to N failures: run one update (delegate id only); if recordid count increases,
     count a failure and retry; otherwise SUCCESS.

Usage:
  python3 run_sync_verified.py
  python3 run_sync_verified.py --max-failures 20 --skip-create   # only test updates
"""

from __future__ import annotations

import argparse
import os
import sys

from reftech_client import RefTechClient
from sync_common import HS_ATTENDEE_PROPERTIES, SyncFormBuilders, hs_get_record_properties, hs_search, load_env
from sync_reftech import push_attendee_to_reftech_and_update_hubspot, resolve_reftech_attendee_id

def fetch_hubspot_rows(token: str, ot: str) -> list[dict]:
    all_rows: list = []
    after = None
    while True:
        page = hs_search(token, ot, after)
        all_rows.extend(page.get("results") or [])
        paging = page.get("paging") or {}
        if "next" not in paging:
            break
        after = paging["next"].get("after")
        if not after:
            break
    return all_rows


def hydrate_props(token: str, ot: str, row: dict) -> dict:
    props = dict(row.get("properties") or {})
    rid = str(row.get("id") or "").strip()
    if rid:
        try:
            fresh = hs_get_record_properties(token, ot, rid, HS_ATTENDEE_PROPERTIES)
            props.update(fresh)
        except Exception as e:
            print(f"    (warn) could not refresh HubSpot properties: {e}", file=sys.stderr)
    hs_id = str(props.get("hs_object_id") or row["id"])
    props["hs_object_id"] = hs_id
    return props


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Sync with RefTech row-count verification by HubSpot recordid."
    )
    ap.add_argument(
        "--max-failures",
        type=int,
        default=20,
        metavar="N",
        help="Stop after N duplicate-detection failures (default: 20).",
    )
    ap.add_argument(
        "--skip-create",
        action="store_true",
        help="Do not run create phase; only test update strategies (needs reftech_delegate_id).",
    )
    ns = ap.parse_args()
    max_failures = max(1, min(ns.max_failures, 500))

    load_env()
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    rt_key = os.environ.get("REFTECH_API_KEY", "")
    if not token or not rt_key:
        print("Set HUBSPOT_ACCESS_TOKEN and REFTECH_API_KEY in .env", file=sys.stderr)
        sys.exit(1)
    ot = os.environ.get("HUBSPOT_ATTENDEE_OBJECT_TYPE_ID", "2-133351180")

    fb = SyncFormBuilders(rt_key)
    fmap = fb.forms_map()
    client = RefTechClient()

    raw_rows = fetch_hubspot_rows(token, ot)
    print(f"{len(raw_rows)} record(s) with send_to_reftech=Yes\n")

    def load_prepared() -> list[dict]:
        out: list[dict] = []
        for row in fetch_hubspot_rows(token, ot):
            props = hydrate_props(token, ot, row)
            hs_id = props["hs_object_id"]
            name = f"{props.get('first_name') or ''} {props.get('last_name') or ''}".strip()
            tid = fb.resolve_attendee_type_id(props, fmap)
            fields = fb.build_fields(props, fb.fields_for(tid))
            delegate = props.get("reftech_delegate_id")
            rid_update = resolve_reftech_attendee_id(client, hs_id, delegate)
            out.append(
                {
                    "hs_id": hs_id,
                    "name": name or "(no name)",
                    "attendee_type": props.get("attendee_type"),
                    "tid": tid,
                    "fields": fields,
                    "rid_update": rid_update,
                    "props": props,
                }
            )
        return out

    prepared = load_prepared()

    # --- Phase 1: creates only ---
    if not ns.skip_create:
        for pr in prepared:
            if pr["rid_update"]:
                continue
            hs_id = pr["hs_id"]
            before = client.count_attendees_with_hubspot_record_id(hs_id)
            print(
                f"--- CREATE {hs_id} {pr['name']} | {pr['attendee_type']}\n"
                f"    RefTech rows with this recordid before: {before}"
            )
            try:
                rid, api_data = push_attendee_to_reftech_and_update_hubspot(
                    hs_id, pr["tid"], pr["fields"], reftech_attendee_id=None
                )
                after = client.count_attendees_with_hubspot_record_id(hs_id)
                print(
                    f"    create OK delegate={rid!r} API data={api_data!r}\n"
                    f"    RefTech rows with this recordid after: {after}"
                )
                if after < before:
                    print("    (unexpected: count dropped)", file=sys.stderr)
            except Exception as e:
                print(f"    FAILED: {e}")
        prepared = load_prepared()

    update_rows = [p for p in prepared if p["rid_update"]]
    if not update_rows:
        print("\nNo rows in update mode (missing RefTech delegate + lookup). Done.")
        return

    hs_ids = [p["hs_id"] for p in update_rows]
    for hid in hs_ids:
        c = client.count_attendees_with_hubspot_record_id(hid)
        if c > 1:
            print(
                f"\nWARNING: RefTech already has {c} row(s) with recordid={hid!r}. "
                "Delete duplicates for a clean test, or we only detect *new* duplicates.\n",
                file=sys.stderr,
            )

    # --- Phase 2: delegate id updates until recordid count does not increase (max N failures) ---
    print(
        "\n========== UPDATE VERIFICATION (recordid row counts; failure = count increased) =========="
        f"\nMax failures: {max_failures}\n"
    )
    failures = 0
    attempt_idx = 0
    while failures < max_failures:
        prepared = load_prepared()
        update_rows = [p for p in prepared if p["rid_update"]]
        if not update_rows:
            print("No update rows left. Exiting.", file=sys.stderr)
            sys.exit(1)
        hs_ids = [p["hs_id"] for p in update_rows]
        attempt_idx += 1

        before = {hid: client.count_attendees_with_hubspot_record_id(hid) for hid in hs_ids}
        print(
            f"--- Probe #{attempt_idx}  POST submission.json with attendee=<delegate id> only  "
            f"failures_so_far={failures}/{max_failures}\n"
            f"    recordid counts BEFORE: {before}"
        )
        probe_failed = False
        for pr in update_rows:
            hs_id = pr["hs_id"]
            print(f"    → update {hs_id} {pr['name']}")
            try:
                rid, api_data = push_attendee_to_reftech_and_update_hubspot(
                    hs_id,
                    pr["tid"],
                    pr["fields"],
                    reftech_attendee_id=pr["rid_update"],
                )
                print(f"      delegate={rid!r}  API data={api_data!r}")
                if api_data != rid:
                    print(
                        "      (note: API data != delegate id — duplicate risk; using row counts)",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(f"      ERROR: {e}")
                probe_failed = True
                break
        if probe_failed:
            failures += 1
            print(f"    COUNT AS FAILURE {failures} (request error)\n")
            continue

        after = {hid: client.count_attendees_with_hubspot_record_id(hid) for hid in hs_ids}
        print(f"    recordid counts AFTER:  {after}")
        dup_hids = [hid for hid in hs_ids if after[hid] > before[hid]]
        if dup_hids:
            failures += 1
            print(
                f"\n*** FAILURE {failures}/{max_failures}: duplicate row(s) for same recordid "
                f"{dup_hids} (before {before} → after {after})\n"
            )
            continue

        print(
            "\nSUCCESS: no extra RefTech row for these record ids "
            "(delegate id update only).\n"
            "Then use:  python3 run_sync_all.py\n"
        )
        return

    print(
        f"\nStopped after {max_failures} duplicate (or error) failures. "
        "Remove duplicate attendees in RefTech, ask EventReference for a true in-place update API, "
        "or try a different form/field set.",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
