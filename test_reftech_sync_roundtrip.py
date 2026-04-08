#!/usr/bin/env python3
"""
Sync → verify RefTech rows by delegate id from HubSpot → resync → verify no duplicates.

RefTech list API returns opaque `import_id` hashes, not gro_* strings — verification
matches HubSpot `reftech_delegate_id` to RefTech row `id`.

Clears `reftech_delegate_id` in HubSpot before the first sync so the first run is
a true create (same as empty RefTech + no stale delegate ids).

Usage:  python3 test_reftech_sync_roundtrip.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from hubspot_attendee import patch_attendee_properties
from reftech_client import RefTechClient
import run_sync_all
from sync_common import hs_search, load_env


def load_env_local() -> None:
    load_env()


def hubspot_search_rows() -> list[dict]:
    load_env_local()
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    ot = os.environ.get("HUBSPOT_ATTENDEE_OBJECT_TYPE_ID", "2-133351180")
    if not token:
        raise SystemExit("HUBSPOT_ACCESS_TOKEN required")
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


def hs_ids_from_rows(rows: list[dict]) -> list[str]:
    out: list[str] = []
    for row in rows:
        p = row.get("properties") or {}
        hs = str(p.get("hs_object_id") or row.get("id") or "").strip()
        if hs:
            out.append(hs)
    return sorted(set(out))


def hubspot_delegate_map(rows: list[dict]) -> dict[str, str]:
    """hs_object_id -> reftech_delegate_id (may be empty)."""
    m: dict[str, str] = {}
    for row in rows:
        p = row.get("properties") or {}
        hs = str(p.get("hs_object_id") or row.get("id") or "").strip()
        if not hs:
            continue
        d = (p.get("reftech_delegate_id") or "").strip()
        m[hs] = d
    return m


def clear_hubspot_delegate_ids(hs_ids: list[str]) -> None:
    for hs in hs_ids:
        patch_attendee_properties(hs, {"reftech_delegate_id": ""})


def verify_hubspot_vs_reftech(
    label: str,
    client: RefTechClient,
    expected_hs_ids: list[str],
    hubspot_delegates: dict[str, str],
) -> bool:
    """Each HubSpot record must have a non-empty delegate id and exactly one RefTech row with that id."""
    print(f"\n--- {label} ---")
    ok = True
    all_rt = client.list_all_attendees()
    by_delegate = {str(r.get("id")): r for r in all_rt if r.get("id")}

    for hs in expected_hs_ids:
        d = (hubspot_delegates.get(hs) or "").strip()
        if not d:
            print(f"  FAIL  hs_object_id={hs}: HubSpot reftech_delegate_id is empty")
            ok = False
            continue
        row = by_delegate.get(d)
        if row is None:
            print(
                f"  FAIL  hs_object_id={hs}: no RefTech row with id={d!r} "
                f"(HubSpot delegate not found in RefTech list)"
            )
            ok = False
            continue
        dup = sum(1 for r in all_rt if (r.get("id") or "") == d)
        if dup != 1:
            print(f"  FAIL  hs_object_id={hs}: delegate {d!r} appears {dup} times in RefTech")
            ok = False
        else:
            print(
                f"  OK    hs={hs}  delegate={d!r}  import_id={str(row.get('import_id'))[:16]}…"
            )

    # No duplicate delegates across our HubSpot records
    dels = [(hs, hubspot_delegates.get(hs, "").strip()) for hs in expected_hs_ids]
    seen: set[str] = set()
    for hs, d in dels:
        if not d:
            continue
        if d in seen:
            print(f"  FAIL  duplicate delegate id {d!r} on multiple HubSpot records")
            ok = False
        seen.add(d)

    return ok


def main() -> None:
    load_env_local()
    rows0 = hubspot_search_rows()
    hs_ids = hs_ids_from_rows(rows0)
    if not hs_ids:
        print("No HubSpot attendees with send_to_reftech=Yes.", file=sys.stderr)
        sys.exit(1)
    print(f"HubSpot records to sync: {len(hs_ids)}  hs_object_ids={hs_ids}")

    print("\nClearing HubSpot reftech_delegate_id (fresh create on first sync)…")
    clear_hubspot_delegate_ids(hs_ids)

    client = RefTechClient()

    print("\n========== FIRST SYNC (expect create) ==========")
    run_sync_all.main()

    rows1 = hubspot_search_rows()
    delegates1 = hubspot_delegate_map(rows1)
    if not verify_hubspot_vs_reftech(
        "After first sync", client, hs_ids, delegates1
    ):
        sys.exit(2)

    print("\n========== SECOND SYNC (expect update only, same delegates) ==========")
    run_sync_all.main()

    rows2 = hubspot_search_rows()
    delegates2 = hubspot_delegate_map(rows2)
    if not verify_hubspot_vs_reftech(
        "After second sync", client, hs_ids, delegates2
    ):
        sys.exit(3)

    print("\n--- HubSpot delegate id stability ---")
    stable_ok = True
    for hs in hs_ids:
        a, b = delegates1.get(hs, "").strip(), delegates2.get(hs, "").strip()
        if a != b:
            print(f"  FAIL  hs={hs}: {a!r} -> {b!r}")
            stable_ok = False
        else:
            print(f"  OK    hs={hs}: still {a!r}")
    if not stable_ok:
        sys.exit(4)

    total_rt = len(client.list_all_attendees())
    our_delegates = {delegates2[hs].strip() for hs in hs_ids if delegates2.get(hs, "").strip()}
    matched = sum(1 for r in client.list_all_attendees() if (r.get("id") or "") in our_delegates)
    print(f"\nRefTech total attendees: {total_rt}")
    print(f"Rows matching our HubSpot delegate ids: {matched} (expect {len(hs_ids)})")
    if matched != len(hs_ids):
        print("FAIL: unexpected row count for our delegates")
        sys.exit(5)

    print("\nAll checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
