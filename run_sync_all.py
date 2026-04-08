#!/usr/bin/env python3
"""
Sync all HubSpot Attendees with send_to_reftech = Yes to RefTech, then update HubSpot.

Requires on the Attendee object:
  reftech_delegate_id (text) — optional but strongly recommended to avoid duplicate RefTech rows
  when import_id fallbacks (gro2_, gro3_, …) differ from lookup.

RefTech creates use submission.json + attendee_import_id; updates use attendee + fields on
submission.json (fallback attendee.json). Each row is GET-refreshed from HubSpot before push
so reftech_delegate_id is not stale from search.

Usage:  python3 run_sync_all.py
        python3 run_sync_all.py --dry-run   # HubSpot + RefTech read-only; shows create vs update plan
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from reftech_client import RefTechClient, reftech_import_id_from_hubspot

from sync_common import (
    HS_ATTENDEE_PROPERTIES,
    SyncFormBuilders,
    hs_get_record_properties,
    hs_search,
    load_env,
)
from sync_reftech import (
    push_attendee_to_reftech_and_update_hubspot,
    resolve_reftech_attendee_id,
    resolve_reftech_attendee_id_explain,
)

_print_lock = threading.Lock()


def _safe_print(*args: object, **kwargs: object) -> None:
    with _print_lock:
        print(*args, **kwargs)


def _sync_one(
    row: dict,
    *,
    token: str,
    ot: str,
    fb: SyncFormBuilders,
    fmap: dict,
    client: RefTechClient,
    dry_run: bool,
) -> None:
    props = dict(row.get("properties") or {})
    rid = str(row.get("id") or "").strip()
    if rid:
        try:
            fresh = hs_get_record_properties(token, ot, rid, HS_ATTENDEE_PROPERTIES)
            props.update(fresh)
        except Exception as e:
            _safe_print(f"    (warn) could not refresh HubSpot properties: {e}", file=sys.stderr)
    hs_id = str(props.get("hs_object_id") or row["id"])
    props["hs_object_id"] = hs_id
    name = f"{props.get('first_name') or ''} {props.get('last_name') or ''}".strip()
    _safe_print("---", hs_id, name or "(no name)", "|", props.get("attendee_type"))
    try:
        tid = fb.resolve_attendee_type_id(props, fmap)
        fields = fb.build_fields(props, fb.fields_for(tid))
        delegate = props.get("reftech_delegate_id")
        if dry_run:
            _resolved, notes = resolve_reftech_attendee_id_explain(client, hs_id, delegate)
            lines = [f"    {n}" for n in notes]
            if not _resolved:
                pri = reftech_import_id_from_hubspot(hs_id)
                lines.append(
                    f"    First attendee_import_id candidate for create: {pri!r} "
                    "(more candidates if RefTech returns 'Import ID already in use')."
                )
            lines.append(f"    Query param id (RefTech attendee type): {tid!r}")
            lines.append(
                f"    Form field keys to POST ({len(fields)}): "
                f"{', '.join(sorted(fields.keys()))}"
            )
            _safe_print("\n".join(lines))
            return
        rid_update = resolve_reftech_attendee_id(client, hs_id, delegate)
        mode = "update" if rid_update else "create"
        result_id, api_data = push_attendee_to_reftech_and_update_hubspot(
            hs_id, tid, fields, reftech_attendee_id=rid_update,
        )
        _safe_print(f"    {mode} OK RefTech id: {result_id}  (API response data: {api_data!r})")
        if rid_update and api_data != result_id:
            _safe_print(
                f"    WARNING: response data differs from delegate sent — RefTech may have "
                f"created a new row; check the UI / attendee list.",
                file=sys.stderr,
            )
    except Exception as e:
        _safe_print(f"    FAILED: {e}")


def main(*, dry_run: bool = False) -> None:
    load_env()

    # Allow the sync to be paused without redeploying.
    # Set SYNC_ENABLED=false (or 0 / no / off) in Railway Variables to disable.
    sync_enabled = os.environ.get("SYNC_ENABLED", "true").strip().lower()
    if sync_enabled in ("false", "0", "no", "off"):
        print("SYNC_ENABLED is disabled — exiting without syncing.")
        return

    token = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    rt_key = os.environ.get("REFTECH_API_KEY", "")
    if not token or not rt_key:
        print("Set HUBSPOT_ACCESS_TOKEN and REFTECH_API_KEY in .env", file=sys.stderr)
        sys.exit(1)
    ot = os.environ.get("HUBSPOT_ATTENDEE_OBJECT_TYPE_ID", "2-133351180")
    # Number of parallel workers. Default 10; override with REFTECH_SYNC_WORKERS.
    workers = int(os.environ.get("REFTECH_SYNC_WORKERS", "10"))

    fb = SyncFormBuilders(rt_key)
    fmap = fb.forms_map()
    # Pre-warm the fields cache for every attendee type so threads don't race on first fetch.
    for tid in fmap.values():
        fb.fields_for(tid)

    client = RefTechClient()
    if dry_run:
        print("DRY RUN — no RefTech POST, no HubSpot PATCH.\n")
        try:
            n_rt = len(client.list_all_attendees())
            print(f"RefTech: {n_rt} row(s) from GET /api/registration/attendees.json (paginated).\n")
        except Exception as e:
            print(f"RefTech: could not list attendees ({e}).\n", file=sys.stderr)

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

    print(f"{len(all_rows)} record(s) with send_to_reftech=Yes  (workers={workers})\n")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _sync_one, row,
                token=token, ot=ot, fb=fb, fmap=fmap,
                client=client, dry_run=dry_run,
            ): row
            for row in all_rows
        }
        for fut in as_completed(futures):
            exc = fut.exception()
            if exc:
                _safe_print(f"    UNHANDLED: {exc}", file=sys.stderr)

    print("\nDone.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Sync HubSpot Attendees (send_to_reftech=Yes) to RefTech."
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Read HubSpot + RefTech only; print planned create/update, no writes.",
    )
    ns = ap.parse_args()
    main(dry_run=ns.dry_run)
