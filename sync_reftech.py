"""
Push one Attendee to RefTech, then update HubSpot reftech_sync_date / reftech_error fields.

Use from a workflow, job, or script after send_to_reftech is Yes and fields are ready.
"""

from __future__ import annotations

from typing import Any

from hubspot_attendee import apply_reftech_sync_result_to_hubspot
from reftech_client import RefTechClient


def resolve_reftech_attendee_id_explain(
    client: RefTechClient,
    hubspot_record_id: str,
    hubspot_delegate_id: str | None,
) -> tuple[str | None, list[str]]:
    """
    Same logic as resolve_reftech_attendee_id, plus human-readable lines for --dry-run.
    """
    notes: list[str] = []
    s = (hubspot_delegate_id or "").strip()
    if s:
        exists = client.attendee_id_exists(s)
        notes.append(f"HubSpot reftech_delegate_id: {s!r}")
        notes.append(f"That delegate id appears in RefTech attendee list: {exists}")
        if exists:
            notes.append(
                "→ Would UPDATE: POST submission.json with **either** attendee= OR "
                f"attendee_import_id= (not both); e.g. attendee={s!r} + form fields."
            )
            return s, notes
        notes.append(
            "Delegate id not in list (cleared in RefTech or stale); scanning by HubSpot id…"
        )
    else:
        notes.append("HubSpot reftech_delegate_id: (empty)")

    ex = client.get_attendee_by_hubspot_record_id(hubspot_record_id)
    rid = (ex or {}).get("id")
    if rid:
        notes.append(
            f"RefTech row found by import_id / recordid / list scan: delegate id {rid!r}"
        )
        notes.append(
            "→ Would UPDATE: POST submission.json with attendee= or attendee_import_id= "
            f"(e.g. attendee={str(rid)!r}) + form fields."
        )
        return str(rid), notes

    notes.append("No RefTech row matches this hs_object_id (import_id / recordid).")
    notes.append(
        "→ Would CREATE: POST submission.json with attendee_import_id "
        "(gro_* candidates) + form fields."
    )
    return None, notes


def resolve_reftech_attendee_id(
    client: RefTechClient,
    hubspot_record_id: str,
    hubspot_delegate_id: str | None,
) -> str | None:
    """
    Prefer HubSpot `reftech_delegate_id` when that delegate still exists in RefTech;
    else scan RefTech by import_id (gro_* candidates).

    If HubSpot still has a stale delegate after RefTech was wiped, we skip it so the
    next action is create instead of POSTing to a non-existent attendee id.
    """
    rid, _ = resolve_reftech_attendee_id_explain(
        client, hubspot_record_id, hubspot_delegate_id
    )
    return rid


def push_attendee_to_reftech_and_update_hubspot(
    hubspot_record_id: str,
    attendee_type_id: str,
    fields: dict[str, Any],
    *,
    reftech_attendee_id: str | None = None,
    status_id: str | None = None,
    status_name: str | None = None,
) -> tuple[str, str]:
    """
    POST registration to RefTech, then patch HubSpot on success or failure.

    Pass reftech_attendee_id when HubSpot already has reftech_delegate_id or after
    get_attendee_by_hubspot_record_id — avoids a second create when import_id fallbacks differ.

    Use optional status_id or status_name (or env REFTECH_SUBMIT_STATUS_*) to set RefTech attendee
    status; workflows may still override when adding an attendee.

    Returns (delegate id to store in HubSpot, raw `data` string from RefTech response).
    Re-raises after recording failure on HubSpot.
    """
    client = RefTechClient()
    hs_id = str(hubspot_record_id).strip()
    try:
        # Always pass hubspot_record_id for create dedupe + field context; query `id` is form type.
        reftech_id = client.submit_registration(
            attendee_type_id=attendee_type_id,
            fields=fields,
            hubspot_record_id=hs_id,
            reftech_attendee_id=reftech_attendee_id,
            status_id=status_id,
            status_name=status_name,
        )
        # Create: trust API `data` as the new delegate id. Update: keep HubSpot on the id we
        # sent — but if API `data` differs, callers should surface it (may indicate a new row).
        sent = (reftech_attendee_id or "").strip()
        delegate_for_hubspot = sent if sent else reftech_id
        apply_reftech_sync_result_to_hubspot(
            hs_id, success=True, reftech_delegate_id=delegate_for_hubspot
        )
        return delegate_for_hubspot, str(reftech_id).strip()
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        try:
            apply_reftech_sync_result_to_hubspot(hs_id, success=False, error_detail=msg)
        except Exception as hub_err:
            raise RuntimeError(
                f"RefTech error: {msg}. Also failed to write HubSpot error fields: {hub_err}"
            ) from e
        raise
