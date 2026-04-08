"""
Update Gro Attendee custom object after a RefTech sync attempt.

Success: sets reftech_sync_date, clears reftech_error and reftech_error_date.
Failure: sets reftech_error and reftech_error_date (does not change reftech_sync_date).

HubSpot `date` properties expect a UTC timestamp at midnight in milliseconds (string).
If your properties are `datetime` instead, set HUBSPOT_DATE_VALUE_FORMAT=iso in .env.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from reftech_client import load_dotenv

HUBSPOT_API = "https://api.hubapi.com"

# Default object type id for custom object Attendee (from your HubSpot setup).
DEFAULT_ATTENDEE_OBJECT_TYPE = "2-133351180"

# Internal property names on the Attendee object
PROP_SYNC_DATE = "reftech_sync_date"
PROP_ERROR = "reftech_error"
PROP_ERROR_DATE = "reftech_error_date"
# RefTech short delegate id (e.g. LHt) — source of truth for updates; avoids duplicate creates.
PROP_DELEGATE_ID = "reftech_delegate_id"


def _utc_now_ms() -> str:
    """HubSpot datetime: milliseconds since epoch for the current moment."""
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def _now_iso_utc() -> str:
    """HubSpot datetime properties: ISO 8601 in UTC."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _date_values_for_hubspot() -> tuple[str, str]:
    """
    Returns (sync_or_error_date_value, error_date_value) using env format.
    For datetime properties both use the same ISO instant; for date properties both use today ms.
    """
    fmt = (os.environ.get("HUBSPOT_DATE_VALUE_FORMAT") or "ms").strip().lower()
    if fmt == "iso":
        v = _now_iso_utc()
        return v, v
    return _utc_now_ms(), _utc_now_ms()


def patch_attendee_properties(
    hubspot_record_id: str,
    properties: dict[str, str],
    *,
    access_token: str | None = None,
    object_type_id: str | None = None,
) -> dict[str, Any]:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    token = access_token or os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
    if not token:
        raise ValueError("HUBSPOT_ACCESS_TOKEN is required")
    ot = object_type_id or os.environ.get(
        "HUBSPOT_ATTENDEE_OBJECT_TYPE_ID", DEFAULT_ATTENDEE_OBJECT_TYPE
    )
    rid = str(hubspot_record_id).strip()
    url = f"{HUBSPOT_API}/crm/v3/objects/{ot}/{rid}"
    body = json.dumps({"properties": properties}).encode("utf-8")
    req = Request(
        url,
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HubSpot PATCH {ot}/{rid} failed: HTTP {e.code} {detail[:2000]}"
        ) from e


def apply_reftech_sync_result_to_hubspot(
    hubspot_record_id: str,
    *,
    success: bool,
    error_detail: str | None = None,
    reftech_delegate_id: str | None = None,
) -> None:
    """
    Record RefTech sync outcome on the Attendee record.

    On success: reftech_sync_date = now (date); clears reftech_error and reftech_error_date.
    If reftech_delegate_id is set, writes reftech_delegate_id (RefTech row id for updates).
    On failure: reftech_error = message; reftech_error_date = now (date).
    """
    sync_d, err_d = _date_values_for_hubspot()
    if success:
        props: dict[str, str] = {
            PROP_SYNC_DATE: sync_d,
            PROP_ERROR: "",
            PROP_ERROR_DATE: "",
            "send_to_reftech": "",
        }
        if reftech_delegate_id and str(reftech_delegate_id).strip():
            props[PROP_DELEGATE_ID] = str(reftech_delegate_id).strip()
        patch_attendee_properties(hubspot_record_id, props)
        return

    msg = (error_detail or "Unknown error").strip()
    if len(msg) > 10000:
        msg = msg[:9997] + "..."
    patch_attendee_properties(
        hubspot_record_id,
        {
            PROP_ERROR: msg,
            PROP_ERROR_DATE: err_d,
        },
    )
