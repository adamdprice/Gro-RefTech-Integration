"""
Shared HubSpot search + RefTech form helpers for sync scripts.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

HS_ATTENDEE_PROPERTIES = [
    "first_name",
    "last_name",
    "company_name",
    "email",
    "job_title",
    "attendee_type",
    "badge_attendee_type",
    "company_type",
    "payment_status",
    "country",
    "exhibiting_as_associated_festival_sponsor",
    "festival_code",
    "hs_object_id",
    "reftech_delegate_id",
]


def load_env() -> None:
    env_path = Path(__file__).resolve().parent.joinpath(".env")
    if not env_path.exists():
        return  # Railway injects env vars directly — no .env file needed
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def hs_search(
    token: str,
    object_type: str,
    after: str | None = None,
    festival_code: str | None = None,
) -> dict:
    filters = [
        {
            "propertyName": "send_to_onsite_badge_printing_system",
            "operator": "EQ",
            "value": "Yes",
        }
    ]
    if festival_code:
        filters.append(
            {"propertyName": "festival_code", "operator": "EQ", "value": festival_code}
        )
    body: dict = {
        "filterGroups": [{"filters": filters}],
        "properties": HS_ATTENDEE_PROPERTIES,
        "limit": 100,
    }
    if after:
        body["after"] = after
    req = Request(
        f"https://api.hubapi.com/crm/v3/objects/{object_type}/search",
        data=json.dumps(body).encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())


def hs_get_record_properties(
    token: str, object_type: str, record_id: str, properties: list[str]
) -> dict[str, str]:
    q = urlencode({"properties": ",".join(properties)})
    url = f"https://api.hubapi.com/crm/v3/objects/{object_type}/{record_id}?{q}"
    req = Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=60) as r:
        out = json.loads(r.read().decode())
    raw = out.get("properties") or {}
    return {k: ("" if raw.get(k) is None else str(raw.get(k))) for k in properties}


def rt_get(key: str, path: str, params: dict) -> dict:
    base = os.environ.get("REFTECH_API_BASE_URL", "https://eventreference.com").rstrip("/")
    q = urlencode({**params, "key": key})
    url = f"{base}{path}?{q}"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def normalize_field_value_for_submission(
    fid: str, val: object, fdef: dict | None
) -> object:
    """
    Clip values to EventReference Data Submission limits (field `type` from fields.json):
    text 255, text_area 5000, date 10 (YYYY-MM-DD), integer/select/radio 255/10, checkbox IDs.
    """
    if val is None:
        return None
    if fdef is None:
        if isinstance(val, str):
            return val[:255]
        if isinstance(val, list):
            return [str(x)[:10] for x in val]
        return val
    ftype = (fdef.get("type") or "").strip().lower()
    if ftype == "checkbox":
        if not isinstance(val, list):
            return val
        return [str(x).strip()[:10] for x in val if str(x).strip()]
    if isinstance(val, list):
        return val
    s = str(val).strip()
    if not s:
        return None
    if ftype in ("text_area", "textarea"):
        return s[:5000]
    if ftype in ("text", "integer"):
        return s[:255]
    if ftype == "date":
        return s[:10]
    if ftype in ("select", "radio"):
        return s[:10]
    return s[:255]


class SyncFormBuilders:
    """RefTech forms.json + field mapping for HubSpot Attendee → RefTech POST body."""

    def __init__(self, rt_key: str) -> None:
        self._rt_key = rt_key

    def forms_map(self) -> dict[str, str]:
        m: dict[str, str] = {}
        for row in rt_get(self._rt_key, "/api/registration/forms.json", {}).get("data") or []:
            m[row["name"].strip().lower()] = row["id"]
        return m

    def fields_for(self, tid: str) -> dict:
        if not hasattr(self, "_fields_cache"):
            self._fields_cache: dict[str, dict] = {}
        if tid not in self._fields_cache:
            out = rt_get(self._rt_key, "/api/registration/fields.json", {"id": tid})
            self._fields_cache[tid] = {f["id"]: f for f in (out.get("data") or [])}
        return self._fields_cache[tid]

    @staticmethod
    def choice_id(field_meta: dict, hubspot_label: str | None) -> str | None:
        if not hubspot_label or not str(hubspot_label).strip():
            return None
        hl = str(hubspot_label).strip()
        choices = field_meta.get("choices") or []
        for c in choices:
            if c["name"].strip().lower() == hl.lower():
                return c["id"]
        hll = hl.lower()
        for c in choices:
            cl = c["name"].lower()
            if hll in cl or cl in hll:
                return c["id"]
        return None

    @staticmethod
    def payment_id(field_meta: dict, hs_val: object) -> str | None:
        choices = {c["name"].lower(): c["id"] for c in (field_meta.get("choices") or [])}
        if hs_val is None or str(hs_val).strip() == "":
            return choices.get("unpaid")
        s = str(hs_val).strip().lower()
        if s in ("yes", "paid", "true", "y"):
            return choices.get("paid")
        if s in ("no", "unpaid", "false", "n"):
            return choices.get("unpaid")
        if "paid" in s and "un" not in s[:2]:
            return choices.get("paid")
        return choices.get("unpaid")

    def resolve_attendee_type_id(self, props: dict, fmap: dict[str, str]) -> str:
        at = (props.get("attendee_type") or "").strip()
        k = at.lower()
        if k in fmap:
            return fmap[k]
        for name, tid in fmap.items():
            if k in name or name in k:
                return tid
        raise ValueError(f"Unknown attendee_type {at!r}")

    def build_fields(self, props: dict, fdefs: dict) -> dict:
        out: dict = {}

        def put(fid: str, val: object) -> None:
            if val is not None and val != "":
                out[fid] = val

        put("firstname", (props.get("first_name") or "").strip())
        put("lastname", (props.get("last_name") or "").strip())
        put("companyname", (props.get("company_name") or "").strip())
        put("email", (props.get("email") or "").strip())
        put("jobtitle", (props.get("job_title") or "").strip())
        ex = props.get("exhibiting_as_associated_festival_sponsor")
        put("exhibitingas", (ex or "").strip() if ex else None)
        # Custom form field (fields.json id `recordid`): HubSpot record id — not RefTech delegate id.
        put("recordid", str(props.get("hs_object_id") or "").strip())
        if "companytype" in fdefs and props.get("company_type"):
            cid = self.choice_id(fdefs["companytype"], props["company_type"])
            if cid:
                out["companytype"] = cid
        if "badgetype" in fdefs and props.get("badge_attendee_type"):
            cid = self.choice_id(fdefs["badgetype"], props["badge_attendee_type"])
            if cid:
                out["badgetype"] = cid
        if "paymentstatus" in fdefs:
            pid = self.payment_id(fdefs["paymentstatus"], props.get("payment_status"))
            if pid:
                out["paymentstatus"] = pid
        if "countryid" in fdefs and props.get("country"):
            cid = self.choice_id(fdefs["countryid"], props["country"])
            if cid:
                out["countryid"] = cid
        clipped: dict = {}
        for fid, val in out.items():
            fdef = fdefs.get(fid)
            nv = normalize_field_value_for_submission(fid, val, fdef)
            if nv is not None and nv != "" and nv != []:
                clipped[fid] = nv
        return clipped
