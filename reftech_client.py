"""
RefTech (EventReference) Registration API — minimal client.

Uses HubSpot `hs_object_id` as the logical key. RefTech `attendee_import_id` / GET `import_id`
use a prefixed string (default `gro_` + object id, e.g. `gro_398794255606`). Pass only the
numeric `hs_object_id` into this module — do not pre-prefix. See `reftech_import_id_from_hubspot`.

Identifiers (EventReference **Data Submission** `/api/registration/submission.json`):
- Query **`key`**: API key (required). **`id`**: attendee type / form id — **required for create**,
  **optional when updating** an existing record.
- **`attendee`** and **`attendee_import_id`** (for update targeting) are **query string parameters**,
  NOT POST body fields. The POST body contains only form field data and optional `status_id`/`status_name`.
- **Create:** omit `attendee` query param. Use **`attendee_import_id`** in POST body for import GUID.
- **Update:** pass **`attendee=<delegate id>`** as a **query string parameter**. Do not put it in the body.
- Optional POST body **`status_id`** / **`status_name`** (max 50) — attendee status; workflows may
  overwrite on add.
- **`import_id`** on list/GET — matching only, not for POST on update.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def reftech_import_id_candidates(hs_object_id: str) -> list[str]:
    """
    Import ids to try for GET / list matching (order matters for POST primary id).
    Default primary: gro{numeric_id}. Also tries hs_{numeric} and plain numeric for legacy.
    """
    load_dotenv(Path(__file__).resolve().parent / ".env")
    sid = str(hs_object_id).strip()
    if not sid:
        return []
    # Default `gro_` (with underscore): `gro398794255606` alone can be rejected as
    # "Import ID already in use" on some RefTech builds; `gro_398794255606` works.
    p = os.environ.get("REFTECH_HUBSPOT_IMPORT_PREFIX", "gro_")
    keys: list[str] = []
    if sid.startswith(p):
        keys.append(sid)
    else:
        keys.append(f"{p}{sid}")
    legacy_hs = f"hs_{sid}"
    if legacy_hs not in keys:
        keys.append(legacy_hs)
    legacy_gro = f"gro{sid}"
    if legacy_gro not in keys:
        keys.append(legacy_gro)
    if os.environ.get("REFTECH_IMPORT_TRY_LEGACY_IDS", "1") == "1" and sid not in keys:
        keys.append(sid)
    # RefTech may ghost-reserve earlier ids; these still map 1:1 from HubSpot hs_object_id.
    for alt in (
        f"gro2_{sid}",
        f"hubspot_{sid}",
        f"gro3_{sid}",
        f"gro4_{sid}",
    ):
        if alt not in keys:
            keys.append(alt)
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


def reftech_import_id_from_hubspot(hs_object_id: str) -> str:
    """Primary import string for POST `attendee_import_id` (first candidate)."""
    c = reftech_import_id_candidates(hs_object_id)
    return c[0] if c else ""


def reftech_rows_for_hubspot_record(
    attendees: list[dict[str, Any]], hubspot_record_id: str
) -> list[dict[str, Any]]:
    """Rows whose `import_id` matches any candidate for this HubSpot `hs_object_id`."""
    cand = set(reftech_import_id_candidates(hubspot_record_id))
    return [r for r in attendees if (r.get("import_id") or "") in cand]


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


class RefTechClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
    ) -> None:
        load_dotenv(Path(__file__).resolve().parent / ".env")
        self.api_key = api_key or os.environ.get("REFTECH_API_KEY", "")
        self.base_url = (base_url or os.environ.get(
            "REFTECH_API_BASE_URL", "https://eventreference.com"
        )).rstrip("/")
        if not self.api_key:
            raise ValueError("REFTECH_API_KEY is required")

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        q = urlencode({**params, "key": self.api_key})
        url = f"{self.base_url}{path}?{q}"
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post_form(
        self,
        path: str,
        query: dict[str, Any],
        body: list[tuple[str, str]],
    ) -> dict[str, Any]:
        q = urlencode({**query, "key": self.api_key})
        url = f"{self.base_url}{path}?{q}"
        data = urlencode(body, doseq=True).encode("utf-8")
        req = Request(
            url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )
        with urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _post_form_json(
        self,
        path: str,
        query: dict[str, Any],
        body: list[tuple[str, str]],
    ) -> dict[str, Any]:
        """POST form; on HTTP error return JSON body if present."""
        try:
            return self._post_form(path, query, body)
        except HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError:
                return {
                    "status": False,
                    "errors": [f"HTTP {e.code}: {raw[:500]}"],
                }
            if isinstance(parsed, dict):
                return parsed
            return {"status": False, "errors": [f"HTTP {e.code}"]}

    def get_attendee_by_hubspot_record_id(
        self, hubspot_record_id: str
    ) -> dict[str, Any] | None:
        """
        GET /api/registration/attendee.json?import_id=<prefixed_id>

        Pass HubSpot numeric `hs_object_id`. Tries GET `import_id` for each candidate,
        then scans attendees.json for:
          1) `recordid` / `record_id` / `hubspot_record_id` == hs_object_id (canonical)
          2) `import_id` in the prefixed candidate set (gro_*, hs_*, …)

        Does not use email — HubSpot record id is the stable key.
        """
        rid = str(hubspot_record_id).strip()
        if not rid:
            return None
        keys = reftech_import_id_candidates(rid)

        for iid in keys:
            try:
                out = self._get(
                    "/api/registration/attendee.json",
                    {"import_id": iid},
                )
            except HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                try:
                    parsed = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    if e.code >= 400:
                        continue
                    raise RuntimeError(f"HTTP {e.code}: {body[:500]}") from e
                if not parsed.get("status"):
                    continue
                raise RuntimeError(f"HTTP {e.code}: {body[:1000]}") from e

            data = out.get("data")
            if isinstance(data, dict) and data.get("id"):
                return data
            if out.get("status") is True and data:
                return data

        found = self._scan_attendees_for_hubspot_record(rid, keys)
        if found:
            return found
        return None

    def _pick_row_for_hubspot_duplicates(
        self,
        matches: list[dict[str, Any]],
        want: set[str],
        rid: str,
    ) -> dict[str, Any] | None:
        """
        When several RefTech rows map to the same HubSpot id (duplicate sync bug), pick one:
        prefer import_id in our candidate set, then explicit recordid == rid, else newest date.
        """
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0]
        for row in matches:
            if (row.get("import_id") or "") in want:
                return row
        for row in matches:
            for key in ("recordid", "record_id", "hubspot_record_id"):
                v = row.get(key)
                if v is not None and str(v).strip() == rid:
                    return row
        return max(
            matches,
            key=lambda r: (
                str(r.get("registration_date") or ""),
                str(r.get("id") or ""),
            ),
        )

    def _scan_attendees_for_hubspot_record(
        self,
        hubspot_record_id: str,
        import_candidates: list[str],
    ) -> dict[str, Any] | None:
        """
        Match by HubSpot `hs_object_id` only. First: RefTech row fields equal to the same id we
        POST as the registration `recordid` field (see run_sync_all build_fields). Then: import_id
        candidates. If the list API omits `recordid`, matching falls back to import_id only.
        """
        want = set(import_candidates)
        rid = str(hubspot_record_id).strip()
        rows = self.list_all_attendees()

        rec_matches: list[dict[str, Any]] = []
        for row in rows:
            for key in ("recordid", "record_id", "hubspot_record_id"):
                v = row.get(key)
                if v is not None and str(v).strip() == rid:
                    rec_matches.append(row)
                    break
        if len(rec_matches) == 1:
            return {"id": rec_matches[0].get("id")}
        if len(rec_matches) > 1:
            picked = self._pick_row_for_hubspot_duplicates(rec_matches, want, rid)
            if picked and picked.get("id"):
                return {"id": picked.get("id")}

        imp_matches = [
            r for r in rows if (r.get("import_id") or "") in want
        ]
        if len(imp_matches) == 1:
            return {"id": imp_matches[0].get("id")}
        if len(imp_matches) > 1:
            picked = self._pick_row_for_hubspot_duplicates(imp_matches, want, rid)
            if picked and picked.get("id"):
                return {"id": picked.get("id")}
        return None

    def _resolve_delegate_for_hubspot_import_collision(
        self,
        hubspot_record_id: str,
    ) -> str | None:
        """
        POST returned "Import ID already in use" — find the existing delegate id without
        trying a new import candidate (which would create a duplicate).

        RefTech may store `gro393…` while we POST `gro_393…`; GET by one string can miss the row,
        so we also scan the full attendee list once.
        """
        rid = str(hubspot_record_id).strip()
        if not rid:
            return None
        found = self.get_attendee_by_hubspot_record_id(rid)
        if found and found.get("id"):
            return str(found["id"])
        return None

    def list_all_attendees(self, *, status_type: str = "any") -> list[dict[str, Any]]:
        """
        Paginate GET /api/registration/attendees.json — used to verify sync / detect duplicates.
        """
        out: list[dict[str, Any]] = []
        page = 1
        while page <= 500:
            res = self._get(
                "/api/registration/attendees.json",
                {"status_type": status_type, "page": str(page)},
            )
            if not res.get("status"):
                break
            data = res.get("data") or {}
            rows = data.get("attendees") or []
            out.extend(rows)
            pag = data.get("pagination") or {}
            if not pag.get("page_next"):
                break
            page += 1
        return out

    def attendee_id_exists(self, delegate_id: str) -> bool:
        """True if a listed attendee has this RefTech `id` (short delegate id)."""
        d = (delegate_id or "").strip()
        if not d:
            return False
        page = 1
        while page <= 500:
            res = self._get(
                "/api/registration/attendees.json",
                {"status_type": "any", "page": str(page)},
            )
            if not res.get("status"):
                break
            data = res.get("data") or {}
            for row in data.get("attendees") or []:
                if (row.get("id") or "") == d:
                    return True
            pag = data.get("pagination") or {}
            if not pag.get("page_next"):
                break
            page += 1
        return False

    def count_attendees_with_hubspot_record_id(self, hubspot_record_id: str) -> int:
        """
        Count RefTech list rows tied to this HubSpot `hs_object_id`.

        Matches `recordid` / `record_id` / `hubspot_record_id` when the list API exposes them,
        **and** matches `import_id` against `reftech_import_id_candidates` (gro_*, gro2_*, …) so
        verification still works when recordid is omitted from attendees.json.
        """
        rid = str(hubspot_record_id).strip()
        if not rid:
            return 0
        want_import = set(reftech_import_id_candidates(rid))
        n = 0
        for row in self.list_all_attendees():
            if (row.get("import_id") or "") in want_import:
                n += 1
                continue
            for key in ("recordid", "record_id", "hubspot_record_id"):
                v = row.get(key)
                if v is not None and str(v).strip() == rid:
                    n += 1
                    break
        return n

    def submit_registration(
        self,
        *,
        attendee_type_id: str,
        fields: dict[str, Any],
        hubspot_record_id: str | None = None,
        reftech_attendee_id: str | None = None,
        status_id: str | None = None,
        status_name: str | None = None,
    ) -> str:
        """
        POST /api/registration/submission.json (creates or updates; response `data` = attendee id).

        - **Create:** `attendee_type_id` required (query `id`). Pass `hubspot_record_id` so we can
          set `attendee_import_id` and dedupe. Do not pass `reftech_attendee_id`.
        - **Update:** POST with **`attendee=<delegate id>`** + form fields. Omit query `id` by
          default (`REFTECH_UPDATE_INCLUDE_FORM_ID`); retry adds it on **Error loading form**.
          Do not send `attendee_import_id` on update (create semantics / "Import ID already in use").
        - **Optional:** `status_id` or `status_name` (or env `REFTECH_SUBMIT_STATUS_ID` /
          `REFTECH_SUBMIT_STATUS_NAME`) — RefTech may still overwrite if a workflow is attached.

        `fields` keys are `fields.json` API ids (values clipped in `build_fields`).

        Returns RefTech attendee id string from response `data`.
        """
        q: dict[str, Any] = {}
        rt = (reftech_attendee_id or "").strip()
        hs = (hubspot_record_id or "").strip()
        st_id = (status_id or os.environ.get("REFTECH_SUBMIT_STATUS_ID") or "").strip()
        st_name = (status_name or os.environ.get("REFTECH_SUBMIT_STATUS_NAME") or "").strip()

        def status_extra() -> list[tuple[str, str]]:
            # Docs: use status_id *or* status_name — prefer id when both are set.
            if st_id:
                return [("status_id", st_id[:50])]
            if st_name:
                return [("status_name", st_name[:50])]
            return []
        # `id` (attendee type): required for create, optional for update per docs.
        # `attendee`: query string param (per docs Parameters section) — NOT a POST body field.
        tid = (attendee_type_id or "").strip()
        if not rt and not tid:
            raise ValueError(
                "attendee_type_id is required when creating a registration "
                "(optional when updating via reftech_attendee_id)."
            )
        if tid:
            q["id"] = tid
        if rt:
            q["attendee"] = rt
        # Form field `recordid` (see fields.json for your attendee type) is **your** custom
        # "Record ID" text field — we store HubSpot `hs_object_id` there for reporting only.
        # It is **not** RefTech's attendee identifier. RefTech identifies the delegate for
        # updates via the **query string** `attendee=<short id>` (NOT the POST body).
        field_iter = fields.items()

        body: list[tuple[str, str]] = []

        for key, val in field_iter:
            if val is None:
                continue
            if isinstance(val, list):
                for item in val:
                    body.append((f"{key}[]", str(item)))
            else:
                body.append((key, str(val)))

        extra = status_extra()

        if rt:
            # Update: `attendee` is already in q (query string). POST body = form fields only.
            out = self._post_form_json(
                "/api/registration/submission.json", q, body + extra
            )
            if out.get("status") and out.get("data"):
                return str(out["data"])
            raise RuntimeError(
                f"submission.json update failed: {out.get('errors')!r}"
            )

        if hs:
            # Never create a second row if RefTech already maps this HubSpot hs_object_id.
            existing = self.get_attendee_by_hubspot_record_id(hs)
            if existing and existing.get("id"):
                return self.submit_registration(
                    attendee_type_id=attendee_type_id,
                    fields=fields,
                    hubspot_record_id=hs,
                    reftech_attendee_id=str(existing["id"]),
                    status_id=status_id,
                    status_name=status_name,
                )
            candidates = reftech_import_id_candidates(hs)
            last_err: Any = None
            for cand in candidates:
                body_try = list(body) + [("attendee_import_id", cand)] + extra
                out = self._post_form(
                    "/api/registration/submission.json", q, body_try
                )
                if out.get("status"):
                    data = out.get("data")
                    if data:
                        return str(data)
                err = out.get("errors") or []
                last_err = err
                if any("Import ID already in use" in str(x) for x in err):
                    delegate = self._resolve_delegate_for_hubspot_import_collision(hs)
                    if delegate:
                        return self.submit_registration(
                            attendee_type_id=attendee_type_id,
                            fields=fields,
                            hubspot_record_id=hs,
                            reftech_attendee_id=delegate,
                            status_id=status_id,
                            status_name=status_name,
                        )
                    # No row to update (empty list or no import_id match). After a DB wipe,
                    # RefTech may still report "already in use" for gro_* — ghost reservation.
                    # Try the next candidate (gro2_, …); we only skip duplicates when resolve
                    # finds an existing delegate above.
                    continue
                raise RuntimeError(f"submission.json failed: {err}")
            raise RuntimeError(f"submission.json failed: {last_err}")

        out = self._post_form("/api/registration/submission.json", q, body + extra)
        if not out.get("status"):
            err = out.get("errors")
            raise RuntimeError(f"submission.json failed: {err}")
        data = out.get("data")
        if not data:
            raise RuntimeError("submission.json returned no data id")
        return str(data)
