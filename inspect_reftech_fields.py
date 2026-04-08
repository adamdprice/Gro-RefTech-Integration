#!/usr/bin/env python3
"""
Load registration form definitions from EventReference (RefTech) so you can see
backend field IDs and (when exposed) dropdown option IDs — the admin UI often
does not show these.

Usage:
  export REFTECH_API_KEY=...
  python3 inspect_reftech_fields.py
  python3 inspect_reftech_fields.py --attendee-type-id b3C   # example: Paying Delegate

Requires: REFTECH_API_KEY in the environment or in a .env file next to this script.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


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


def api_get(base: str, path: str, params: dict) -> dict:
    q = urlencode({**params, "key": os.environ["REFTECH_API_KEY"]})
    url = f"{base}{path}?{q}"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def print_forms(base: str) -> list[dict]:
    data = api_get(base, "/api/registration/forms.json", {})
    if not data.get("status"):
        print("forms.json error:", data.get("errors"), file=sys.stderr)
        sys.exit(1)
    rows = data.get("data") or []
    print("Attendee types with forms (use id with --attendee-type-id):\n")
    for row in rows:
        print(f"  id={row.get('id')!r}  name={row.get('name')!r}")
    print()
    return rows


def normalize_attendee_type_id(raw: str) -> str:
    """Strip whitespace; allow accidental <b3C> paste from docs."""
    s = raw.strip()
    if len(s) >= 2 and s[0] == "<" and s[-1] == ">":
        s = s[1:-1].strip()
    return s


def looks_like_docs_placeholder(attendee_type_id: str) -> bool:
    t = attendee_type_id.lower()
    return "id_from_forms" in t or t in ("id", "your_id_here", "attendee_type_id")


def print_fields(base: str, attendee_type_id: str) -> None:
    data = api_get(
        base,
        "/api/registration/fields.json",
        {"id": attendee_type_id},
    )
    if not data.get("status"):
        err = data.get("errors")
        print("fields.json error:", err, file=sys.stderr)
        if err and any(
            "valid attendee type" in str(x).lower() for x in (err if isinstance(err, list) else [err])
        ):
            print(
                "\nUse a real id from the forms list (e.g. --attendee-type-id b3C), "
                "not the literal text <id_from_forms> from documentation.",
                file=sys.stderr,
            )
        sys.exit(1)
    fields = data.get("data") or []
    print(f"Fields for attendee_type id={attendee_type_id!r} ({len(fields)} total):\n")
    for f in sorted(fields, key=lambda x: (x.get("name") or "")):
        fid = f.get("id")
        fname = f.get("name")
        ftype = f.get("type")
        mand = f.get("mandatory")
        line = f"  API id: {fid!r:20}  label: {fname!r}  type: {ftype!r}  mandatory: {mand}"
        print(line)
        # Some installs return options/choices for select/radio; print full structure once if useful
        extra_keys = set(f.keys()) - {
            "id",
            "type",
            "name",
            "mandatory",
            "mandatoryDesc",
            "regexp",
        }
        if extra_keys:
            for k in sorted(extra_keys):
                print(f"         {k}: {json.dumps(f[k], ensure_ascii=False)[:500]}")
    print()
    print("Tip: POST keys for /api/registration/submission.json match the 'API id' column above.")
    print("If dropdown option IDs are not listed here, fetch a real attendee with")
    print("GET /api/registration/attendee.json?id=... and inspect data.fields[].value for select/radio.")


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    ap = argparse.ArgumentParser(description="List RefTech registration field API IDs")
    ap.add_argument(
        "--attendee-type-id",
        metavar="ID",
        help="Attendee type id from forms.json, e.g. b3C (Paying Delegate). Not a placeholder.",
    )
    ap.add_argument(
        "--list-forms-only",
        action="store_true",
        help="Only call forms.json and exit",
    )
    args = ap.parse_args()

    if not os.environ.get("REFTECH_API_KEY"):
        print("Set REFTECH_API_KEY in the environment or .env", file=sys.stderr)
        sys.exit(1)

    base = os.environ.get("REFTECH_API_BASE_URL", "https://eventreference.com").rstrip(
        "/"
    )

    try:
        if args.list_forms_only:
            print_forms(base)
            return
        if not args.attendee_type_id:
            print_forms(base)
            tid = os.environ.get("REFTECH_DEFAULT_ATTENDEE_TYPE_ID")
            if not tid:
                print(
                    "Pass --attendee-type-id with a real id from the list above "
                    "(example: --attendee-type-id b3C), or set REFTECH_DEFAULT_ATTENDEE_TYPE_ID in .env",
                    file=sys.stderr,
                )
                sys.exit(1)
            args.attendee_type_id = tid
        tid = normalize_attendee_type_id(args.attendee_type_id)
        if looks_like_docs_placeholder(tid):
            print(
                "That does not look like a real attendee type id. "
                "Run without --attendee-type-id to list ids, then e.g. "
                "--attendee-type-id b3C",
                file=sys.stderr,
            )
            sys.exit(1)
        print_fields(base, tid)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        print(f"HTTP {e.code}: {e.reason}\n{body[:2000]}", file=sys.stderr)
        sys.exit(1)
    except URLError as e:
        print(f"Request failed: {e.reason}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
