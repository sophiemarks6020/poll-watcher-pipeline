"""
pw_ingest_assignments.py

Stage 1 of the poll watcher credentialing pipeline.

Reads LBJ CSV assignment exports and MERGEs the result into
commons.pw_credential_status.

Design notes:
- Recompute-fresh model: every run reads the FULL current set of CSVs, not
  just new ones. Reassignments self-correct automatically.
- SHAPE DECISION (2026-09-03, pending VoPro confirmation in tomorrow's
  meeting): one row per (van_id, county), NOT one row per volunteer.
  This matches a teammate's existing credential_letters_script.ipynb, which
  already generates a SEPARATE letter per county for multi-county
  volunteers (it groups by county before grouping by VAN ID, so a
  multi-county volunteer's letter for County A only lists County A's
  assignments). If VoPro wants ONE combined letter per volunteer instead,
  this table's shape -- and a teammate's letter script -- both need to
  change together.
- needs_confirmation only flips TRUE when a (van_id, county) row is new,
  was removed and came back, has a different EMAIL or multi-county status,
  or (ADDED 2026-10-06) its SHIFTS changed -- detected by comparing the old
  pw_credential_shifts to the new rows before the table is overwritten.
  CHANGED 2026-10-06: a changed name, phone, or address alone no longer
  triggers an email (the assignment email contains none of them); the row is
  refreshed quietly. All text is whitespace-collapsed on read so the LBJ
  export's double spaces don't look like changes. Every run prints a change
  report (new / email changed / shifts changed / quiet updates / removed /
  unchanged) and refuses to run if the new list is under 80% the size of the
  currently active list (partial-file guard; allow_shrink=true overrides).
  Rows with a blank VAN ID are skipped and listed.
- HUMAN REVIEW GATE: needs_human_review tracks is_multi_county directly.
  A multi-county volunteer's assignments are exactly the case most likely
  to have something off (wrong county in the LBJ export, a VAN ID merge
  issue, a duplicate assignment) so Stage 2 (assignment email) should
  hold anyone with needs_human_review = TRUE out of its send query until
  a person has checked them. Deliberately a boolean on the same row
  rather than a separate table -- it rides the same MERGE change-detection
  logic for free (if their multi-county status changes again after
  being cleared, it flips back to TRUE automatically, same as any other
  tracked field). NOTE: a multi-county volunteer has multiple rows (one
  per county) -- clearing review for one of their rows does NOT clear
  the others. Whoever does review should clear by van_id, not by single
  row, e.g.:
      UPDATE `commons.pw_credential_status`
      SET needs_human_review = FALSE
      WHERE van_id = '12345678'
- Removal handling: a volunteer's CSV row can just disappear (reassigned
  elsewhere, pulled from a county). Since MERGE only sees what's in
  staging, a WHEN NOT MATCHED BY SOURCE branch flags stale rows with
  is_removed_from_county = TRUE rather than silently leaving them stale
  or deleting them outright. Someone should periodically check that flag.
- SHIFT-LEVEL DETAIL, SEPARATE TABLE (added 2026-09-03): pw_credential_status
  only ever stored county-level rollup (is_multi_county, county name) --
  the individual shift rows (date, time, location) from the raw CSVs were
  being thrown away, which is fine for Stage 4/5 (letters/CSVs only need
  county-level info) but Stage 2 (assignment email) needs real shift
  detail to build email content and .ics attachments. Rather than
  widening pw_credential_status to shift-grain (which would duplicate
  needs_confirmation/needs_human_review across every shift row for the
  same volunteer-county pairing, risking drift) or having Stage 2
  re-read the raw CSVs itself (decoupling what's sent from what was
  actually ingested/confirmed), shift detail goes in its own table,
  pw_credential_shifts, one row per (van_id, county, event_date,
  location). Written as a plain full replace each run, not a MERGE --
  unlike pw_credential_status, this table carries no per-row state
  (no confirmation flags, nothing to preserve across runs), so a
  straight WRITE_TRUNCATE is simplest and correct. Stage 2 should query
  this table (grouped by van_id, across all of a person's counties, per
  the "one combined email" decision) for content, and still check
  pw_credential_status separately for needs_confirmation /
  needs_human_review gating.
- KNOWN DOWNSTREAM ISSUE: because a multi-county volunteer now has
  multiple rows in pw_credential_status, Stage 2's "query everyone with
  needs_confirmation=TRUE" will need to dedupe by van_id before sending
  (per the "one combined email" decision above), or a multi-county
  volunteer gets duplicate first emails. Not solved here -- flagging so
  it isn't missed when Stage 2 gets built.

OPEN, NOT YET CONFIRMED: where these assignment rows ultimately come from
for real production runs. CHANGED 2026-09-29: intake switched from local
CSVs (read out of csv_assignments_input/, run manually in PyCharm) to a
Google Sheet, so this stage can run from the portal job like every other
stage instead of needing a local machine. The sheet and tab
are job parameters with NO default (assignments_spreadsheet_id,
assignments_tab_gid) -- set them to the real intake sheet once VoPro
confirms it. Target tables default to the real ones; test runs pass
status_table=pw_credential_status_test, shifts_table=pw_credential_shifts_test.
"""

import json
import os
import urllib.parse

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# CHANGED 2026-10-02: no default for the source sheet/tab. These must be set
# as job parameters (assignments_spreadsheet_id, assignments_tab_gid) so a
# forgotten parameter stops the run instead of silently loading the TEST
# sheet into the tables. main() enforces this.
ASSIGNMENTS_SPREADSHEET_ID = os.getenv("assignments_spreadsheet_id", "").strip()
ASSIGNMENTS_TAB_GID = os.getenv("assignments_tab_gid", "").strip()

PROJECT_ID = "your-gcp-project"
DATASET = "commons"
# CHANGED 2026-10-02: table names are job parameters (status_table,
# shifts_table) and default to the REAL tables. For test runs, set
# status_table=pw_credential_status_test and
# shifts_table=pw_credential_shifts_test. A "commons." prefix is tolerated
# (Stage 2 takes dataset-qualified names) and stripped.
TABLE = os.getenv("status_table", "pw_credential_status").split(".")[-1]
SHIFTS_TABLE = os.getenv("shifts_table", "pw_credential_shifts").split(".")[-1]

# Column names as they appear in a teammate's LBJ export CSVs -- confirm these
# match the real production export before trusting this against real data.
COL_VAN_ID = "VAN ID"
COL_FIRST = "First Name"
COL_LAST = "Last Name"
COL_EMAIL = "Email"          # not seen in a teammate's script directly -- verify this column name exists
COL_PHONE = "Phone Number"   # confirmed against the real VP export header (2026-09-19)
COL_ADDRESS = "Address"
COL_COUNTY = "Assignment County"
COL_DATE = "Date"
COL_LOCATION = "Polling Location"
# Seen directly in a teammate's credential_letters_script.ipynb (START, END,
# LOCADDR constants there) -- these two are more trustworthy than
# COL_EMAIL/COL_PHONE above, but still worth a final check against a real
# production export.
COL_START_TIME = "Start Time"
COL_END_TIME = "End Time"
COL_LOCATION_ADDRESS = "Polling Location Address"


# ---------------------------------------------------------------------------
# Google Sheets auth (same pattern as the other pw_pipeline stages)
# ---------------------------------------------------------------------------

def build_service_account_info() -> dict:
    return json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))


# CHANGED 2026-10-06: built lazily (was module-level) so the change-detection
# functions below can be imported and unit-tested with no credentials set.
_sheets_service = None


def get_sheets_service():
    global _sheets_service
    if _sheets_service is None:
        creds = service_account.Credentials.from_service_account_info(
            build_service_account_info(),
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        _sheets_service = build("sheets", "v4", credentials=creds)
    return _sheets_service


# ---------------------------------------------------------------------------
# Read + consolidate
# ---------------------------------------------------------------------------

def resolve_sheet_title_by_gid(spreadsheet_id: str, gid: str) -> str:
    """A1-notation ranges need the tab's TITLE, not its gid -- the URL only
    gives us the gid, so look up the matching tab by sheetId once per run."""
    meta = get_sheets_service().spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if str(props.get("sheetId")) == str(gid):
            return props["title"]
    raise ValueError(f"No tab with gid={gid} found in spreadsheet {spreadsheet_id}")


def read_assignments_sheet(spreadsheet_id: str, gid: str) -> pd.DataFrame:
    """
    Read the assignments tab (identified by gid) into one DataFrame. CHANGED
    2026-09-29 -- replaces the old read_all_csvs() local-folder reader; see
    module docstring. Same downstream contract as before (dtype=str-like,
    stripped, deduped on van_id/date/location) so nothing else in this file
    needed to change.
    """
    tab_title = resolve_sheet_title_by_gid(spreadsheet_id, gid)
    # Quote the tab title: names with hyphens/digits/spaces (e.g. "GA-assignments
    # - MASTER 10-13 to 10-30") aren't reliably parsed as an unquoted A1 range.
    quoted_title = "'" + tab_title.replace("'", "''") + "'"
    resp = get_sheets_service().spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{quoted_title}!A:Z",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        raise ValueError(f"No data found in tab '{tab_title}' of spreadsheet {spreadsheet_id}")

    header = [c.strip() for c in rows[0]]
    body = [row + [""] * (len(header) - len(row)) for row in rows[1:]]
    data = pd.DataFrame(body, columns=header)

    data = clean_assignment_rows(data)

    print(f"Read {len(data)} assignment rows from sheet tab '{tab_title}', "
          f"{data[COL_VAN_ID].nunique()} people")

    return data


def clean_assignment_rows(data: pd.DataFrame) -> pd.DataFrame:
    """
    Strip whitespace, drop fully empty rows, drop (and loudly list) rows with
    a blank VAN ID, and dedupe on van_id/date/location.

    ADDED 2026-10-06: blank-VAN-ID rows used to flow through as van_id=''
    (two such volunteers would have collapsed into one fake person). They are
    now skipped and listed so someone can fix the source sheet.
    """
    data = data.copy()
    # CHANGED 2026-10-06: also collapse runs of whitespace to one space. The
    # LBJ export has "720 Fond Ln  Atlanta" (two spaces) where the stored data
    # has one, which flagged ~everyone as changed. Done here, before anything
    # is compared or the confirmation link is built, so both sides match.
    for c in data.columns:
        data[c] = data[c].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    data = data[~(data == "").all(axis=1)]

    blank_id = data[COL_VAN_ID] == ""
    if blank_id.any():
        print(f"WARNING: skipping {int(blank_id.sum())} row(s) with a blank {COL_VAN_ID} "
              f"(fix these in the source sheet):")
        who = data[blank_id].drop_duplicates(subset=[COL_FIRST, COL_LAST, COL_EMAIL])
        for _, r in who.iterrows():
            print(f"  - {r.get(COL_FIRST, '')} {r.get(COL_LAST, '')} <{r.get(COL_EMAIL, '')}>")
        data = data[~blank_id]

    return data.drop_duplicates(subset=[COL_VAN_ID, COL_DATE, COL_LOCATION])


# ---------------------------------------------------------------------------
# Pre-filled confirmation form link
# ---------------------------------------------------------------------------

FORM_BASE_URL = "https://docs.google.com/forms/d/e/YOUR_FORM_ID/viewform"
FORM_ENTRY_IDS = {
    "name": "entry.1606577515",
    "email": "entry.1628512254",
    "phone": "entry.271969659",
    "address": "entry.78801999",
}


def build_confirmation_link(first_name: str, last_name: str, email: str, phone: str, address: str) -> str:
    """
    Pre-filled Google Form link for one volunteer. Built once per person
    (not per county) -- it's the same confirmation regardless of how many
    counties they're assigned to.
    """
    full_name = f"{first_name} {last_name}".strip()
    params = {
        "usp": "pp_url",
        FORM_ENTRY_IDS["name"]: full_name,
        FORM_ENTRY_IDS["email"]: email,
        FORM_ENTRY_IDS["phone"]: phone,
        FORM_ENTRY_IDS["address"]: address,
    }
    return f"{FORM_BASE_URL}?{urllib.parse.urlencode(params)}"


def build_volunteer_county_rows(data: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (van_id, county). Per-person fields (name, email, phone,
    address) are taken from that volunteer's first row overall -- assumed
    stable across their rows, not yet verified against real data.

    is_multi_county is computed from the volunteer's FULL county set (not
    just the current row's county), so it's TRUE on every one of that
    volunteer's rows if they're assigned anywhere else too.
    """
    person_info = {}
    for van_id, g in data.groupby(COL_VAN_ID):
        first = g.iloc[0]
        counties = sorted(set(g[COL_COUNTY].tolist()))
        first_name = first.get(COL_FIRST, "")
        last_name = first.get(COL_LAST, "")
        email = first.get(COL_EMAIL, "")
        phone = first.get(COL_PHONE, "")
        address = first.get(COL_ADDRESS, "")
        person_info[van_id] = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "address": address,
            "counties": counties,
            "is_multi_county": len(counties) > 1,
            "confirmation_link": build_confirmation_link(first_name, last_name, email, phone, address),
        }

    rows = []
    for van_id, info in person_info.items():
        for county in info["counties"]:
            rows.append({
                "van_id": van_id,
                "county": county,
                "first_name": info["first_name"],
                "last_name": info["last_name"],
                "email": info["email"],
                "phone": info["phone"],
                "address": info["address"],
                "is_multi_county": info["is_multi_county"],
                "confirmation_link": info["confirmation_link"],
            })

    grouped = pd.DataFrame(rows)
    n_people = len(person_info)
    n_multi = sum(1 for i in person_info.values() if i["is_multi_county"])
    print(f"{n_people} volunteers, {len(grouped)} (volunteer, county) rows, "
          f"{n_multi} volunteers flagged multi-county (needs_human_review)")

    return grouped


# ---------------------------------------------------------------------------
# Shift-level detail (for Stage 2 -- email content + .ics attachments)
# ---------------------------------------------------------------------------

def build_shift_rows(data: pd.DataFrame) -> pd.DataFrame:
    """
    One row per raw assignment row (van_id, county, date, location) --
    unlike build_volunteer_county_rows, nothing gets collapsed here.
    This is the detail pw_credential_status intentionally doesn't keep.
    """
    shift_rows = pd.DataFrame({
        "van_id": data[COL_VAN_ID],
        "county": data[COL_COUNTY],
        "event_date": data[COL_DATE],
        "start_time": data[COL_START_TIME],
        "end_time": data[COL_END_TIME],
        "location": data[COL_LOCATION],
        "location_address": data[COL_LOCATION_ADDRESS],
    })
    print(f"{len(shift_rows)} shift-level rows built for {SHIFTS_TABLE}")
    return shift_rows


def write_shifts_table(shift_rows: pd.DataFrame, client: bigquery.Client, dry_run: bool = False):
    """
    Plain full replace, not a MERGE -- this table has no per-row state to
    preserve across runs (no confirmation flags, no review gate), so
    recompute-fresh is simplest and correct here. Stage 2 should query it
    grouped by van_id for email content/.ics building, alongside a
    separate check of pw_credential_status for needs_confirmation /
    needs_human_review gating.
    """
    target_table = f"{PROJECT_ID}.{DATASET}.{SHIFTS_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("van_id", "STRING"),
            bigquery.SchemaField("county", "STRING"),
            bigquery.SchemaField("event_date", "STRING"),
            bigquery.SchemaField("start_time", "STRING"),
            bigquery.SchemaField("end_time", "STRING"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("location_address", "STRING"),
        ],
    )

    if dry_run:
        print(f"[DRY RUN] Would WRITE_TRUNCATE {len(shift_rows)} rows into {target_table}")
        return

    load_job = client.load_table_from_dataframe(shift_rows, target_table, job_config=job_config)
    load_job.result()

    print(f"Wrote {len(shift_rows)} rows to {SHIFTS_TABLE} (full replace)")


# ---------------------------------------------------------------------------
# Change detection (ADDED 2026-10-06)
#
# The MERGE only compares person-level fields, so a volunteer whose SHIFTS
# changed (but not their name/email/etc.) was never flagged for a new
# assignment email. Fix: compare each (van_id, county)'s old shifts (read from
# pw_credential_shifts BEFORE it is overwritten) to the new ones, using the
# SAME function on both sides so the fingerprints can't mismatch, and stage
# the result as shifts_changed for the MERGE to use.
#
# Everything in this section is pure (no BigQuery / Sheets calls) so it can be
# unit-tested on its own -- see test_pw_ingest_changes.py.
# ---------------------------------------------------------------------------

SHIFT_SIG_COLS = ["event_date", "start_time", "end_time", "location", "location_address"]
# CHANGED 2026-10-06: not every change to a person warrants a new assignment
# email. The assignment email contains shifts, not phone/address, so:
#   - TRIGGER fields (new email, county/multi-county status, new shifts, new
#     person, re-added person) set needs_confirmation=TRUE.
#   - QUIET fields (name, phone, address, confirmation link) are refreshed on
#     the row but do NOT trigger an email. (The MERGE below has two matched
#     clauses to match this -- keep the two in sync.)
TRIGGER_INFO_COLS = ["email", "is_multi_county"]
QUIET_INFO_COLS = ["first_name", "last_name", "phone", "address", "confirmation_link"]
MIN_LIST_RATIO = 0.8   # new list must have at least this share of currently active people
ALLOW_SHRINK = (os.getenv("allow_shrink") or os.getenv("ALLOW_SHRINK") or "false").lower() == "true"


def _norm(v) -> str:
    """None / NaN / NA -> '', everything else str with whitespace collapsed
    (incl. embedded newlines). Used for every comparison so BigQuery NULLs and
    sheet blanks compare equal, and so old stored values (which may still hold
    line breaks / double spaces) compare equal to the whitespace-cleaned new
    ones."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    return " ".join(str(v).split())


def _is_true(v) -> bool:
    return v is True or _norm(v).lower() == "true"


def shifts_signature_map(shift_rows) -> dict:
    """(van_id, county) -> fingerprint of that pairing's shifts. Order of the
    rows doesn't matter; any change to date/time/location/address does."""
    sigs = {}
    if shift_rows is None or len(shift_rows) == 0:
        return sigs
    for (van_id, county), g in shift_rows.groupby(["van_id", "county"]):
        lines = sorted(
            "|".join(_norm(v) for v in row)
            for row in g[SHIFT_SIG_COLS].itertuples(index=False, name=None)
        )
        sigs[(_norm(van_id), _norm(county))] = "\n".join(lines)
    return sigs


def compute_shifts_changed(grouped, old_sigs: dict, new_sigs: dict, existing_keys: set) -> list:
    """One bool per row of `grouped`. A pairing with old shifts on file is
    changed iff its fingerprint differs. A pairing that exists in the status
    table but has NO old shifts on file can't be compared, so it is treated as
    changed (conservative). A brand-new pairing is False (the MERGE inserts it
    flagged anyway)."""
    out = []
    for van_id, county in zip(grouped["van_id"], grouped["county"]):
        key = (_norm(van_id), _norm(county))
        if key in old_sigs:
            out.append(old_sigs[key] != new_sigs.get(key, ""))
        else:
            out.append(key in existing_keys)
    return out


def _shift_tuples(shift_rows) -> dict:
    out = {}
    if shift_rows is None or len(shift_rows) == 0:
        return out
    for (van_id, county), g in shift_rows.groupby(["van_id", "county"]):
        out[(_norm(van_id), _norm(county))] = [
            tuple(_norm(v) for v in row)
            for row in g[SHIFT_SIG_COLS].itertuples(index=False, name=None)
        ]
    return out


def _fmt_shift(t) -> str:
    date, start, end, loc, addr = t
    return f"{date} {start}-{end} @ {loc} ({addr})"


def describe_shift_diffs(old_rows, new_rows, keys) -> dict:
    """(van_id, county) -> short text of what differs between the old and new
    shifts, e.g. '1 removed, 1 added | - 2026-10-21 7:00 AM-7:00 PM @ ... |
    + 2026-10-21 7:00 AM-1:00 PM @ ...'. Report-only; never affects the MERGE."""
    from collections import Counter
    old, new = _shift_tuples(old_rows), _shift_tuples(new_rows)
    out = {}
    for key in keys:
        o, n = Counter(old.get(key, [])), Counter(new.get(key, []))
        removed, added = sorted((o - n).elements()), sorted((n - o).elements())
        parts = [f"{len(removed)} removed, {len(added)} added"]
        parts += [f"- {_fmt_shift(t)}" for t in removed[:2]]
        parts += [f"+ {_fmt_shift(t)}" for t in added[:2]]
        out[key] = " | ".join(parts)
    return out


def classify_changes(grouped, existing_status, shift_detail=None) -> dict:
    """
    Mirror of what the MERGE will do, for the dry-run report. `grouped` must
    already carry shifts_changed. Precedence: new > reactivated >
    info_changed (a TRIGGER field changed: email / multi-county) >
    shifts_changed > quiet_update (only name/phone/address changed: row is
    refreshed, NO email) > unchanged. 'removed' = active existing pairings
    missing from the new list. 'shifts_any' lists every existing pairing whose
    shifts changed, whatever bucket it landed in, so shift changes are never
    hidden behind another flag.
    """
    existing = {}
    if existing_status is not None and len(existing_status) > 0:
        for r in existing_status.to_dict("records"):
            existing[(_norm(r.get("van_id")), _norm(r.get("county")))] = r

    result = {k: [] for k in ("new", "reactivated", "info_changed", "shifts_changed",
                              "quiet_update", "unchanged", "removed", "shifts_any")}
    seen = set()
    for r in grouped.to_dict("records"):
        key = (_norm(r.get("van_id")), _norm(r.get("county")))
        seen.add(key)
        entry = {"van_id": key[0], "county": key[1],
                 "name": f"{_norm(r.get('first_name'))} {_norm(r.get('last_name'))}".strip()}
        if shift_detail and key in shift_detail:
            entry["shift_diff"] = shift_detail[key]
        cur = existing.get(key)
        shifts_changed = _is_true(r.get("shifts_changed"))
        if cur is None:
            result["new"].append(entry)
            continue
        if shifts_changed:
            result["shifts_any"].append(entry)
        if _is_true(cur.get("is_removed_from_county")):
            result["reactivated"].append(entry)
            continue
        trigger = [c for c in TRIGGER_INFO_COLS if _norm(cur.get(c)) != _norm(r.get(c))]
        quiet = [c for c in QUIET_INFO_COLS if _norm(cur.get(c)) != _norm(r.get(c))]
        if trigger:
            result["info_changed"].append(
                {**entry, "fields": trigger, "shifts_also": shifts_changed})
        elif shifts_changed:
            result["shifts_changed"].append({**entry, "fields": quiet} if quiet else entry)
        elif quiet:
            result["quiet_update"].append({**entry, "fields": quiet})
        else:
            result["unchanged"].append(entry)

    for key, cur in existing.items():
        if key not in seen and not _is_true(cur.get("is_removed_from_county")):
            result["removed"].append({
                "van_id": key[0], "county": key[1],
                "name": f"{_norm(cur.get('first_name'))} {_norm(cur.get('last_name'))}".strip(),
            })
    return result


def count_flagged(result: dict) -> int:
    """People/pairings that will end up needs_confirmation=TRUE after the MERGE."""
    return sum(len(result[k]) for k in ("new", "reactivated", "info_changed", "shifts_changed"))


def list_shrunk_too_much(new_people: int, existing_active_people: int,
                         min_ratio: float = MIN_LIST_RATIO) -> bool:
    """True if the new list is suspiciously small vs. who is currently active
    (e.g. a partial/delta file run as a full list)."""
    if existing_active_people <= 0:
        return False
    return new_people < min_ratio * existing_active_people


def print_change_report(result: dict, max_lines: int = 400):
    print("=" * 60)
    print("CHANGE REPORT (what the MERGE will do)")
    for label, key in (("New", "new"), ("Re-added after removal", "reactivated"),
                       ("Email / multi-county changed", "info_changed"),
                       ("Shifts changed (no email change)", "shifts_changed")):
        print(f"  {label}: {len(result[key])}")
    print(f"  --> will be flagged for an assignment email: {count_flagged(result)}")
    print(f"  Shifts changed, all categories combined (existing people): {len(result['shifts_any'])}")
    print(f"  Quiet updates (name/phone/address only, NO email): {len(result['quiet_update'])}")
    print(f"  Unchanged (no email): {len(result['unchanged'])}")
    print(f"  Removed (missing from list): {len(result['removed'])}")
    for label, key, cap in (("NEW", "new", max_lines), ("RE-ADDED", "reactivated", max_lines),
                            ("EMAIL / MULTI-COUNTY CHANGED", "info_changed", max_lines),
                            ("SHIFTS CHANGED", "shifts_changed", max_lines),
                            ("REMOVED", "removed", max_lines),
                            ("QUIET UPDATES (NO EMAIL)", "quiet_update", 10)):
        rows = result[key]
        if not rows:
            continue
        print(f"--- {label} ({len(rows)}) ---")
        for e in rows[:cap]:
            extra = f"  fields={e['fields']}" if e.get("fields") else ""
            if e.get("shifts_also"):
                extra += "  (shifts also changed)"
            print(f"  {e['van_id']}  {e['name']}  [{e['county']}]{extra}")
            if e.get("shift_diff"):
                print(f"      shifts: {e['shift_diff']}")
        if len(rows) > cap:
            print(f"  ...and {len(rows) - cap} more")
    print("=" * 60)


def read_existing_status(client) -> pd.DataFrame:
    cols = ["van_id", "county", "first_name", "last_name", "email", "phone", "address",
            "is_multi_county", "confirmation_link", "is_removed_from_county"]
    sql = f"SELECT {', '.join(cols)} FROM `{PROJECT_ID}.{DATASET}.{TABLE}`"
    try:
        return client.query(sql).result().to_dataframe(create_bqstorage_client=False)
    except NotFound:
        return pd.DataFrame(columns=cols)


def read_existing_shifts(client) -> pd.DataFrame:
    cols = ["van_id", "county"] + SHIFT_SIG_COLS
    sql = f"SELECT {', '.join(cols)} FROM `{PROJECT_ID}.{DATASET}.{SHIFTS_TABLE}`"
    try:
        return client.query(sql).result().to_dataframe(create_bqstorage_client=False)
    except NotFound:
        return pd.DataFrame(columns=cols)


# ---------------------------------------------------------------------------
# BigQuery MERGE
# ---------------------------------------------------------------------------

def merge_into_state_table(grouped: pd.DataFrame, client: bigquery.Client, dry_run: bool = False):
    """
    Load the grouped data into a temp staging table, then MERGE into
    pw_credential_status. Key is (van_id, county).

    - MATCHED + something changed -> update, flip needs_confirmation TRUE,
      needs_human_review tracks S.is_multi_county
    - NOT MATCHED (new pairing)   -> insert, needs_confirmation TRUE,
      needs_human_review = S.is_multi_county
    - NOT MATCHED BY SOURCE       -> stale row (volunteer removed from
      that county) -> flag is_removed_from_county TRUE. CHANGED
      2026-09-30: also flips needs_confirmation FALSE (was previously left
      untouched, on the reasoning that "removal isn't something the
      volunteer confirms"). Found via a real test case: a multi-county
      volunteer dropped to one county, and Stage 2's dedup query (one row
      per van_id, "ORDER BY county ASC" tiebreak, no is_removed_from_county
      filter) has no protection against picking the now-stale removed row
      over the correct remaining one -- it only worked out by luck for that
      case (the stale county name happened to sort after the real one). A
      removed row must never be eligible for needs_confirmation=TRUE, not
      just flagged, since dedup order is arbitrary from that query's
      perspective.
    """
    # Staging table name follows TABLE, so a test run (status_table=..._test)
    # uses pw_credential_status_test_staging and never touches the real
    # staging table, even transiently.
    staging_table = f"{PROJECT_ID}.{DATASET}.{TABLE}_staging"
    target_table = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("van_id", "STRING"),
            bigquery.SchemaField("county", "STRING"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("is_multi_county", "BOOL"),
            bigquery.SchemaField("confirmation_link", "STRING"),
            # ADDED 2026-10-06: computed in main() by comparing old vs new shifts
            bigquery.SchemaField("shifts_changed", "BOOL"),
        ],
    )

    merge_sql = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.van_id = S.van_id AND T.county = S.county
    WHEN MATCHED AND (
        T.email IS DISTINCT FROM S.email OR
        T.is_multi_county IS DISTINCT FROM S.is_multi_county OR
        COALESCE(S.shifts_changed, FALSE) OR
        T.is_removed_from_county = TRUE
    ) THEN
      UPDATE SET
        first_name = S.first_name,
        last_name = S.last_name,
        email = S.email,
        phone = S.phone,
        address = S.address,
        is_multi_county = S.is_multi_county,
        confirmation_link = S.confirmation_link,
        needs_confirmation = TRUE,
        needs_human_review = S.is_multi_county,
        is_removed_from_county = FALSE,
        last_ingested_at = CURRENT_TIMESTAMP()
    WHEN MATCHED AND (
        T.first_name IS DISTINCT FROM S.first_name OR
        T.last_name IS DISTINCT FROM S.last_name OR
        T.phone IS DISTINCT FROM S.phone OR
        T.address IS DISTINCT FROM S.address OR
        T.confirmation_link IS DISTINCT FROM S.confirmation_link
    ) THEN
      UPDATE SET
        first_name = S.first_name,
        last_name = S.last_name,
        phone = S.phone,
        address = S.address,
        confirmation_link = S.confirmation_link,
        last_ingested_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (
        van_id, county, first_name, last_name, email, phone, address,
        is_multi_county, confirmation_link, needs_confirmation,
        needs_human_review, is_removed_from_county,
        needs_credential, needs_county_notify, last_ingested_at
      )
      VALUES (
        S.van_id, S.county, S.first_name, S.last_name, S.email, S.phone, S.address,
        S.is_multi_county, S.confirmation_link, TRUE,
        S.is_multi_county, FALSE,
        FALSE, FALSE, CURRENT_TIMESTAMP()
      )
    WHEN NOT MATCHED BY SOURCE AND T.is_removed_from_county = FALSE THEN
      UPDATE SET
        is_removed_from_county = TRUE,
        needs_confirmation = FALSE,
        last_ingested_at = CURRENT_TIMESTAMP()
    """

    if dry_run:
        print("[DRY RUN] Would load staging table and run MERGE:")
        print(merge_sql)
        print(f"[DRY RUN] {len(grouped)} rows would be staged")
        return

    load_job = client.load_table_from_dataframe(grouped, staging_table, job_config=job_config)
    load_job.result()

    # ADDED (2026-09-12) -- MERGE requires the target table to already
    # exist; unlike a plain load job it can't create one. This is an
    # idempotent no-op once the table exists (exists_ok=True), so it's
    # safe to leave in permanently, not just for testing -- it also
    # covers the very first-ever run against the real table someday.
    client.create_table(
        bigquery.Table(target_table, schema=[
            bigquery.SchemaField("van_id", "STRING"),
            bigquery.SchemaField("county", "STRING"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("phone", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("is_multi_county", "BOOL"),
            bigquery.SchemaField("confirmation_link", "STRING"),
            bigquery.SchemaField("needs_confirmation", "BOOL"),
            bigquery.SchemaField("needs_human_review", "BOOL"),
            bigquery.SchemaField("is_removed_from_county", "BOOL"),
            bigquery.SchemaField("needs_credential", "BOOL"),
            bigquery.SchemaField("needs_county_notify", "BOOL"),
            bigquery.SchemaField("last_ingested_at", "TIMESTAMP"),
            # ADDED (2026-09-12) -- pw_assignment_emails.py's
            # flip_confirmation_sent() writes this column on successful
            # send; Stage 1's own MERGE never touches it, but the target
            # table needs it to exist. Matches the module docstring's own
            # already-documented open item for the real table too.
            bigquery.SchemaField("confirmation_sent_at", "TIMESTAMP"),
        ]),
        exists_ok=True,
    )

    query_job = client.query(merge_sql)
    query_job.result()

    print(f"MERGE complete: {query_job.num_dml_affected_rows} rows affected")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False):
    if not ASSIGNMENTS_SPREADSHEET_ID or not ASSIGNMENTS_TAB_GID:
        raise SystemExit(
            "Set job parameters assignments_spreadsheet_id and assignments_tab_gid "
            "(no defaults, so the wrong sheet can't be read by accident)."
        )
    print(f"Ingesting sheet {ASSIGNMENTS_SPREADSHEET_ID} (gid {ASSIGNMENTS_TAB_GID}) "
          f"into {PROJECT_ID}.{DATASET}.{TABLE} and {SHIFTS_TABLE}"
          f"{' [DRY RUN]' if dry_run else ''}")
    data = read_assignments_sheet(ASSIGNMENTS_SPREADSHEET_ID, ASSIGNMENTS_TAB_GID)
    grouped = build_volunteer_county_rows(data)
    shift_rows = build_shift_rows(data)

    # CHANGED 2026-10-06: the client is created for dry runs too, but is only
    # used for READS there (current status + shifts). Nothing is written.
    client = bigquery.Client(project=PROJECT_ID)
    existing_status = read_existing_status(client)
    existing_shifts = read_existing_shifts(client)

    old_sigs = shifts_signature_map(existing_shifts)
    new_sigs = shifts_signature_map(shift_rows)
    existing_keys = {(_norm(v), _norm(c))
                     for v, c in zip(existing_status["van_id"], existing_status["county"])}
    grouped["shifts_changed"] = compute_shifts_changed(grouped, old_sigs, new_sigs, existing_keys)

    changed_keys = {(_norm(v), _norm(c))
                    for v, c, f in zip(grouped["van_id"], grouped["county"], grouped["shifts_changed"]) if f}
    shift_detail = describe_shift_diffs(existing_shifts, shift_rows, changed_keys)
    result = classify_changes(grouped, existing_status, shift_detail)
    print_change_report(result)

    # Safety guards. In a dry run they are reported; in a real run they stop it.
    problems = []
    active = existing_status[~existing_status["is_removed_from_county"].map(_is_true)] \
        if len(existing_status) else existing_status
    active_people = active["van_id"].nunique() if len(active) else 0
    new_people = grouped["van_id"].nunique()
    if list_shrunk_too_much(new_people, active_people) and not ALLOW_SHRINK:
        problems.append(
            f"New list has {new_people} people but {active_people} are currently active "
            f"(under {int(MIN_LIST_RATIO * 100)}%). Looks like a partial list; a full-list "
            f"run would mark everyone missing as removed. Set allow_shrink=true to override."
        )
    if len(existing_shifts) == 0 and active_people > 0:
        problems.append(
            f"{TABLE} has {active_people} active people but {SHIFTS_TABLE} is empty/missing, "
            f"so shift changes can't be detected."
        )
    for p in problems:
        print(f"{'[DRY RUN] WOULD STOP' if dry_run else 'STOPPING'}: {p}")
    if problems and not dry_run:
        raise SystemExit("; ".join(problems))

    if dry_run:
        merge_into_state_table(grouped, client=None, dry_run=True)
        write_shifts_table(shift_rows, client=None, dry_run=True)
        return

    merge_into_state_table(grouped, client, dry_run=dry_run)
    write_shifts_table(shift_rows, client, dry_run=dry_run)


if __name__ == "__main__":
    dry_run = (os.getenv("DRY_RUN") or os.getenv("dry_run") or "false").lower() == "true"
    main(dry_run=dry_run)