"""
pw_county_email.py

Stage 4 of the poll watcher credentialing pipeline: builds one Gmail
DRAFT per county (never sent), attaching that county's volunteer
roster CSV and one combined PDF of all their credential letters.

Design decisions (confirmed 2026-09-11/12):
- DRAFTS ONLY, NEVER SENT. County notification is entirely a human-
  review step by design -- this script only ever calls
  drafts().create(), never messages().send(), same pattern as
  pw_send_credential_emails.py (Stage 5). There's deliberately no
  needs_human_review gate in the send logic here (unlike Stage 2) --
  a human opening Gmail and hitting send on a specific county IS the
  review step.
- TWO ATTACHMENTS PER COUNTY: (1) that county's roster CSV, attached
  as-is. (2) CHANGED 2026-09-21: the stitched all_assignments_<County>.pdf
  -- one PDF holding every credential letter for the county, attached as
  Credentials_<County>.pdf. This replaces an earlier zip of the individual
  PDFs, because counties can't accept zip files. The individual PDFs are
  still counted and checked against the roster (see MISMATCH HANDLING).
- INDIVIDUAL PDFs ARE ALREADY COUNTY-SCOPED. a teammate's script groups
  by county before grouping by volunteer, so a multi-county
  volunteer's Fulton letter and DeKalb letter are two separate files
  in two separate county folders (same filename, different content).
  This script just globs whatever's already in a given county's
  folder -- no cross-county filtering needed.
- MISMATCH HANDLING: NO DEGRADED DRAFTS (CHANGED 2026-09-12 -- original
  design drafted anyway with a warning subject prefix; corrected per
  feedback that this should follow the same skip-and-log pattern the
  other stages already use, e.g. pw_send_credential_emails.py's
  no_shots_found/template_missing/no_email statuses). If a county's
  roster row count doesn't match its individual PDF count, OR the
  county isn't found in the BRE merge sheet, OR the BRE row has no
  "To" address on file (a real, observed case -- several rows in the
  actual sheet have a blank To column with a comment like "emailed
  form requesting their email") -- that county's draft is NOT created.
  Instead a specific status is logged (credential_count_mismatch /
  county_not_in_bre_sheet / no_county_email_on_file) so a reviewer
  gets an actionable reason, not silence and not a half-populated
  draft someone could accidentally send.

OPEN, NOT YET CONFIRMED:
- BRE_MERGE_TAB: only the real sheet's gid (1353010808) is known, not
  its tab name. Placeholder guess below ("BRE") -- confirm before
  running against the real sheet.
- COUNTY_EMAIL_TAB / COUNTY_EMAIL_TEMPLATE_NAME: no confirmed evidence
  a "County Notification Email" tab/template exists yet in the Email
  Composition sheet (unlike Stage 2/5/6, which all confirmed their
  tabs against real screenshots). Placeholder guesses, assumed to
  follow the same transposed pattern (row1=template name,
  row2=subject, row3+=block list) as every other stage's tab.
- RESULTS STORAGE: Stage 4 has no BigQuery results table the way
  Stages 2/5/6 do -- this stage isn't wired into BigQuery at all yet
  (checklist: "wire the letter/CSV generator into the automated
  pipeline" is still open). Logging to a local CSV instead of
  inventing a new BigQuery table on a guess.
- Badge pickup merge fields: BRE sheet has "BRE Badge Blurb" plus three
  more specific blurb columns (Plan/Time/Location) -- which one(s) the
  real county-notification template merges in is unconfirmed. All four
  are made available as merge values below; unused ones are simply
  ignored by apply_merge_fields() if the real template doesn't
  reference them.
- a teammate's safe() function (used to build his county folder names) is
  not defined anywhere in his notebook as reviewed -- _normalize_county()
  below is a best-effort guess at reversing whatever sanitization it
  applies. If real folder names are sanitized in a way this doesn't
  account for (e.g. stripped apostrophes), county matching against the
  BRE sheet will silently fail for those counties and show up as
  county_not_in_bre_sheet even though the county IS in the sheet --
  revisit once safe()'s real behavior is confirmed.
"""

import base64
import csv
import glob
import html as html_lib
import io
import json
import logging
import os
import re
import uuid
import zipfile
from datetime import datetime, timezone

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- config -------------------------------------------------------------

CREDENTIALS_DIR = os.getenv("credentials_dir", "credentials_output")
RESULTS_CSV = os.getenv("results_csv", "pw_county_email_results.csv")

SPREADSHEET_ID = os.getenv("spreadsheet_id")
CONTENT_BLOCKS_TAB = os.getenv("content_blocks_tab", "Content Blocks")

# PLACEHOLDER -- real tab name not confirmed, only the gid is known.
BRE_MERGE_TAB = os.getenv("bre_merge_tab", "BRE")

# PLACEHOLDER -- existence of this tab/template not confirmed.
COUNTY_EMAIL_TAB = os.getenv("county_email_tab", "5 County Notification")
COUNTY_EMAIL_TEMPLATE_NAME = os.getenv(
    "county_email_template_name", "County Notification Email"
)

SENDER_ADDRESS = os.getenv("sender_address", "voterprotection@example.org")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# TEMPORARY TESTING WORKAROUND (added 2026-09-12) -- gmail.compose isn't
# authorized for domain-wide delegation yet (confirmed via unauthorized_client
# error), so drafts().create() can't succeed at all right now. This flag
# swaps to messages().send() (gmail.send scope, already confirmed working
# elsewhere in this pipeline) so the rest of Stage 4's logic -- Sheets
# reading, BRE matching, attachment building -- can still be validated
# end-to-end while compose stays blocked.
# NOT a production design change: Stage 4's confirmed design is
# drafts-only, human-review-before-send (see module docstring). Revert to
# draft-only once gmail.compose is actually authorized. DO NOT point this
# at the real BRE sheet while this flag is on -- it will actually send.
SEND_INSTEAD_OF_DRAFT = os.getenv("send_instead_of_draft", "false").lower() == "true"

# ADDED 2026-09-21 -- TEST-ONLY recipient override. When set, every county email
# goes to this one address instead of the BRE sheet's To, and CC is dropped.
# The sheet is never modified. Leave unset in production.
OVERRIDE_TO_ADDRESS = os.getenv("override_to_address", "").strip()

# ADDED (2026-09-18) -- scope used for the draft attempt, overridable so a
# broader scope already on the service account's delegation list (a teammate
# reports it's "scoped to overkill") can be tried without a code change,
# e.g. gmail_compose_scope=https://mail.google.com/ . Default stays the
# narrowest scope that covers drafts -- only override for testing, since
# a broader scope gives the token more access than draft creation needs.
GMAIL_COMPOSE_SCOPE = os.getenv("gmail_compose_scope", "https://www.googleapis.com/auth/gmail.compose")

# ADDED 2026-10-06 -- counties whose email goes out WITHOUT the roster CSV
# attached (comma-separated, case-insensitive), e.g. no_roster_counties=Fulton,DeKalb
# when VoPro prepares that county's roster by hand. The roster is still read for
# the letter-count check; it just isn't attached.
NO_ROSTER_COUNTIES = {c.strip().lower() for c in os.getenv("no_roster_counties", "").split(",") if c.strip()}

# ADDED 2026-10-27 -- Gmail bounces anything over 25 MB (after encoding, ~1.33x the raw
# attachment size) even though messages().send() reports success. Fulton (31 MB) and DeKalb
# (29 MB) stitched PDFs bounced. When roster + stitched PDF exceed max_attachment_mb, the
# county's letters are split into several emails ("Part 1 of 3"), each under the cap, with the
# roster CSV on part 1 only. Under the cap = one email, exactly as before.
MAX_ATTACHMENT_BYTES = int(float(os.getenv("max_attachment_mb", "12")) * 1_000_000)

# ADDED 2026-10-27 -- only process these counties (comma-separated, case-insensitive), e.g.
# only_counties=Fulton,DeKalb to resend two counties without re-sending the rest.
ONLY_COUNTIES = {c.strip().lower() for c in os.getenv("only_counties", "").split(",") if c.strip()}

BULLET = "\u25cf"
_URL_RE = re.compile(r"(https?://[^\s]+)")
BOLD_PHRASES = ["Regarding Poll Watcher badges:"]  # bolded inline wherever they appear
HEADER_LINES = set()  # unconfirmed for this template -- fill in once real content blocks are known


# --- auth -----------------------------------------------------------------

def build_service_account_info() -> dict:
    return json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))


def get_sheets_service():
    sa_info = build_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return build("sheets", "v4", credentials=creds)


def get_gmail_service():
    """Normally needs gmail.compose (drafts only). If SEND_INSTEAD_OF_DRAFT
    is on (temporary testing workaround -- see config section), uses
    gmail.send instead, since compose isn't authorized for domain-wide
    delegation yet (confirmed via unauthorized_client error). Only called
    when DRY_RUN is False, so local testing of the packaging logic doesn't
    require either scope."""
    sa_info = build_service_account_info()
    scope = (
        "https://www.googleapis.com/auth/gmail.send"
        if SEND_INSTEAD_OF_DRAFT
        else GMAIL_COMPOSE_SCOPE
    )
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=[scope])
    delegated = creds.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated)


# --- county folder discovery + file matching ------------------------------

def _normalize_county(name: str) -> str:
    """Best-effort match key between a teammate's folder names and the BRE
    sheet's County column. See module docstring re: safe() being
    unconfirmed."""
    return re.sub(r"[\s_]+", " ", name).strip().lower()


def discover_counties(credentials_dir: str) -> list:
    """One entry per county SUBFOLDER a teammate's script produced. A
    county with zero volunteers never gets a folder at all (his script
    only groups over counties present in the CSVs), so there's nothing
    to explicitly handle for that case -- it's simply absent here."""
    if not os.path.isdir(credentials_dir):
        raise FileNotFoundError(f"{credentials_dir} not found -- run a teammate's "
                                 f"letter/CSV generation first")
    counties = sorted(
        d for d in os.listdir(credentials_dir)
        if os.path.isdir(os.path.join(credentials_dir, d))
    )
    logger.info(f"Found {len(counties)} county folder(s) in {credentials_dir}")
    return counties


def count_roster_volunteers(csv_path: str) -> int:
    """Distinct volunteers in the roster, NOT raw rows.
    CHANGED 2026-09-18: a teammate's DeKalb roster format is one row per
    SHIFT while letters are one per volunteer, so a raw row count
    falsely mismatched whenever a DeKalb volunteer had 2+ shifts (seen
    with a real-shaped export: 31 rows vs 26 letters).
    CHANGED 2026-09-30: broadened to cover every county CSV format's real
    column names (credential_letters.py's build_standard/build_fulton/
    build_dekalb/build_cobb each use different columns -- this silently
    fell back to a raw row count for DeKalb/Cobb after that rewrite,
    reproducing the exact 2026-09-18 bug, since neither format's columns
    matched the old hardcoded "Volunteer_Name"/"Volunteer Address" check).
    Tries each known format's name column(s) in turn; falls back to the
    raw row count only if none match."""
    NAME_COLUMN_SETS = [
        ("Volunteer_Name",),                                              # standard
        ("Poll Watcher's (Last Name)", "Poll Watcher's (First Name)"),    # fulton
        ("First Name", "Last Name"),                                      # dekalb
        ("First_Name", "Last_Name"),                                      # cobb
    ]
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames or []
        name_cols = next((nc for nc in NAME_COLUMN_SETS if all(c in cols for c in nc)), None)
        if name_cols is None:
            return sum(1 for _ in reader)
        return len({tuple(r[c].strip() for c in name_cols) for r in reader})


def get_county_files(credentials_dir: str, county: str):
    """Returns (roster_csv_path, individual_pdf_paths, roster_row_count,
    stitched_pdf_path). The individual PDFs are only COUNTED (they are
    checked against the roster); CHANGED 2026-09-21: the email attaches the
    stitched all_assignments_<county>.pdf -- one PDF with every letter --
    instead of a zip of the individual PDFs (counties can't accept zips)."""
    county_dir = os.path.join(credentials_dir, county)

    roster_matches = glob.glob(os.path.join(county_dir, "county_submission_*.csv"))
    if not roster_matches:
        raise FileNotFoundError(f"No county_submission_*.csv found in {county_dir}")
    roster_csv_path = roster_matches[0]

    pdf_paths = sorted(
        p for p in glob.glob(os.path.join(county_dir, "*.pdf"))
        if not os.path.basename(p).startswith("all_assignments_")
    )

    stitched_matches = glob.glob(os.path.join(county_dir, "all_assignments_*.pdf"))
    if not stitched_matches:
        raise FileNotFoundError(f"No all_assignments_*.pdf found in {county_dir}")
    stitched_pdf_path = stitched_matches[0]

    roster_count = count_roster_volunteers(roster_csv_path)
    return roster_csv_path, pdf_paths, roster_count, stitched_pdf_path


# --- BRE merge sheet -------------------------------------------------------

def read_bre_merge_sheet() -> dict:
    """Keyed by _normalize_county(County) -> dict of {column_name: value}
    for that row, using the sheet's own header row for keys."""
    sheets_service = get_sheets_service()
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{BRE_MERGE_TAB}!A:W",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        logger.warning(f"BRE merge tab '{BRE_MERGE_TAB}' returned no rows")
        return {}

    header = rows[0]
    by_county = {}
    for row in rows[1:]:
        row = row + [""] * (len(header) - len(row))  # pad short rows
        record = dict(zip(header, row))
        county = record.get("County", "").strip()
        if not county:
            continue
        by_county[_normalize_county(county)] = record

    logger.info(f"Read {len(by_county)} county row(s) from BRE merge tab '{BRE_MERGE_TAB}'")
    return by_county


# --- sheet reading (same transposed pattern as other stages) --------------

def read_content_blocks():
    sheets_service = get_sheets_service()
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{CONTENT_BLOCKS_TAB}!A:B",
    ).execute()
    rows = resp.get("values", [])
    blocks = {}
    for r in rows:
        name = (r[0].strip() if len(r) > 0 and r[0] else "")
        content = (r[1] if len(r) > 1 and r[1] else "")
        if name and content:
            blocks[name] = content
    logger.info(f"Read {len(blocks)} content blocks from '{CONTENT_BLOCKS_TAB}'")
    return blocks


def read_county_composition():
    """Only COUNTY_EMAIL_TEMPLATE_NAME is read, same single-template
    pattern as Stage 5's read_credential_composition()."""
    import itertools

    sheets_service = get_sheets_service()
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{COUNTY_EMAIL_TAB}!B:Z",
    ).execute()
    rows = resp.get("values", [])
    transposed = list(itertools.zip_longest(*rows, fillvalue=""))

    composition = {}
    for col in transposed:
        name = col[0]
        if name != COUNTY_EMAIL_TEMPLATE_NAME:
            continue
        subject = col[1] if len(col) > 1 else ""
        blocks = [cell for cell in col[2:] if cell != ""]
        composition[name] = {"subject": subject, "blocks": blocks}

    logger.info(f"Read county template '{COUNTY_EMAIL_TEMPLATE_NAME}' from "
                f"'{COUNTY_EMAIL_TAB}': found={bool(composition)}")
    return composition


def apply_merge_fields(text: str, values: dict) -> str:
    def repl(m):
        return values.get(m.group(1).strip(), m.group(0))
    return re.sub(r"\*\|(.+?)\|\*", repl, text)


def build_email_from_composition(composition, content_blocks, values):
    comp = composition.get(COUNTY_EMAIL_TEMPLATE_NAME)
    if comp is None:
        logger.warning(f"Template '{COUNTY_EMAIL_TEMPLATE_NAME}' not found in "
                        f"'{COUNTY_EMAIL_TAB}' -- skipping")
        return None, None

    body = "\n\n".join(
        apply_merge_fields(content_blocks[b], values)
        for b in comp["blocks"]
        if b in content_blocks
    )
    missing = [b for b in comp["blocks"] if b not in content_blocks]
    if missing:
        logger.warning(f"Template references missing content block(s): {missing}")

    # ADDED 2026-09-20 -- the template's Subject cell may hold a content block
    # name (looked up in Content Blocks) or literal subject text.
    subject_cell = comp["subject"]
    subject = apply_merge_fields(content_blocks.get(subject_cell.strip(), subject_cell), values)
    # CHANGED 2026-10-27 -- the VoPro lead: Election Day county emails must not say "Advanced Voting"
    # (the "County Notification Subject" block still does). Rewritten here when election_type is
    # "Election Day", so it can't go out wrong even if the sheet block isn't edited. Remove once
    # the block itself is fixed.
    if os.getenv("election_type", "").strip().lower() == "election day":
        fixed = re.sub(r"\b(?:Advanced|Advance|Early)\s+Voting\b", "Election Day", subject, flags=re.I)
        if fixed != subject:
            logger.info(f"Subject rewritten for Election Day: {subject!r} -> {fixed!r}")
            subject = fixed
    return subject, body


def county_merge_values(county: str, bre_row: dict, volunteer_count: int) -> dict:
    return {
        "County": county,
        # ADDED (2026-09-18) -- the real "County Body" content block uses
        # these three (marked "Config" in the real sheet's Data Source
        # column). Same static values Stage 2/5 already use for the same
        # fields; without them they render as literal *|...|* text.
        # CHANGED 2026-10-27 -- was hardcoded "Early Voting"; set election_type="Election Day"
        # on the job/run for the Election Day county emails. Default unchanged.
        "Election_Type": os.getenv("election_type", "Early Voting"),
        "Election_Date": "November 3rd,",
        "Election Name": "2026 General Election",
        "Volunteer Count": str(volunteer_count),
        "BRE Badge Blurb": bre_row.get("BRE Badge Blurb", ""),
        "Badge Pick Up Plan Blurb": bre_row.get("Badge Pick Up Plan Blurb", ""),
        "Badge Pick Up Time Blurb": bre_row.get("Badge Pick Up Time Blurb", ""),
        "Badge Pick Up Location Blurb": bre_row.get("Badge Pick Up Location Blurb", ""),
    }


# --- rendering (same to_html pattern as other stages) ----------------------

def _linkify(text):
    return _URL_RE.sub(r'<a href="\1">\1</a>', text)


def to_html(body):
    out = []
    for block in body.split("\n\n"):
        lines = block.split("\n")
        nonblank = [l for l in lines if l.strip()]
        if nonblank and all(l.lstrip().startswith(BULLET) for l in nonblank):
            items = ""
            for l in nonblank:
                text = l.lstrip().lstrip(BULLET).strip()
                items += "<li>" + _linkify(html_lib.escape(text)) + "</li>"
            out.append("<ul>" + items + "</ul>")
        else:
            rendered = []
            for l in lines:
                esc = _linkify(html_lib.escape(l))
                for phrase in BOLD_PHRASES:
                    esc = esc.replace(html_lib.escape(phrase), "<b>" + html_lib.escape(phrase) + "</b>")
                if l.strip() in HEADER_LINES:
                    esc = "<b>" + esc + "</b>"
                rendered.append(esc)
            out.append("<p>" + "<br>".join(rendered) + "</p>")
    return "".join(out)


# --- attachment building ----------------------------------------------------

def read_stitched_pdf(pdf_path: str) -> bytes:
    """The county's single combined credentials PDF (a teammate's
    all_assignments_<county>.pdf). CHANGED 2026-09-21 -- replaces the old
    build_credentials_zip(); counties can't accept zips."""
    with open(pdf_path, "rb") as f:
        return f.read()


# --- gmail: draft creation (default) or send (temporary testing mode) ------

def split_county_pdfs(pdf_paths: list, first_part_reserved_bytes: int, max_bytes: int) -> list:
    """ADDED 2026-10-27 -- groups the individual letter PDFs (alphabetical, the same order as the
    stitched file) into parts of at most max_bytes each; part 1 leaves room for the roster CSV.
    Returns a list of stitched-PDF bytes, one per part."""
    from pypdf import PdfWriter   # only needed when a county is too big for one email

    groups, size = [[]], first_part_reserved_bytes
    for p in pdf_paths:
        s = os.path.getsize(p)
        if groups[-1] and size + s > max_bytes:
            groups.append([])
            size = 0
        groups[-1].append(p)
        size += s

    parts = []
    for g in groups:
        w = PdfWriter()
        for p in g:
            w.append(p)
        buf = io.BytesIO()
        w.write(buf)
        w.close()
        parts.append(buf.getvalue())
    return parts


def _build_county_message(to_email, cc_email, subject, body,
                           roster_csv_path: str, credentials_pdf_bytes: bytes, county: str,
                           attach_roster: bool = True, pdf_filename: str = None) -> str:
    """Shared MIME construction for both create_county_draft() and
    send_county_email() -- identical message either way, only the final
    API call differs."""
    html_body = to_html(body)

    message = MIMEMultipart()
    message.attach(MIMEText(html_body, "html"))

    if attach_roster and county.strip().lower() not in NO_ROSTER_COUNTIES:
        with open(roster_csv_path, "rb") as f:
            roster_part = MIMEApplication(f.read(), _subtype="csv")
        roster_part.add_header(
            "Content-Disposition", "attachment", filename=os.path.basename(roster_csv_path)
        )
        message.attach(roster_part)

    pdf_part = MIMEApplication(credentials_pdf_bytes, _subtype="pdf")
    pdf_part.add_header(
        "Content-Disposition", "attachment",
        filename=pdf_filename or f"Credentials_{county.replace(' ', '_')}.pdf",
    )
    message.attach(pdf_part)

    message["to"] = to_email
    if cc_email:
        message["cc"] = cc_email
    message["subject"] = subject
    return base64.urlsafe_b64encode(message.as_bytes()).decode()


def create_county_draft(service, to_email, cc_email, subject, body,
                         roster_csv_path: str, credentials_pdf_bytes: bytes, county: str,
                         attach_roster: bool = True, pdf_filename: str = None):
    raw = _build_county_message(to_email, cc_email, subject, body,
                                 roster_csv_path, credentials_pdf_bytes, county,
                                 attach_roster, pdf_filename)
    return service.users().drafts().create(userId="me", body={"message": {"raw": raw}}).execute()


def send_county_email(service, to_email, cc_email, subject, body,
                       roster_csv_path: str, credentials_pdf_bytes: bytes, county: str,
                       attach_roster: bool = True, pdf_filename: str = None):
    """TEMPORARY TESTING WORKAROUND -- only called when SEND_INSTEAD_OF_DRAFT
    is on. See config section for why this exists and why it must not be
    pointed at the real BRE sheet."""
    raw = _build_county_message(to_email, cc_email, subject, body,
                                 roster_csv_path, credentials_pdf_bytes, county,
                                 attach_roster, pdf_filename)
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()


# --- results log ------------------------------------------------------------

def write_results(results: list, results_csv: str):
    if not results:
        return
    file_exists = os.path.isfile(results_csv)
    with open(results_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)
    logger.info(f"Wrote {len(results)} result row(s) to {results_csv}")


# --- main --------------------------------------------------------------

def main():
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting county notification draft run -- run_id={run_id} "
                f"(DRY_RUN={DRY_RUN})")
    if OVERRIDE_TO_ADDRESS:
        logger.warning(f"override_to_address is set -- ALL county emails go to "
                        f"{OVERRIDE_TO_ADDRESS}, not to the BRE sheet's addresses. TEST ONLY.")
    if SEND_INSTEAD_OF_DRAFT and not DRY_RUN:
        logger.warning("SEND_INSTEAD_OF_DRAFT is TRUE -- this run will actually SEND "
                        "real emails via messages().send(), not create drafts. "
                        "TEMPORARY TESTING MODE ONLY -- confirm spreadsheet_id points "
                        "at a TEST sheet before proceeding.")

    content_blocks = read_content_blocks()
    composition = read_county_composition()
    if COUNTY_EMAIL_TEMPLATE_NAME not in composition:
        logger.warning("County notification template not found in the sheet -- nothing to do")
        return

    bre_by_county = read_bre_merge_sheet()
    gmail_service = None if DRY_RUN else get_gmail_service()

    results = []
    if ONLY_COUNTIES:
        logger.warning(f"only_counties is set -- processing only: {sorted(ONLY_COUNTIES)}")
    for county in discover_counties(CREDENTIALS_DIR):
        if ONLY_COUNTIES and county.strip().lower() not in ONLY_COUNTIES:
            continue
        roster_csv_path, pdf_paths, roster_count, stitched_pdf_path = get_county_files(CREDENTIALS_DIR, county)
        pdf_count = len(pdf_paths)

        base_result = {
            "county": county, "roster_count": roster_count, "pdf_count": pdf_count,
            "run_id": run_id, "run_datetime": run_datetime,
        }

        if roster_count != pdf_count:
            logger.error(f"{county}: roster/PDF count mismatch "
                         f"({roster_count} roster volunteers vs {pdf_count} PDFs) -- skipping draft")
            results.append({**base_result, "status": "credential_count_mismatch", "error_reason":
                             f"{roster_count} roster volunteers vs {pdf_count} individual PDFs"})
            continue

        bre_row = bre_by_county.get(_normalize_county(county))
        if bre_row is None:
            logger.error(f"{county}: not found in BRE merge sheet -- skipping draft")
            results.append({**base_result, "status": "county_not_in_bre_sheet",
                             "error_reason": "no matching row in BRE merge tab"})
            continue

        to_email = bre_row.get("To", "").strip()
        # CHANGED 2026-10-06: a blank BRE 'To' only skips the county when there is
        # no override. With override_to_address set, the review copy still goes to
        # the override address (the county's real address isn't used anyway), and a
        # warning reminds the reviewer that the real address is missing.
        if not to_email and not OVERRIDE_TO_ADDRESS:
            logger.error(f"{county}: BRE sheet has no 'To' address on file -- skipping draft")
            results.append({**base_result, "status": "no_county_email_on_file",
                             "error_reason": "BRE sheet 'To' column is blank"})
            continue
        cc_email = bre_row.get("CC", "").strip()
        if OVERRIDE_TO_ADDRESS:
            if not to_email:
                logger.warning(f"{county}: BRE sheet has NO 'To' address -- review copy goes to "
                               f"{OVERRIDE_TO_ADDRESS}; get a real address before forwarding")
            else:
                logger.info(f"{county}: OVERRIDE -- sheet To={to_email!r} cc={cc_email!r} replaced "
                            f"with {OVERRIDE_TO_ADDRESS}")
            to_email, cc_email = OVERRIDE_TO_ADDRESS, ""

        values = county_merge_values(county, bre_row, pdf_count)
        subject, body = build_email_from_composition(composition, content_blocks, values)
        if subject is None:
            results.append({**base_result, "status": "template_missing",
                             "error_reason": f"template '{COUNTY_EMAIL_TEMPLATE_NAME}' not found"})
            continue

        roster_attached = county.strip().lower() not in NO_ROSTER_COUNTIES
        roster_bytes = os.path.getsize(roster_csv_path) if roster_attached else 0
        if os.path.getsize(stitched_pdf_path) + roster_bytes <= MAX_ATTACHMENT_BYTES:
            # (subject, body, pdf bytes, attachment filename, attach roster?)
            mail_parts = [(subject, body, read_stitched_pdf(stitched_pdf_path), None, True)]
        else:
            try:
                pdf_chunks = split_county_pdfs(pdf_paths, roster_bytes, MAX_ATTACHMENT_BYTES)
            except Exception as e:
                logger.error(f"{county}: could not split the credentials PDF: {e}")
                results.append({**base_result, "status": "pdf_split_error", "error_reason": str(e)})
                continue
            n = len(pdf_chunks)
            logger.warning(f"{county}: too big for one email -- split into {n} parts "
                           f"({[round(len(c) / 1e6, 1) for c in pdf_chunks]} MB of PDF)")
            mail_parts = []
            for i, chunk in enumerate(pdf_chunks, 1):
                note = (f"[Part {i} of {n}] The credentials for all {pdf_count} volunteers are split "
                        f"across {n} emails because of file size."
                        + (" The roster CSV is attached to this first email." if i == 1 and roster_attached else "")
                        + "\n\n")
                mail_parts.append((f"{subject} (Part {i} of {n})", note + body, chunk,
                                   f"Credentials_{county.replace(' ', '_')}_Part_{i}_of_{n}.pdf", i == 1))

        if DRY_RUN:
            for (p_subject, _b, p_pdf, p_name, p_roster) in mail_parts:
                logger.info(f"[DRY RUN] Would draft to={to_email} cc={cc_email or '(none)'} "
                            f"subject={p_subject!r} attachments=["
                            f"{'NO ROSTER CSV' if not (p_roster and roster_attached) else os.path.basename(roster_csv_path)}, "
                            f"{p_name or f'Credentials_{county}.pdf'} ({pdf_count} letters in county, {len(p_pdf)} bytes)]")
            results.append({**base_result, "status": "dry_run_ok", "error_reason": None})
            continue

        sent_parts = 0
        try:
            for (p_subject, p_body, p_pdf, p_name, p_roster) in mail_parts:
                if SEND_INSTEAD_OF_DRAFT:
                    send_county_email(gmail_service, to_email, cc_email, p_subject, p_body,
                                       roster_csv_path, p_pdf, county, p_roster, p_name)
                else:
                    create_county_draft(gmail_service, to_email, cc_email, p_subject, p_body,
                                         roster_csv_path, p_pdf, county, p_roster, p_name)
                sent_parts += 1
            part_note = f" in {len(mail_parts)} emails" if len(mail_parts) > 1 else ""
            if SEND_INSTEAD_OF_DRAFT:
                logger.info(f"{county}: SENT ({pdf_count} credential(s) attached{part_note}) -- TEMPORARY TESTING MODE")
                results.append({**base_result, "status": "sent_test_mode", "error_reason": None})
            else:
                logger.info(f"{county}: drafted ({pdf_count} credential(s) attached{part_note})")
                results.append({**base_result, "status": "drafted", "error_reason": None})
        except Exception as e:
            action = "send" if SEND_INSTEAD_OF_DRAFT else "draft creation"
            status = "send_error" if SEND_INSTEAD_OF_DRAFT else "draft_error"
            logger.error(f"{county}: {action} failed after {sent_parts} of {len(mail_parts)} part(s): {e}")
            results.append({**base_result, "status": status,
                             "error_reason": f"{sent_parts} of {len(mail_parts)} parts sent; {e}"})

    write_results(results, RESULTS_CSV)

    by_status = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    logger.info("=" * 60)
    logger.info("  COUNTY NOTIFICATION DRAFT RUN COMPLETE")
    logger.info(f"  Run ID: {run_id}")
    for status, count in sorted(by_status.items()):
        logger.info(f"  {status}: {count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()