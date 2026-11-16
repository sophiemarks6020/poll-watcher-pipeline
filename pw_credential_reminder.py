"""
pw_send_credential_reminders.py

Stage 6 of the poll watcher credentialing pipeline: a quick heads-up
email 2 days before each volunteer's shift.

Design notes:
- ONE EMAIL PER SHIFT, NOT A ROLLUP (per checklist) -- this is the
  opposite pattern from Stage 2's combined email. A volunteer with 3
  shifts gets 3 separate reminder emails, each 2 days ahead of that
  specific shift, each with its own single-event .ics. Structurally
  this matches send_training_reminders.py's existing pattern (one row
  = one event = one email), not Stage 2's per-volunteer grouping.
- SOURCE: queries commons.pw_credential_shifts directly for shifts
  starting in 48 hours -- no join to pw_credential_status needed here,
  since reminders aren't gated by confirmation/review status the way
  Stage 2 sends are. (Open question: should a volunteer who never
  confirmed their assignment, or who's still held for human review,
  get a reminder at all? Not addressed here -- currently reminds
  everyone with an upcoming shift regardless of pw_credential_status.
  Flag for the VoPro lead if that's wrong.)
- DEDUP: same run_id/results-table pattern as send_training_reminders.py
  -- a results table tracks what's already been sent per (van_id,
  county, event_date, location) so a daily run doesn't re-send. Unlike
  Stage 2, there's no state flip on the source table itself (nothing on
  pw_credential_shifts to update -- it's a plain recompute-fresh table
  with no per-row status), so the results table IS the only dedup
  mechanism here, not a secondary belt-and-suspenders layer.
- 🔴 PDF ATTACHMENT IS STUBBED -- "re-attach the cached credential PDF"
  is not implemented. a teammate's credential_letters_script.ipynb
  currently runs manually, locally, saving PDFs to his own machine --
  there's no BigQuery/GCS location or automated output for these files
  yet. That's Stage 4's open "wire the letter/CSV generator into the
  automated pipeline" item. get_cached_credential_pdf() below is a
  clearly-marked no-op placeholder; wire it up once Stage 4 has a real
  storage location for generated PDFs.
- OWN send_email(), NOT REUSED FROM send_training_reminders.py (CHANGED
  2026-09-05): originally this imported send_email directly from
  send_training_reminders.py. That works fine for the .ics/MIME
  mechanics, but it means this script had NO hook to apply the named-
  link replacements ("Poll Watcher Manual Link", "On Demand Training",
  "LBJ Walkthrough" all need to become real <a> tags at send time, same
  pattern as pw_post_training_emails.py and the assignment-email fix)
  -- calling someone else's send_email() as a black box means there's
  nowhere to inject that. Also, send_training_reminders.py's own
  to_html() has its own HEADER_LINES set scoped to ITS templates
  (doesn't include "LBJ Practice"/"LBJ", which this template's content
  blocks use) -- reusing it as-is would silently fail to bold those
  two headers. So build_ics_attachment/get_gmail_service are still
  reused (generic utilities, no template-specific behavior baked in),
  but to_html() and send_email() are now defined locally here, with a
  HEADER_LINES set correct for THIS template, tested against the real
  sheet content via pw_send_credential_reminder_preview.py.

OPEN, NOT YET CONFIRMED:
- Whether reminders should be gated by pw_credential_status at all (see
  SOURCE note above).
- Shift time parsing: event_date parsing is locked to a teammate's
  confirmed "%Y-%m-%d" format, but start_time is still unconfirmed/
  fuzzy-parsed since his script never parses time.
- Tab is confirmed (4 Assignment Emails, per the Email Composition - 26
  General sheet) -- "-2 Day Reminder" is column I on this same tab.
- 3 "Badge Pick Up" fields (*|Badge Pick Up Time/Location/Plan Blurb|*)
  found in the real template (2026-09-05) are genuinely blocked on
  Stage 4/5 -- no data source exists yet. NOT addressed in this file.
"""

import json
import logging
import os
import re
import uuid
import base64
import html as html_lib
from datetime import date, datetime, timedelta, timezone
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dateutil import parser as dateutil_parser

# Only these two are still reused -- generic .ics/auth utilities with
# no template-specific rendering behavior. send_email is NOT imported
# anymore -- see module docstring.
# FIXED 2026-09-28: the module docstring's references to
# "send_training_reminders.py" were stale -- that file doesn't exist in
# this repo. The real module with these two utilities is
# pw_training_reminder_email.py (same SENDER_ADDRESS, same
# build_ics_attachment signature). This import has apparently never
# resolved until now.
from pw_training_reminder_email import build_ics_attachment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- config -----------------------------------------------------------

BQ_PROJECT_ID = os.getenv("project", "your-gcp-project")
DATASET = "commons"

SHIFTS_TABLE = os.getenv("shifts_table", f"{DATASET}.pw_credential_shifts")
STATUS_TABLE = os.getenv("status_table", f"{DATASET}.pw_credential_status")
RESULTS_TABLE = os.getenv("results_table", f"{DATASET}.pw_credential_reminder_results")

SPREADSHEET_ID = os.getenv("spreadsheet_id")
CONTENT_BLOCKS_TAB = os.getenv("content_blocks_tab", "Content Blocks")
REMINDER_EMAIL_TAB = os.getenv("credential_reminder_tab", "4 Assignment Emails")  # confirmed: "-2 Day Reminder" is column I on this same tab
REMINDER_TEMPLATE_NAME = os.getenv("reminder_template_name", "-2 Day Reminder")

REMINDER_OFFSET_HOURS = int(os.getenv("reminder_offset_hours", "48"))

# ADDED 2026-10-09 -- test mode, same idea as pw_send_credential_emails.py's
# test_recipient. When test_recipient is set: every reminder goes to that
# address (subject prefixed with the real address), the results table is NOT
# written and NOT read for dedup, so a test can never suppress a real reminder.
# test_van_id (optional) limits the test to one real volunteer's shifts;
# otherwise one shift per county. test_limit caps how many emails go out.
# Leave all three unset for normal runs.
TEST_RECIPIENT = os.getenv("test_recipient", "").strip()
TEST_VAN_ID = os.getenv("test_van_id", "").strip()
TEST_LIMIT = int(os.getenv("test_limit", "5"))

# Fail closed: the test params widen the window / narrow the audience, so if
# they are present but test_recipient did not arrive (typo, dropped param),
# refuse to run rather than send real reminders across a huge window.
if (TEST_VAN_ID or REMINDER_OFFSET_HOURS != 48) and not TEST_RECIPIENT:
    raise SystemExit(
        "REFUSING TO RUN: test_van_id / reminder_offset_hours is set but test_recipient is empty. "
        "Set test_recipient, or remove the test params."
    )
# A mistyped test address must never reach an outside inbox.
if TEST_RECIPIENT and not TEST_RECIPIENT.lower().endswith("@example.org"):
    raise SystemExit("REFUSING TO RUN: test_recipient must be an @example.org address")

# dry_run=true builds every email and logs who it WOULD go to, but never creates
# a Gmail connection, never sends, and never writes the results table.
DRY_RUN = os.getenv("dry_run", os.getenv("DRY_RUN", "false")).strip().lower() == "true"

# CHANGED 2026-10-09: now a job param (same as pw_send_credential_emails.py) so
# this job can send as assignments@example.org. Default is unchanged.
SENDER_ADDRESS = os.getenv("sender_address", "voterprotection@example.org")
EASTERN = ZoneInfo("America/New_York")

# Optional job param: comma-separated @example.org addresses that get a one-line
# "ran successfully" email after a clean real run (>= 1 reminder sent, 0 failed). Nothing
# is sent on dry runs, test runs, failures, or runs where nothing went out. Unset = no email.
STATUS_RECIPIENTS_RAW = os.getenv("status_recipients", "")

# Confirmed 2026-09-05 by Sophie. Single fixed date for the whole
# election cycle -- update this once per cycle, same pattern as
# TRAINING_DEADLINE_EDAY / ASSIGNMENT_DATES / CREDENTIAL_DATES in
# pw_post_training_emails.py.
ELECTION_DATE = os.getenv("election_date", "November 3rd")
POLL_WATCHER_MANUAL_LINK = os.getenv("poll_watcher_manual_link", "https://gadems.info/PWManual")
ON_DEMAND_TRAINING_LINK = os.getenv("on_demand_training_link", "https://gadems.info/OD")
LBJ_WALKTHROUGH_LINK = os.getenv("lbj_walkthrough_link", "https://gadems.info/LBJvideo")
# ADDED 2026-10-09 -- "Preparing for Your Shift" bullets.
HOW_TO_PACK_LINK = os.getenv("how_to_pack_link", "https://gadems.info/how-to-pack")
LBJ_DEMO_LINK = os.getenv("lbj_demo_link", "https://gadems.info/lbj-demo")
# ADDED 2026-10-11 -- links from the VoPro lead's approved mockup: "LBJ On Demand" and the
# "watching this brief walkthrough demonstration" phrase in the same bullet.
LBJ_ON_DEMAND_LINK = os.getenv("lbj_on_demand_link", "https://gadems.info/lbj-OD")
# CHANGED 2026-10-26 -- the VoPro lead: this phrase must link to https://gadems.info/lbj-demo
# (matches the Preparation Content Block), not the Drive video.
LBJ_DEMO_VIDEO_LINK = os.getenv("lbj_demo_video_link", "https://gadems.info/lbj-demo")
# ADDED 2026-09-25 -- the shared "Resources" block now has a *|Poll Watcher FAQ|* merge
# field (VoPro's Data Mapping sheet, "4 Assignments" tab). Same URL as pw_assignment_emails.py.
POLL_WATCHER_FAQ_LINK = os.getenv(
    "poll_watcher_faq_link",
    "https://drive.google.com/file/d/YOUR_FILE_ID/view",
).strip()
# ADDED 2026-09-28 -- "My Voter Page - GA Secretary of State" is a literal
# phrase in the content blocks (not a *|...|* merge field), same pattern
# and same URL as pw_assignment_emails.py / pw_send_credential_emails.py.
MY_VOTER_PAGE_LINK = os.getenv("my_voter_page_link", "https://mvp.sos.ga.gov/s/")

# ADDED 2026-09-28 -- badge pickup blurbs, ported from
# pw_send_credential_emails.py's BRE-merge-sheet lookup (same tab, same
# three fields). Stage 6 is per-shift/per-county (see module docstring),
# so only the single-county path is needed here -- no multi-county
# grouping like Stage 5's badge_pickup_values_for_counties().
BRE_MERGE_TAB = os.getenv("bre_merge_tab", "BRE")
BADGE_PICKUP_MODE = os.getenv("badge_pickup_mode", "real")

# ADDED 2026-10-11 -- the daily REMINDER LIST. the VoPro lead's team hands us the day's LBJ
# assignments export (the shifts two days out); it is imported into a tab (default
# "Reminder List") and ONLY volunteers on it -- matched on VAN ID + date + start time -- get a
# reminder. A cancelled shift is simply absent from the list. Replaces the 48-hour sliding window
# for real runs: everyone on the list is reminded in one run (shifts that already started, or
# that start more than reminder_list_max_hours out, are skipped so a stale or wrong-day file
# can't trigger sends). CHANGED 2026-10-12: the shift (time, location, address, county) and the
# contact email/first name come from the list's OWN columns; pw_credential_shifts is only a fallback
# for blanks, so a shift changed after the last ingest is reminded correctly.
# FAILS CLOSED: if the tab can't be read, or has no valid rows, the run stops and sends nothing.
# Test mode (test_recipient set) ignores the list and uses the old window, since test emails
# can only go to the test recipient. Lives in the same spreadsheet as the content blocks unless
# reminder_list_spreadsheet_id points elsewhere (recommended: a separate restricted sheet shared
# with the service account -- the export holds names/emails/phones; keep only the three columns
# VAN ID, Date, Start Time).
REMINDER_LIST_SPREADSHEET_ID = os.getenv("reminder_list_spreadsheet_id", "").strip() or SPREADSHEET_ID
REMINDER_LIST_TAB = os.getenv("reminder_list_tab", "Reminder List").strip()
REMINDER_LIST_MAX_HOURS = int(os.getenv("reminder_list_max_hours", "72"))

# ADDED 2026-10-09 -- same personal badge-pickup assignment the credential emails used
# (pw_send_credential_emails.py): for Fulton/DeKalb volunteers, find their nearest site in
# NEAREST_PICKUP_TABLE and use THAT site's row from the pickup-sites TAB (same spreadsheet as
# the BRE merge sheet). Without this, Fulton/DeKalb reminders fell back to the county-level
# BRE row, which is blank for them, and said "the county has not yet provided badge pick up
# instructions" -- contradicting their credential email. Same param names as Stage 5. The gid
# default is the pickup-sites tab used for the real credential send; set pickup_sites_tab_gid
# (or pickup_sites_tab) on the job to override, or set both to "-" to switch the feature off.
NEAREST_PICKUP_TABLE = os.getenv("nearest_pickup_table", f"{DATASET}.pw_volunteer_nearest_pickup")
PICKUP_SITES_TAB = os.getenv("pickup_sites_tab", "").strip()
PICKUP_SITES_GID = os.getenv("pickup_sites_tab_gid", "1013613473").strip()
NEAREST_PICKUP_COUNTIES = set(
    c.strip() for c in os.getenv("nearest_pickup_counties", "Fulton,DeKalb").split(",") if c.strip()
)

# Local to_html() rendering constants -- see module docstring for why
# this isn't reused from send_training_reminders.py.
BULLET = "\u25cf"
# CHANGED 2026-10-26 -- bold markers (\x01 \x02, defined below) are not part of a URL, so a bold
# link can't pull its closing marker into the href.
_URL_RE = re.compile(r"(https?://[^\s\x01\x02]+)")
# Whole lines rendered bold. CHANGED 2026-10-09 -- added the headers and
# full-sentence bolds from the VoPro lead's updated content blocks (Remind
# Location, Remind Badge, Preparation, Remind Arriving, Credential
# Reminder, Remind Closing).
HEADER_LINES = {
    "Credentials",
    "Resources",
    "Communications During Your Shift",
    "LBJ Practice",
    "LBJ",
    "Preparing for Your Shift",
    "When Your Shift Starts",
    "Badge and Credential Letter(s)",
    "BADGE PICK UP INSTRUCTIONS",
    "THANK YOU!!",
    "LBJ Tips",
    "As a reminder, your Advanced Voting assignment two days from now is:",
    # CHANGED 2026-10-11 -- the sheet now says "Early Voting".
    "As a reminder, your Early Voting assignment two days from now is:",
}
# ADDED 2026-10-11 -- whole lines rendered bold AND italic (per the VoPro lead's mockup).
BOLD_ITALIC_LINES = {
    "If you have not yet picked up your badge, please see badge pick up instructions at the bottom of this email.",
}
# ADDED 2026-10-09 -- partial-line bolds/italics from the updated content
# blocks. Matched against already-HTML-escaped text, so none of these may
# contain characters html.escape changes (& < > " ').
BOLD_PHRASES = [
    "A digital copy of your credential letter(s) was emailed to you on Wednesday, October 7th, 2026.",
    "bring your ID and a printed copy of your credential letter(s) with you",
]
# Bold only when the line starts with it ("Remember: You are not permitted..."),
# so "But remember, the best day..." elsewhere is left alone.
LINE_START_BOLD = ["Remember"]
# Bold only inside bullet items -- the Resources "Remember" paragraph also
# mentions the Boiler Room, and that mention is not bold in the sheet.
BULLET_BOLD_PHRASES = [
    "Boiler Room",
    "800-743-1541",
    "Voter Protection Hotline",
    "888-730-5816",
]
ITALIC_LINES = {"Happy Poll Watching!"}
# Wraps a merge value that must render bold (e.g. the Badge Pick Up Plan Blurb).
BOLD_MARKER_START = "\x01"
BOLD_MARKER_END = "\x02"

# ADDED 2026-10-26 -- bold_source, ported from pw_assignment_emails.py. "code" (default) = unchanged:
# the hardcoded bold lists above decide what is bold. "sheet" = the bold in the Content Blocks tab
# is carried through to the email (read with the Sheets API's textFormatRuns), so a bold word added
# in the sheet appears in the email with no script change. In sheet mode BOLD_PHRASES,
# LINE_START_BOLD and BULLET_BOLD_PHRASES are NOT applied and the link swaps in render_html() stop
# adding their own <b>; HEADER_LINES / BOLD_ITALIC_LINES / ITALIC_LINES still format whole lines.
BOLD_SOURCE = os.getenv("bold_source", "code").strip().lower()
if BOLD_SOURCE not in ("code", "sheet"):
    raise SystemExit(f"REFUSING TO RUN: bold_source must be 'code' or 'sheet', got {BOLD_SOURCE!r}")
# True while sheet bold is in effect; read_content_blocks() turns it off if the sheet's formatting
# can't be read, so every code path below checks THIS, not BOLD_SOURCE.
_SHEET_BOLD = (BOLD_SOURCE == "sheet")
# Merge fields and the "My Voter Page" phrase are swapped later, so bold is widened to cover them
# whole instead of letting a marker split one (the swap would silently fail).
_MY_VOTER_PHRASE = "My Voter Page - GA Secretary of State"
_KEEP_WHOLE_RE = re.compile(r"\*\|.+?\|\*|" + re.escape(_MY_VOTER_PHRASE))
_MARKERS_RE = re.compile("[" + BOLD_MARKER_START + BOLD_MARKER_END + "]")


def _strip_bold_markers(text: str) -> str:
    return _MARKERS_RE.sub("", text)


def _mark_bold_runs(text: str, runs: list, cell_bold: bool) -> str:
    """Insert bold markers into a cell's text from the Sheets API's textFormatRuns (each run
    starts at startIndex, in UTF-16 code units, and lasts until the next run starts; a run
    without its own "bold" setting inherits the cell-level bold). Markers are written one
    line at a time so bold that spans a line break stays balanced."""
    n = len(text)
    flags = [cell_bold] * n
    if runs:
        u16_to_char = []
        for ci, ch in enumerate(text):
            u16_to_char.extend([ci] * (2 if ord(ch) > 0xFFFF else 1))
        total = len(u16_to_char)
        for i, run in enumerate(runs):
            start = run.get("startIndex", 0)
            end = runs[i + 1].get("startIndex", 0) if i + 1 < len(runs) else total
            bold = run.get("format", {}).get("bold", cell_bold)
            for u in range(start, min(end, total)):
                flags[u16_to_char[u]] = bold
    for m in _KEEP_WHOLE_RE.finditer(text):
        if any(flags[m.start():m.end()]):
            flags[m.start():m.end()] = [True] * (m.end() - m.start())
    out, i = [], 0
    while i < n:
        if text[i] == "\n":
            out.append("\n")
            i += 1
            continue
        j = i
        while j < n and text[j] != "\n" and flags[j] == flags[i]:
            j += 1
        seg = text[i:j]
        out.append(BOLD_MARKER_START + seg + BOLD_MARKER_END if flags[i] and seg.strip() else seg)
        i = j
    return "".join(out)


def build_service_account_info() -> dict:
    return json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))


sa_info = build_service_account_info()
sheets_creds = service_account.Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)
sheets_service = build("sheets", "v4", credentials=sheets_creds)


def get_gmail_service():
    """
    CHANGED 2026-10-09: local copy (was imported from pw_training_reminder_email.py,
    which hardcodes voterprotection@) so the sender follows the sender_address param.
    """
    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    return build("gmail", "v1", credentials=credentials.with_subject(SENDER_ADDRESS))


# --- shift parsing (same split as pw_send_assignment_emails.py) ---------

def parse_shift_datetimes(event_date: str, start_time: str, end_time: str):
    """
    DATE: strict "%Y-%m-%d", matching a teammate's confirmed fmt_date().
    TIME: dateutil fuzzy parse, still unconfirmed -- see
    pw_send_assignment_emails.py's parse_shift_datetimes docstring for
    the full reasoning; identical caveat applies here.
    """
    try:
        event_date_parsed = datetime.strptime(event_date, "%Y-%m-%d").date()
    except ValueError as e:
        logger.warning(f"Could not parse event_date {event_date!r} as %Y-%m-%d: {e}")
        return None, None

    try:
        start_dt = dateutil_parser.parse(start_time, default=datetime.combine(event_date_parsed, datetime.min.time()))
        end_dt = dateutil_parser.parse(end_time, default=datetime.combine(event_date_parsed, datetime.min.time()))
        return start_dt.replace(tzinfo=EASTERN), end_dt.replace(tzinfo=EASTERN)
    except (ValueError, OverflowError, TypeError) as e:
        logger.warning(f"Could not parse shift time ({start_time!r}, {end_time!r}) for date {event_date!r}: {e}")
        return None, None


# --- reminder list -----------------------------------------------------------
# ADDED 2026-10-11. Every ambiguity below resolves toward NOT sending: a row that can't be read
# in full is skipped (and warned about), never widened into "everything for this volunteer".

def _norm_van_id(value) -> str:
    # "100000001.0" (a number-formatted cell) must not become "1135940000"
    return re.sub(r"\D", "", re.sub(r"\.0+\s*$", "", str(value or "").strip()))


def _norm_header(value) -> str:
    return re.sub(r"[^a-z]", "", str(value).lower())


_LIST_VAN_HEADERS = {"vanid"}
_LIST_DATE_HEADERS = {"date", "shiftdate", "eventdate"}
_LIST_TIME_HEADERS = {"starttime", "shifttime", "time", "shiftstarttime"}


def _find_col(header: list, names: set):
    for i, h in enumerate(header):
        if h in names:
            return i
    return None


def _parse_list_date(text: str):
    text = text.strip()
    if not text:
        return None
    try:
        today = datetime.now(EASTERN).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        return dateutil_parser.parse(text, default=today).date()
    except (ValueError, OverflowError):
        return None


def _parse_list_time(text: str):
    """(hour, minute) of the START time; "7:00 AM - 1:00 PM" uses the 7:00 AM. None if blank/unreadable."""
    text = re.split(r"\s*(?:-|\u2013|\u2014|\bto\b)\s*", text.strip(), maxsplit=1)[0].strip()
    if not text:
        return None
    try:
        parsed = dateutil_parser.parse(text)
        return (parsed.hour, parsed.minute)
    except (ValueError, OverflowError):
        return None


_LIST_END_HEADERS = {"endtime", "shiftendtime"}
_LIST_LOCATION_HEADERS = {"pollinglocation", "locationname"}
_LIST_ADDRESS_HEADERS = {"pollinglocationaddress", "locationaddress"}
# "Assignment County" is the shift's county (e.g. "Fulton"); the export's plain "County" column is
# the volunteer's own ("ga-fulton"), which is NOT what the badge-pickup lookup keys on.
_LIST_COUNTY_HEADERS = {"assignmentcounty"}
_LIST_EMAIL_HEADERS = {"email", "emailaddress"}
_LIST_FIRSTNAME_HEADERS = {"firstname"}


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return re.sub(r"\s+", " ", str(value)).strip()


def read_reminder_list() -> list:
    """
    The reminder-list tab as a list of shift records (one per VAN ID + date + start time):
    {van_id, date, start (hour, minute), start_text, end_text, county, location, address, email,
    first_name}. CHANGED 2026-10-12: the list is now the source of truth for the shift itself
    (time, location, address, county) and for the contact email/first name -- BigQuery is only a
    fallback for blank fields -- so a shift changed after the last ingest still gets the right
    reminder. Optional columns that are absent just come back blank.
    Raises (-> the run stops, nothing is sent) if the tab can't be read, has no header, lacks the
    VAN ID / Date / Start Time columns, or has data rows but not one valid one.
    """
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=REMINDER_LIST_SPREADSHEET_ID, range=f"'{REMINDER_LIST_TAB}'",
    ).execute()
    rows = resp.get("values", [])
    if not rows:
        raise RuntimeError(
            f"Reminder list tab '{REMINDER_LIST_TAB}' is empty -- import the day's file (needs VAN ID, "
            f"Date, Start Time columns). Stopping so nothing is sent."
        )
    header = [_norm_header(h) for h in rows[0]]
    van_i = _find_col(header, _LIST_VAN_HEADERS)
    date_i = _find_col(header, _LIST_DATE_HEADERS)
    time_i = _find_col(header, _LIST_TIME_HEADERS)
    if van_i is None or date_i is None or time_i is None:
        raise RuntimeError(
            f"Reminder list tab '{REMINDER_LIST_TAB}' must have 'VAN ID', 'Date' and 'Start Time' columns "
            f"(found headers: {rows[0]}). Stopping so nothing is sent."
        )
    optional = {
        "end": _find_col(header, _LIST_END_HEADERS),
        "location": _find_col(header, _LIST_LOCATION_HEADERS),
        "address": _find_col(header, _LIST_ADDRESS_HEADERS),
        "county": _find_col(header, _LIST_COUNTY_HEADERS),
        "email": _find_col(header, _LIST_EMAIL_HEADERS),
        "first_name": _find_col(header, _LIST_FIRSTNAME_HEADERS),
    }

    def cell(row, key):
        i = optional[key]
        return _clean_text(row[i]) if i is not None else ""

    records, seen, data_rows, skipped = [], set(), 0, 0
    for n, row in enumerate(rows[1:], start=2):
        row = list(row) + [""] * (len(header) - len(row))
        if not any(str(c).strip() for c in row):
            continue
        data_rows += 1
        van_id = _norm_van_id(row[van_i])
        list_date = _parse_list_date(str(row[date_i]))
        list_time = _parse_list_time(str(row[time_i]))
        if not van_id or list_date is None or list_time is None:
            skipped += 1
            logger.warning(f"Reminder list row {n}: VAN ID / Date / Start Time not all readable "
                           f"(van_id={van_id or '?'}) -- NOT emailed; fix the row if they should be")
            continue
        key = (van_id, list_date, list_time)
        if key in seen:
            continue  # the same shift listed twice in the file
        seen.add(key)
        start_text = re.split(r"\s*(?:-|\u2013|\u2014|\bto\b)\s*", _clean_text(row[time_i]), maxsplit=1)[0].strip()
        records.append({
            "van_id": van_id, "date": list_date, "start": list_time, "start_text": start_text,
            "end_text": cell(row, "end"), "county": cell(row, "county"), "location": cell(row, "location"),
            "address": cell(row, "address"), "email": cell(row, "email"), "first_name": cell(row, "first_name"),
        })
    if data_rows and not records:
        raise RuntimeError(f"Reminder list tab '{REMINDER_LIST_TAB}' has {data_rows} row(s) but none are "
                           f"readable (VAN ID / Date / Start Time). Stopping so nothing is sent.")
    logger.info(f"Reminder list: {len(records)} valid row(s) read from '{REMINDER_LIST_TAB}' "
                f"({skipped} skipped as unreadable)")
    return records


# --- bigquery: shifts due for reminder -----------------------------------

def get_shifts_due(project_id: str) -> pd.DataFrame:
    """
    All shifts from pw_credential_shifts joined back to pw_credential_status
    for volunteer contact info (van_id -> first_name/email), filtered in
    Python to those starting within REMINDER_OFFSET_HOURS. Filtering in
    Python rather than SQL since event_date/start_time are unparsed raw
    strings in BigQuery -- see parse_shift_datetimes.

    Contact info join uses the same "first row wins" dedup as Stage 2's
    get_eligible_volunteers, for the same reason (pw_credential_status is
    van_id+county grain, contact fields assumed identical across a
    volunteer's rows).
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT s.van_id, s.county, s.event_date, s.start_time, s.end_time,
               s.location, s.location_address,
               t.first_name, t.email
        FROM `{project_id}.{SHIFTS_TABLE}` s
        LEFT JOIN (
            SELECT van_id, first_name, email
            FROM `{project_id}.{STATUS_TABLE}`
            QUALIFY ROW_NUMBER() OVER (PARTITION BY van_id ORDER BY county ASC) = 1
        ) t
        ON s.van_id = t.van_id
    """
    all_shifts = client.query(query).to_dataframe()
    logger.info(f"{len(all_shifts)} total shift row(s) pulled from {SHIFTS_TABLE}")

    now = datetime.now(EASTERN)
    window_end = now + timedelta(hours=REMINDER_OFFSET_HOURS)

    parsed = []  # BigQuery shifts with a readable start: (normalized van_id, start_dt, row)
    for _, row in all_shifts.iterrows():
        start_dt, _ = parse_shift_datetimes(row["event_date"], row["start_time"], row["end_time"])
        if start_dt is None:
            continue
        parsed.append((_norm_van_id(row["van_id"]), start_dt, row))

    # Test mode ignores the list and keeps the old BigQuery window (test emails only ever go to
    # the test recipient).
    if TEST_RECIPIENT:
        due_df = pd.DataFrame([row for _, start_dt, row in parsed if now <= start_dt <= window_end])
        logger.info(f"{len(due_df)} shift(s) starting within {REMINDER_OFFSET_HOURS}h (test mode) -- due for reminder")
        return due_df

    # ADDED 2026-10-11 / CHANGED 2026-10-12 -- real runs are driven by the reminder list; read
    # BEFORE anything is selected, and a failure here stops the run. The list's own columns
    # (time, location, address, county, email, first name) are used; BigQuery only fills blanks.
    records = read_reminder_list()

    bq_by_key, bq_dates, contacts = {}, set(), {}
    for van_id, start_dt, row in parsed:
        bq_by_key[(van_id, start_dt.date(), (start_dt.hour, start_dt.minute))] = row
        bq_dates.add((van_id, start_dt.date()))
    for _, row in all_shifts.iterrows():
        van_id = _norm_van_id(row["van_id"])
        first_name, email = _clean_text(row.get("first_name")), _clean_text(row.get("email"))
        if van_id not in contacts or (email and not contacts[van_id][1]):
            contacts[van_id] = (first_name, email)

    cap_end = now + timedelta(hours=REMINDER_LIST_MAX_HOURS)
    due_rows, same, changed_time, new_assign, out_of_range, unbuildable = [], 0, [], [], 0, 0
    differs = {}  # field -> [van_id] where the list disagrees with BigQuery (list value is used)
    for rec in records:
        van_id = rec["van_id"]
        bq = bq_by_key.get((van_id, rec["date"], rec["start"]))

        def bq_val(col):
            return _clean_text(bq[col]) if bq is not None else ""

        county = rec["county"] or bq_val("county")
        location = rec["location"] or bq_val("location")
        address = rec["address"] or bq_val("location_address")
        end_text = rec["end_text"] or bq_val("end_time")
        bq_first, bq_email = contacts.get(van_id, ("", ""))
        email = rec["email"] or bq_email
        first_name = rec["first_name"] or bq_first
        event_date = rec["date"].isoformat()
        missing = [label for label, v in (("county", county), ("location", location), ("end time", end_text)) if not v]
        start_dt = None if missing else parse_shift_datetimes(event_date, rec["start_text"], end_text)[0]
        if missing or start_dt is None:
            unbuildable += 1
            logger.warning(f"Reminder list: can't build the shift for van_id={van_id} {event_date} "
                           f"(missing/unreadable: {', '.join(missing) or 'start or end time'}) -- NOT emailed")
            continue

        # How does the list compare with what BigQuery has? (for the log only)
        if bq is None:
            (changed_time if (van_id, rec["date"]) in bq_dates else new_assign).append(van_id)
        else:
            diffs = []
            if _norm_text(location) != _norm_text(bq_val("location")):
                diffs.append("location")
            if _norm_text(address) != _norm_text(bq_val("location_address")):
                diffs.append("address")
            if email and bq_email and email.lower() != bq_email.lower():
                diffs.append("email")
            for d in diffs:
                differs.setdefault(d, []).append(van_id)
            same += 0 if diffs else 1

        if start_dt < now or start_dt > cap_end:
            out_of_range += 1
            logger.info(f"On the list but outside the send range (already started, or more than "
                        f"{REMINDER_LIST_MAX_HOURS}h out) -- skipped: van_id={van_id} {event_date} {rec['start_text']}")
            continue
        due_rows.append({
            "van_id": van_id, "county": county, "event_date": event_date, "start_time": rec["start_text"],
            "end_time": end_text, "location": location, "location_address": address,
            "first_name": first_name, "email": email,
        })

    other_changed = sorted({v for vs in differs.values() for v in vs})
    logger.info(f"Reminder list: {len(records)} listed; {len(due_rows)} due now; {out_of_range} outside the send "
                f"range; {unbuildable} skipped (missing data)")
    logger.info(f"List vs BigQuery (the LIST's values are used): {same} identical; {len(changed_time)} start time "
                f"changed; {len(new_assign)} not in BigQuery (new); {len(other_changed)} with a changed "
                f"{'/'.join(sorted(differs)) or 'location/address/email'}")
    if changed_time or new_assign or other_changed:
        logger.info(f"  van_ids -- time changed: {changed_time[:15]}; new: {new_assign[:15]}; "
                    f"location/address/email changed: {other_changed[:15]}")

    due_df = pd.DataFrame(due_rows)
    logger.info(f"{len(due_df)} shift(s) on the reminder list -- due for reminder")
    return due_df


def _norm_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _dedup_key(van_id, county, event_date, location) -> tuple:
    """Whitespace/case-insensitive, so a location typed slightly differently in the list than in
    BigQuery (or in an earlier run's record) can't make an already-reminded shift look new."""
    return (str(van_id).strip(), _norm_text(county), str(event_date).strip(), _norm_text(location))


def get_already_sent(project_id: str, results_table: str) -> set:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT van_id, county, event_date, location
        FROM `{project_id}.{results_table}`
        WHERE send_status = 'success'
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{results_table} doesn't exist yet -- treating as first run")
        return set()
    return set(
        _dedup_key(row["van_id"], row["county"], row["event_date"], row["location"])
        for _, row in result.iterrows()
    )


# --- badge pickup blurbs (BRE merge sheet) ----------------------------------
# ADDED 2026-09-28, ported from pw_send_credential_emails.py -- same BRE
# tab, same three fields, same TBD fallback. See BRE_MERGE_TAB/
# BADGE_PICKUP_MODE in the config section above.

_bre_by_county = None

_BADGE_TBD = {
    "Badge Pick Up Time Blurb": (
        "Unfortunately, the county has not yet provided badge "
        "pick up instructions. We will send you detailed "
        "information as soon as we receive it. Thank you for "
        "your patience."
    ),
    "Badge Pick Up Location Blurb": "",
    "Badge Pick Up Plan Blurb": "",
}
_BADGE_KEYS = ("Badge Pick Up Time Blurb", "Badge Pick Up Location Blurb", "Badge Pick Up Plan Blurb")


def _normalize_county(name: str) -> str:
    """Match key between the shifts table's county names and the BRE sheet's
    County column (same rule pw_send_credential_emails.py / pw_county_email.py use)."""
    return re.sub(r"[\s_]+", " ", name).strip().lower()


def read_bre_merge_sheet() -> dict:
    """County (normalized) -> that county's row as {header: value}, read once per run."""
    global _bre_by_county
    if _bre_by_county is not None:
        return _bre_by_county
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{BRE_MERGE_TAB}!A:W",
    ).execute()
    rows = resp.get("values", [])
    by_county = {}
    if rows:
        header = rows[0]
        for row in rows[1:]:
            row = row + [""] * (len(header) - len(row))
            record = dict(zip(header, row))
            county = record.get("County", "").strip()
            if county:
                by_county[_normalize_county(county)] = record
    logger.info(f"Read {len(by_county)} county row(s) from BRE merge tab '{BRE_MERGE_TAB}'")
    _bre_by_county = by_county
    return by_county


_nearest_pickup_by_key = None
_pickup_tab_rows = None


def _pickup_feature_on() -> bool:
    return bool((PICKUP_SITES_TAB and PICKUP_SITES_TAB != "-") or (PICKUP_SITES_GID and PICKUP_SITES_GID != "-"))


def read_nearest_pickup_table(project_id: str) -> dict:
    """(van_id, county) -> (nearest_pickup_location, distance_miles), read once per run.
    Read-only. A missing table means "no personal assignments" (falls back to BRE text)."""
    global _nearest_pickup_by_key
    if _nearest_pickup_by_key is not None:
        return _nearest_pickup_by_key
    client = bigquery.Client(project=project_id)
    query = (f"SELECT van_id, county, nearest_pickup_location, distance_miles "
             f"FROM `{project_id}.{NEAREST_PICKUP_TABLE}`")
    try:
        df = client.query(query).to_dataframe()
    except NotFound:
        logger.warning(f"{NEAREST_PICKUP_TABLE} doesn't exist -- no personal badge pickup sites this run")
        _nearest_pickup_by_key = {}
        return _nearest_pickup_by_key
    _nearest_pickup_by_key = {
        (str(r.van_id), r.county): (r.nearest_pickup_location, r.distance_miles) for r in df.itertuples()
    }
    logger.info(f"Read {len(_nearest_pickup_by_key)} nearest-pickup-location row(s) from {NEAREST_PICKUP_TABLE}")
    return _nearest_pickup_by_key


def read_pickup_sites_tab() -> list:
    """Rows of the pickup-sites tab as {header: value} dicts, read once per run. Empty list if the
    tab can't be read (error logged; callers fall back to the BRE text)."""
    global _pickup_tab_rows
    if _pickup_tab_rows is not None:
        return _pickup_tab_rows
    out = []
    if _pickup_feature_on():
        try:
            tab = PICKUP_SITES_TAB if PICKUP_SITES_TAB and PICKUP_SITES_TAB != "-" else ""
            if not tab:
                meta = sheets_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
                title = next((s["properties"]["title"] for s in meta.get("sheets", [])
                              if str(s["properties"].get("sheetId")) == PICKUP_SITES_GID), None)
                if title is None:
                    raise ValueError(f"no tab with gid={PICKUP_SITES_GID} in spreadsheet {SPREADSHEET_ID}")
                tab = "'" + title.replace("'", "''") + "'"
            resp = sheets_service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A:H",
            ).execute()
            values = resp.get("values", [])
            if values:
                header = [str(h).strip() for h in values[0]]
                for row in values[1:]:
                    row = list(row) + [""] * (len(header) - len(row))
                    rec = {h: str(v or "").strip() for h, v in zip(header, row)}
                    if rec.get("County") and (rec.get("Pick up Location Address")
                                               or rec.get("Badge Pick Up Location Blurb")):
                        out.append(rec)
        except Exception as e:
            logger.error(f"Could not read pickup sites tab: {e} -- falling back to the BRE badge pickup text")
        logger.info(f"Read {len(out)} pickup site row(s) from the pickup-sites tab")
    _pickup_tab_rows = out
    return out


def _pickup_site_key(county: str, address: str) -> tuple:
    """(county, street number, ZIP): robust to punctuation / 'GA' / line-break differences between
    the same address typed in the tab and stored in BigQuery."""
    address = address or ""
    num = re.search(r"\b\d+\b", address)
    zips = re.findall(r"\b\d{5}\b", address)
    return (_normalize_county(county), num.group(0) if num else "", zips[-1] if zips else "")


def _blurbs_from_pickup_tab(county: str, van_id, project_id: str):
    """This volunteer's own site blurbs (same text as their credential email), or None when they
    have no assigned site / the site isn't in the tab (caller falls back and logs a warning)."""
    rows = [r for r in read_pickup_sites_tab()
            if _normalize_county(r.get("County", "")) == _normalize_county(county)]
    if not rows:
        return None
    match = read_nearest_pickup_table(project_id).get((str(van_id), county))
    if not match:
        logger.warning(f"{county}: van_id={van_id} has no assigned badge pickup site -- using the county BRE text")
        return None
    key = _pickup_site_key(county, match[0])
    for r in rows:
        if _pickup_site_key(county, r.get("Pick up Location Address", "")) == key:
            location = r.get("Badge Pick Up Location Blurb", "")
            location = (f"{location}\n(Based on your address, this is your nearest pickup "
                        f"location, about {match[1]:.1f} miles away.)").strip()
            return {
                "Badge Pick Up Time Blurb": r.get("Badge Pick Up Time Blurb", ""),
                "Badge Pick Up Location Blurb": location,
                "Badge Pick Up Plan Blurb": r.get("Badge Pick Up Plan Blurb", ""),
            }
    logger.warning(f"{county}: assigned site {match[0]!r} (van_id={van_id}) is not in the pickup tab")
    return None


def get_badge_pickup_blurbs(county: str, van_id=None, project_id: str = None) -> dict:
    """
    The three merge values the "Badge Pick Up Instructions" block needs, for
    ONE county (Stage 6 is per-shift, so there's never more than one county
    per email -- unlike Stage 5, no multi-county grouping is needed here).
    "real" mode (default): for Fulton/DeKalb, the volunteer's OWN assigned site from the
    pickup-sites tab (same as their credential email); otherwise that county's
    Time/Location/Plan blurbs from the BRE merge sheet. A county missing from the
    sheet, or with all three blank, falls back to the generic TBD text (and logs a
    warning) so every reminder still carries a badge blurb.
    """
    if BADGE_PICKUP_MODE == "tbd":
        return dict(_BADGE_TBD)
    if BADGE_PICKUP_MODE != "real":
        raise ValueError(f"badge_pickup_mode must be 'real' or 'tbd', got {BADGE_PICKUP_MODE!r}")
    if _pickup_feature_on() and county in NEAREST_PICKUP_COUNTIES and van_id is not None:
        from_tab = _blurbs_from_pickup_tab(county, van_id, project_id or BQ_PROJECT_ID)
        if from_tab:
            return from_tab
    row = read_bre_merge_sheet().get(_normalize_county(county))
    if row is None:
        logger.warning(f"{county}: not in BRE merge tab '{BRE_MERGE_TAB}' -- using TBD badge text")
        return dict(_BADGE_TBD)
    blurbs = {k: str(row.get(k, "") or "").strip() for k in _BADGE_KEYS}
    if not any(blurbs.values()):
        logger.warning(f"{county}: BRE row has no badge pickup blurbs -- using TBD badge text")
        return dict(_BADGE_TBD)
    return blurbs


# --- pdf attachment (stubbed, pending Stage 4) ---------------------------

def get_cached_credential_pdf(van_id: str, county: str):
    """
    🔴 STUBBED. Should return the raw bytes of this volunteer's
    already-generated credential PDF for re-attachment to the reminder
    email. There is currently no automated location for these PDFs --
    a teammate's credential_letters_script.ipynb runs manually and saves
    locally. Returns None until Stage 4 wires up real PDF storage
    (BigQuery object table, GCS bucket, etc.) -- callers must handle
    None by sending the reminder WITHOUT the attachment rather than
    failing the send, since a reminder without the PDF is still more
    useful than no reminder at all.
    """
    logger.warning(f"get_cached_credential_pdf is stubbed -- no PDF storage wired up yet "
                    f"(van_id={van_id}, county={county}). Sending without attachment.")
    return None


# --- sheet reading (same transposed pattern as send_training_reminders.py) --

def read_content_blocks():
    if BOLD_SOURCE == "sheet":
        blocks = {}
        try:
            resp = sheets_service.spreadsheets().get(
                spreadsheetId=SPREADSHEET_ID,
                ranges=[f"{CONTENT_BLOCKS_TAB}!A:B"],
                includeGridData=True,
                fields=("sheets(data(rowData(values(formattedValue,textFormatRuns,"
                        "effectiveFormat(textFormat(bold))))))"),
            ).execute()
            for row in resp["sheets"][0]["data"][0].get("rowData", []):
                cells = row.get("values", [])
                name = (cells[0].get("formattedValue", "") if len(cells) > 0 else "").strip()
                cell = cells[1] if len(cells) > 1 else {}
                text = cell.get("formattedValue", "")
                if not (name and text):
                    continue
                cell_bold = bool(cell.get("effectiveFormat", {}).get("textFormat", {}).get("bold", False))
                blocks[name] = _mark_bold_runs(text, cell.get("textFormatRuns", []), cell_bold)
            n_bold = sum(1 for t in blocks.values() if BOLD_MARKER_START in t)
            logger.info(f"Read {len(blocks)} content blocks from '{CONTENT_BLOCKS_TAB}' "
                        f"({n_bold} with bold formatting from the sheet)")
            return blocks
        except Exception as e:
            # Falls back to plain text AND to the hardcoded ("code") bold, so a daily run still goes
            # out formatted the way it has been, instead of with no bold at all.
            global _SHEET_BOLD
            _SHEET_BOLD = False
            logger.warning(f"Could not read content-block formatting ({e}) -- falling back to the "
                           f"hardcoded bold rules (bold_source=code) for this run")
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


def read_reminder_composition():
    import itertools
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{REMINDER_EMAIL_TAB}!B:Z",
    ).execute()
    rows = resp.get("values", [])
    transposed = list(itertools.zip_longest(*rows, fillvalue=""))

    composition = {}
    for col in transposed:
        name = col[0]
        if name == "":
            continue
        subject = col[1] if len(col) > 1 else ""
        blocks = [cell for cell in col[2:] if cell != ""]
        composition[name] = {"subject": subject, "blocks": blocks}

    logger.info(f"Read {len(composition)} templates from '{REMINDER_EMAIL_TAB}': {list(composition.keys())}")
    return composition


def apply_merge_fields(text: str, values: dict) -> str:
    def repl(m):
        return values.get(m.group(1).strip(), m.group(0))
    return re.sub(r"\*\|(.+?)\|\*", repl, text)


def build_email_from_composition(template_name, composition, content_blocks, values):
    if template_name not in composition:
        logger.warning(f"Reminder template '{template_name}' not found in '{REMINDER_EMAIL_TAB}' -- skipping")
        return None, None
    comp = composition[template_name]
    body = "\n\n".join(
        apply_merge_fields(content_blocks[b], values)
        for b in comp["blocks"]
        if b in content_blocks
    )
    missing = [b for b in comp["blocks"] if b not in content_blocks]
    if missing:
        logger.warning(f"Template '{template_name}' references missing content block(s): {missing}")
    # ADDED 2026-09-20 -- the template's Subject cell may hold a content block
    # name (looked up in Content Blocks) or literal subject text.
    subject_cell = comp["subject"]
    subject = apply_merge_fields(content_blocks.get(subject_cell.strip(), subject_cell), values)
    subject = _strip_bold_markers(subject)  # a subject line can't carry bold
    return subject, body


def apply_shift_wording_fallback(body: str, values: dict) -> str:
    """
    ADDED 2026-10-09 -- safety net for the "-2 Day Reminder" template wording. The sheet's
    block says "your Election Day assignment for Tuesday is:" with no date/time, but these
    reminders are for individual (early voting) shifts, so a volunteer with several shifts got
    identical-looking emails. This swaps that sentence for one carrying THIS email's shift
    date/time ("Shift Date/Time" is already built in reminder_merge_values). If the sheet is
    later fixed to use *|Shift Date/Time|* directly, these phrases no longer occur and this is
    a silent no-op, so it is safe to leave in. Whitespace-tolerant on purpose.
    """
    shift = values.get("Shift Date/Time")
    if not shift:
        return body
    # Skip if the sheet already prints the shift date/time itself.
    if shift in body:
        return body
    body = re.sub(r"your\s+Election\s+Day\s+assignment\s+for\s+Tuesday\s+is:",
                  lambda m: f"your poll watching shift is on {shift} at:", body)
    body = re.sub(r"your\s+poll\s+watching\s+shift\s+on\s+Tuesday\.",
                  "your upcoming poll watching shift.", body)
    # ADDED 2026-10-09 -- the VoPro lead's updated Remind Location block says "your Advanced
    # Voting assignment two days from now is:" (still no date/time). Keep her sentence
    # as-is (it stays bold via HEADER_LINES) and put this shift's real date/time on the
    # line under it, above the location.
    # CHANGED 2026-10-11 -- the sheet now says "Early Voting"; accept both spellings.
    # CHANGED 2026-10-26 -- in sheet-bold mode that whole sentence is wrapped in bold markers, so the
    # date/time goes after the closing marker (group 2) and stays OUT of the bold, as before.
    body = re.sub(r"(your\s+(?:Advanced|Early)\s+Voting\s+assignment\s+two\s+days\s+from\s+now\s+is:)"
                  + "(" + BOLD_MARKER_END + "?)",
                  lambda m: f"{m.group(1)}{m.group(2)}\n{shift}", body, count=1)
    return body


def _tidy_name(name: str) -> str:
    name = name.strip()
    return name.title() if name and (name == name.lower() or name == name.upper()) else name


def reminder_merge_values(row) -> dict:
    start_dt, end_dt = parse_shift_datetimes(row["event_date"], row["start_time"], row["end_time"])
    if start_dt:
        # Cross-platform safe -- avoids %-d/%-I (Unix-only strftime
        # flags that raise ValueError on Windows). Kept as-is here
        # since this runs fine in the real Linux Cloud Build container
        # either way, but this form works everywhere with no downside.
        def fmt(dt):
            return dt.strftime("%d").lstrip("0"), dt.strftime("%I").lstrip("0")
        start_day, start_hour = fmt(start_dt)
        _, end_hour = fmt(end_dt)
        shift_display = (
            f"{start_dt.strftime('%A, %B')} {start_day}, {start_dt.strftime('%Y')}, "
            f"{start_hour}:{start_dt.strftime('%M %p')} - {end_hour}:{end_dt.strftime('%M %p')}"
        )
    else:
        shift_display = f"{row['event_date']}, {row['start_time']} - {row['end_time']}"

    blurbs = get_badge_pickup_blurbs(row["county"], van_id=row["van_id"], project_id=BQ_PROJECT_ID)
    # ADDED 2026-10-09 -- the Credential Reminder block shows the plan blurb in bold.
    if blurbs.get("Badge Pick Up Plan Blurb"):
        blurbs["Badge Pick Up Plan Blurb"] = BOLD_MARKER_START + blurbs["Badge Pick Up Plan Blurb"] + BOLD_MARKER_END

    return {
        # CHANGED 2026-10-11 -- names entered all-lowercase / ALL-CAPS ("mark") are title-cased;
        # mixed-case names ("McDonald", "DeShawn") are left alone.
        "First Name": _tidy_name(str(row.get("first_name") or "")),
        "Election Name": "2026 General Election",
        "Election_Date": ELECTION_DATE,
        "County": str(row["county"]),
        "Shift Date/Time": shift_display,
        # FIXED 2026-09-05: was "Location"/"Location Address" -- real
        # template asks for "Polling Location"/"Polling Location
        # Address". Same data, was just the wrong key name.
        "Polling Location": str(row["location"]),
        "Polling Location Address": str(row.get("location_address") or ""),
        # "Poll Watcher Manual Link", "On Demand Training", and
        # "LBJ Walkthrough" are deliberately NOT here -- they get the
        # named-link replace treatment in send_email() below instead of
        # a raw-URL substitution.
        # ADDED 2026-09-28 -- see get_badge_pickup_blurbs() above.
        **blurbs,
    }


# --- rendering (local to_html, correct HEADER_LINES for this template) --

def _linkify(text):
    return _URL_RE.sub(r'<a href="\1">\1</a>', text)


def _bold_markers(text):
    # doubled markers (a merge value wrapped in bold that the sheet also bolds) collapse to one <b>
    text = re.sub(BOLD_MARKER_START + "{2,}", BOLD_MARKER_START, text)
    text = re.sub(BOLD_MARKER_END + "{2,}", BOLD_MARKER_END, text)
    return text.replace(BOLD_MARKER_START, "<b>").replace(BOLD_MARKER_END, "</b>")


def _bold_phrases(text, phrases):
    for phrase in phrases:
        text = text.replace(phrase, f"<b>{phrase}</b>")
    return text


def _bold_line_start(text):
    for phrase in LINE_START_BOLD:
        if text.startswith(phrase):
            return f"<b>{phrase}</b>{text[len(phrase):]}"
    return text


def to_html(body):
    """
    Per-line <br> joins + bullet-block <ul><li> detection. NOT reused
    from send_training_reminders.py -- see module docstring for why
    (that version's HEADER_LINES doesn't cover "LBJ Practice"/"LBJ",
    which this template's content blocks use).
    """
    out = []
    for block in body.split("\n\n"):
        # CHANGED 2026-10-11 -- bullet lines that follow a sentence in the same block
        # (Remind Arriving: "...ask the following:" then its bullets) now become a real
        # <ul> instead of "●" characters inside a paragraph, which is what the mockup shows.
        # A block that is only bullets, or only text, renders exactly as before.
        para, items = [], ""

        def flush_para():
            nonlocal para
            # blank-only runs (a leading/trailing "\n" next to a bullet list) are dropped,
            # as the old bullet-block path did, so they don't add an empty <p> gap
            if any(p.strip() for p in para):
                out.append("<p>" + "<br>".join(para) + "</p>")
            para = []

        def flush_items():
            nonlocal items
            if items:
                out.append("<ul>" + items + "</ul>")
                items = ""

        for l in block.split("\n"):
            if _SHEET_BOLD:
                # CHANGED 2026-10-26 (bold_source=sheet) -- bold comes from the sheet's markers,
                # which stay in the text here and become <b> in render_html(), AFTER its link
                # swaps. A line whose markers don't pair up is un-bolded rather than letting bold
                # bleed into the lines after it.
                if l.count(BOLD_MARKER_START) != l.count(BOLD_MARKER_END):
                    l = _strip_bold_markers(l)
                plain = _strip_bold_markers(l)
                if plain.lstrip().startswith(BULLET):
                    flush_para()
                    text = re.sub(r"^([\x01\x02\s]*)" + BULLET + r"\s*",
                                  lambda m: re.sub(r"\s", "", m.group(1)), l.lstrip(), count=1).strip()
                    items += "<li>" + _linkify(html_lib.escape(text)) + "</li>"
                    continue
                flush_items()
                stripped = plain.strip()
                # whole-line formats are built from the plain text so sheet bold can't nest (<b><b>)
                if stripped in HEADER_LINES:
                    esc = "<b>" + _linkify(html_lib.escape(plain)) + "</b>"
                elif stripped in BOLD_ITALIC_LINES:
                    esc = "<b><i>" + _linkify(html_lib.escape(plain)) + "</i></b>"
                elif stripped in ITALIC_LINES:
                    esc = "<i>" + _linkify(html_lib.escape(plain)) + "</i>"
                else:
                    esc = _linkify(html_lib.escape(l))
                para.append(esc)
                continue
            if l.lstrip().startswith(BULLET):
                flush_para()
                text = l.lstrip().lstrip(BULLET).strip()
                item = _bold_phrases(_linkify(html_lib.escape(text)), BULLET_BOLD_PHRASES)
                items += "<li>" + _bold_markers(item) + "</li>"
                continue
            flush_items()
            esc = _linkify(html_lib.escape(l))
            if l.strip() in HEADER_LINES:
                esc = "<b>" + esc + "</b>"
            elif l.strip() in BOLD_ITALIC_LINES:
                esc = "<b><i>" + esc + "</i></b>"
            elif l.strip() in ITALIC_LINES:
                esc = "<i>" + esc + "</i>"
            else:
                esc = _bold_line_start(_bold_phrases(esc, BOLD_PHRASES))
            para.append(_bold_markers(esc))
        flush_items()
        flush_para()
    return "".join(out)


# --- send (own send_email(), not reused -- see module docstring) --------

def send_email(service, to_email, subject, body, ics_attachment=None):
    # Last-line guard: the only function that calls Gmail. In test mode it can
    # only ever send to the test recipient, whatever the caller passed in.
    if TEST_RECIPIENT and to_email != TEST_RECIPIENT:
        raise RuntimeError(f"TEST MODE guard: refusing to send to {to_email} (only {TEST_RECIPIENT} allowed)")
    html_body = render_html(body)

    if ics_attachment is None:
        message = MIMEText(html_body, "html")
    else:
        message = MIMEMultipart()
        message.attach(MIMEText(html_body, "html"))
        ics_part = MIMEBase("text", "calendar", method="PUBLISH", name="reminder.ics")
        ics_part.set_payload(ics_attachment)
        encoders.encode_base64(ics_part)
        ics_part.add_header("Content-Disposition", "attachment", filename="reminder.ics")
        message.attach(ics_part)

    message["to"] = to_email
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()


def render_html(body):
    """CHANGED 2026-10-09 -- split out of send_email() (unchanged logic) so the
    final HTML can be previewed without sending."""
    html_body = to_html(body)

    # CHANGED 2026-10-26 (bold_source=sheet) -- the sheet's bold is still in html_body as markers,
    # so the link swaps below only add the <a>; bold() leaves it to the markers instead of adding
    # a second <b>. In code mode bold() is the original <b> wrap and nothing else changes.
    def bold(s):
        return s if _SHEET_BOLD else f"<b>{s}</b>"

    # ADDED 2026-10-11 -- per the VoPro lead's mockup, a few links are plain (underlined, not
    # bold) where the same name is a bold link in the Resources list. Done BEFORE the
    # global *|Poll Watcher FAQ|* swap below so only this one stays plain.
    if POLL_WATCHER_FAQ_LINK:
        faq_href = html_lib.escape(POLL_WATCHER_FAQ_LINK, quote=True)
        html_body = html_body.replace(
            "and *|Poll Watcher FAQ|* documents", f'and <a href="{faq_href}">Poll Watcher FAQ</a> documents', 1,
        )
        html_body = html_body.replace(
            "in your FAQ to make sure", f'in your <a href="{faq_href}">FAQ</a> to make sure', 1,
        )
    html_body = html_body.replace(
        "How to Pack for Poll Watching", f'<a href="{HOW_TO_PACK_LINK}">How to Pack for Poll Watching</a>', 1,
    )

    # Named-link replacements. NEW 2026-09-05 -- this script previously
    # had no hook to do these at all, since it called
    # send_training_reminders.py's send_email() directly as a black
    # box. Same pattern as pw_post_training_emails.py / the
    # assignment-email fix: left OUT of reminder_merge_values() on
    # purpose so the literal "*|...|*" placeholder survives
    # apply_merge_fields() untouched, then gets swapped here for a
    # real named <a> link instead of a raw pasted URL.
    # CHANGED 2026-10-09 -- Resources link names are bold in the updated block.
    html_body = html_body.replace(
        # CHANGED 2026-10-11 -- anchor text is "Poll Watcher Manual" in the approved mockup.
        "*|Poll Watcher Manual Link|*", bold(f'<a href="{POLL_WATCHER_MANUAL_LINK}">Poll Watcher Manual</a>')
    )
    html_body = html_body.replace(
        "*|On Demand Training|*", bold(f'<a href="{ON_DEMAND_TRAINING_LINK}">On Demand Training</a>')
    )
    html_body = html_body.replace(
        "*|LBJ Walkthrough|*", bold(f'<a href="{LBJ_WALKTHROUGH_LINK}">LBJ Walkthrough</a>')
    )
    html_body = html_body.replace(
        "*|Poll Watcher FAQ|*",
        bold(f'<a href="{html_lib.escape(POLL_WATCHER_FAQ_LINK, quote=True)}">Poll Watcher FAQ</a>')
        if POLL_WATCHER_FAQ_LINK else bold("Poll Watcher FAQ"),
    )
    # ADDED 2026-09-28 -- literal phrase, not a *|...|* merge field, same
    # as pw_assignment_emails.py / pw_send_credential_emails.py.
    html_body = html_body.replace(
        "My Voter Page - GA Secretary of State",
        bold(f'<a href="{MY_VOTER_PAGE_LINK}">My Voter Page - GA Secretary of State</a>'),
    )

    # ADDED 2026-10-09 -- "Preparing for Your Shift" bullets. Literal phrases (same
    # approach as the My Voter Page line above), first occurrence only so a later
    # mention of the same words stays plain text. The lbj.voting bullet needs no
    # entry here: a bare https URL is already linked by _linkify() in to_html().
    # CHANGED 2026-10-11 -- the Preparing bullets' link names are bold in the mockup.
    html_body = html_body.replace(
        "How to Pack For Poll Watching",
        bold(f'<a href="{HOW_TO_PACK_LINK}">How to Pack For Poll Watching</a>'), 1,
    )
    if not _SHEET_BOLD:
        html_body = html_body.replace(
            '<li><a href="https://lbj.voting/">https://lbj.voting/</a> - Please sign',
            '<li><b><a href="https://lbj.voting/">https://lbj.voting/</a></b> - Please sign', 1,
        )
    if _SHEET_BOLD:
        # The "LBJ Demo" / "LBJ On Demand" names are separate bold runs in the sheet, so the
        # compound phrase below is split by bold markers. Find the bullet that holds both names and
        # link each piece inside it; bold stays whatever the sheet says.
        for li in re.finditer(r"<li>.*?</li>", html_body):
            item = li.group(0)
            if "LBJ Demo" in item and "LBJ On Demand" in item:
                new = item.replace("LBJ Demo", f'<a href="{LBJ_DEMO_LINK}">LBJ Demo</a>', 1)
                new = new.replace("LBJ On Demand", f'<a href="{LBJ_ON_DEMAND_LINK}">LBJ On Demand</a>', 1)
                new = new.replace(
                    "watching this brief walkthrough demonstration",
                    f'<a href="{LBJ_DEMO_VIDEO_LINK}">watching this brief walkthrough demonstration</a>', 1,
                )
                new = re.sub(r"(self-pace\s+[\x01\x02]*)LBJ On Demand",
                             lambda m: f'{m.group(1)}<a href="{LBJ_ON_DEMAND_LINK}">LBJ On Demand</a>', new, count=1)
                html_body = html_body[:li.start()] + new + html_body[li.end():]
                break
        else:
            html_body = html_body.replace(
                "LBJ Demo", f'<a href="{LBJ_DEMO_LINK}">LBJ Demo</a>', 1,
            )
    elif "LBJ Demo and LBJ On Demand" in html_body:
        html_body = html_body.replace(
            "LBJ Demo and LBJ On Demand",
            f'<b><a href="{LBJ_DEMO_LINK}">LBJ Demo</a></b> and '
            f'<b><a href="{LBJ_ON_DEMAND_LINK}">LBJ On Demand</a></b>', 1,
        )
        html_body = html_body.replace(
            "watching this brief walkthrough demonstration",
            f'<a href="{LBJ_DEMO_VIDEO_LINK}">watching this brief walkthrough demonstration</a>', 1,
        )
        html_body = html_body.replace(
            "self-pace LBJ On Demand",
            f'self-pace <a href="{LBJ_ON_DEMAND_LINK}">LBJ On Demand</a>', 1,
        )
    else:
        html_body = html_body.replace(
            "LBJ Demo", f'<a href="{LBJ_DEMO_LINK}">LBJ Demo</a>', 1,
        )
    # Sheet bold: markers become <b> only now, after every link swap above has had plain text to match.
    # In code mode to_html() already converted them, so this is a no-op.
    return _bold_markers(html_body)


# --- main --------------------------------------------------------------

# ADDED 2026-10-11 -- results are saved in small batches DURING the send loop (not once at the end),
# so a crash / timeout part-way through a run can only lose the last (< batch size) rows. The
# results table is the only thing that stops a rerun from emailing people again -- the reminder
# list stays in its tab -- so it must never lag far behind the actual sends.
RESULTS_BATCH_SIZE = max(1, int(os.getenv("results_batch_size", "10")))
RESULTS_SCHEMA = [
    {"name": "van_id", "type": "STRING"},
    {"name": "county", "type": "STRING"},
    {"name": "event_date", "type": "STRING"},
    {"name": "location", "type": "STRING"},
    {"name": "send_status", "type": "STRING"},
    {"name": "error_reason", "type": "STRING"},
    {"name": "run_id", "type": "STRING"},
    {"name": "run_datetime", "type": "TIMESTAMP"},
]


def parse_status_recipients(raw: str) -> list:
    """Allow-list parse: only @example.org addresses are ever used."""
    out = []
    for addr in re.split(r"[,;\s]+", raw or ""):
        addr = addr.strip().lower()
        if not addr:
            continue
        if addr.endswith("@example.org") and addr not in out:
            out.append(addr)
        else:
            logger.warning(f"status_recipients: ignoring {addr!r} (not an @example.org address)")
    return out


def send_status_email(service, recipients: list) -> None:
    """One-line success notice. Never raises -- the reminders are what matter, not this email."""
    if not recipients:
        return
    try:
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        message = MIMEText(
            f"Today's assignment list was received and the Credential Reminder portal job completed successfully @ {stamp}.\n\n"
            "This is an automated message. No action is required.", "plain")
        message["to"] = ", ".join(recipients)
        message["subject"] = "Credential Reminder"
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        logger.info(f"Status email sent to {len(recipients)} recipient(s)")
    except Exception as e:
        logger.warning(f"Could not send the status email (reminders are unaffected): {e}")


def write_results(rows: list, project_id: str, results_table: str):
    pandas_gbq.to_gbq(
        pd.DataFrame(rows), destination_table=results_table, project_id=project_id,
        if_exists="append", table_schema=RESULTS_SCHEMA, progress_bar=False,
    )


def main(project_id: str = BQ_PROJECT_ID, results_table: str = RESULTS_TABLE):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting credential reminder send -- run_id={run_id}")
    logger.info(f"CONFIG: test_recipient={TEST_RECIPIENT or '(none)'} test_van_id={TEST_VAN_ID or '(none)'} "
                f"offset_hours={REMINDER_OFFSET_HOURS} test_limit={TEST_LIMIT} dry_run={DRY_RUN} "
                f"bold_source={BOLD_SOURCE} "
                f"sender={SENDER_ADDRESS}")

    content_blocks = read_content_blocks()
    composition = read_reminder_composition()
    if not composition:
        logger.warning("No reminder templates found in the sheet -- nothing to do")
        return

    due = get_shifts_due(project_id)
    if due.empty:
        logger.info("No shifts due for reminder this run")
        return

    if TEST_RECIPIENT:
        logger.warning(f"TEST MODE: every reminder goes to {TEST_RECIPIENT}; results table NOT read or written")
        if TEST_VAN_ID:
            due = due[due["van_id"].astype(str) == TEST_VAN_ID].head(TEST_LIMIT)
        else:
            due = due.drop_duplicates("county").head(TEST_LIMIT)
        if due.empty:
            logger.warning("TEST MODE: no matching shifts found -- check test_van_id / reminder_offset_hours")
            return
        logger.warning(f"TEST MODE: {len(due)} shift(s) selected: "
                       + "; ".join(f"van_id={r.van_id} {r.county} {r.event_date}" for r in due.itertuples()))

    already_sent = set() if TEST_RECIPIENT else get_already_sent(project_id, results_table)
    gmail_service = None if DRY_RUN else get_gmail_service()
    saving = not (TEST_RECIPIENT or DRY_RUN)
    results = []   # every outcome this run (for the summary)
    pending = []   # outcomes not yet saved to BigQuery

    def record(entry):
        results.append(entry)
        pending.append(entry)

    def flush():
        """Save pending outcomes. One retry; False if they still could not be saved."""
        if not pending:
            return True
        for attempt in (1, 2):
            try:
                write_results(pending, project_id, results_table)
                logger.info(f"Saved {len(pending)} result row(s) to {results_table}")
                pending.clear()
                return True
            except Exception as e:
                logger.error(f"Could not save {len(pending)} result row(s) to {results_table} "
                             f"(attempt {attempt}/2): {e}")
        return False

    save_failed = False
    try:
        for _, row in due.iterrows():
            # Stop sending the moment results can't be saved: anything sent after that could be
            # emailed AGAIN on the next run, because the results table is the dedup record.
            if saving and len(pending) >= RESULTS_BATCH_SIZE and not flush():
                save_failed = True
                logger.error("STOPPING: results could not be saved, so no further reminders will be sent this run")
                break
            key = _dedup_key(row["van_id"], row["county"], row["event_date"], row["location"])
            if key in already_sent:
                continue

            to_email = row.get("email")
            if not to_email:
                logger.warning(f"No email on file for van_id={row['van_id']} -- skipping reminder")
                record({
                    "van_id": row["van_id"], "county": row["county"], "event_date": row["event_date"],
                    "location": row["location"], "send_status": "no_email", "error_reason": "no email on file",
                    "run_id": run_id, "run_datetime": run_datetime,
                })
                continue

            values = reminder_merge_values(row)
            subject, body = build_email_from_composition(REMINDER_TEMPLATE_NAME, composition, content_blocks, values)
            if subject is not None:
                body = apply_shift_wording_fallback(body, values)
            if subject is None:
                record({
                    "van_id": row["van_id"], "county": row["county"], "event_date": row["event_date"],
                    "location": row["location"], "send_status": "template_missing",
                    "error_reason": f"template '{REMINDER_TEMPLATE_NAME}' not found",
                    "run_id": run_id, "run_datetime": run_datetime,
                })
                continue

            start_dt, end_dt = parse_shift_datetimes(row["event_date"], row["start_time"], row["end_time"])
            ics_attachment = None
            if start_dt and end_dt:
                uid = f"pw-cred-reminder-{row['van_id']}-{row['county']}-{row['event_date']}-{row['location']}@example.org"
                try:
                    ics_attachment = build_ics_attachment(
                        uid=uid, title=f"Poll Watcher Shift - {row['county']} County",
                        begin=start_dt, end=end_dt,
                        description=f"Location: {row['location']}",
                        location=row.get("location_address") or row["location"],
                    )
                except Exception as e:
                    logger.warning(f"Could not build .ics for van_id={row['van_id']}: {e} -- sending without .ics")

            # 🔴 stubbed -- see get_cached_credential_pdf docstring
            credential_pdf = get_cached_credential_pdf(row["van_id"], row["county"])
            if credential_pdf is not None:
                # Not yet wired into send_email -- revisit send_email's
                # signature once Stage 4 has real PDFs to attach alongside
                # the .ics.
                logger.warning("credential_pdf attachment support not yet implemented in send_email -- ignoring for now")

            try:
                send_to = TEST_RECIPIENT or to_email
                send_subject = f"[TEST - real address: {to_email}] {subject}" if TEST_RECIPIENT else subject
                if DRY_RUN:
                    logger.info(f"[DRY RUN] Would send to={send_to} subject={send_subject!r} "
                                f"(van_id={row['van_id']}, county={row['county']}, shift={row['event_date']})")
                    record({
                        "van_id": row["van_id"], "county": row["county"], "event_date": row["event_date"],
                        "location": row["location"], "send_status": "dry_run_ok", "error_reason": None,
                        "run_id": run_id, "run_datetime": run_datetime,
                    })
                    continue
                send_email(gmail_service, send_to, send_subject, body, ics_attachment=ics_attachment)
                logger.info(f"Sent reminder to {send_to} (van_id={row['van_id']}, county={row['county']}, "
                            f"shift={row['event_date']})")
                record({
                    "van_id": row["van_id"], "county": row["county"], "event_date": row["event_date"],
                    "location": row["location"], "send_status": "success", "error_reason": None,
                    "run_id": run_id, "run_datetime": run_datetime,
                })
            except Exception as e:
                logger.error(f"Failed to send reminder to {to_email} (van_id={row['van_id']}): {e}")
                record({
                    "van_id": row["van_id"], "county": row["county"], "event_date": row["event_date"],
                    "location": row["location"], "send_status": "send_error", "error_reason": str(e),
                    "run_id": run_id, "run_datetime": run_datetime,
                })

    finally:
        if saving and not flush():
            save_failed = True

    if save_failed:
        logger.error(f"{len(pending)} result row(s) are NOT saved to {results_table}. Do NOT rerun this job "
                     f"until that is fixed -- people already emailed this run would be emailed again.")
        raise SystemExit(1)

    if TEST_RECIPIENT or DRY_RUN:
        logger.info(f"{'DRY RUN' if DRY_RUN else 'TEST MODE'}: {len(results)} reminder(s) processed; "
                    f"results table not written")
        if DRY_RUN and results and all(r["send_status"] == "dry_run_ok" for r in results):
            recipients = parse_status_recipients(STATUS_RECIPIENTS_RAW)
            logger.info("[DRY RUN] Would send the status email to: "
                        + (", ".join(recipients) if recipients else "(nobody -- status_recipients is empty or has no valid addresses)"))
        return

    if not results:
        logger.info("Nothing new to send this run (all due shifts already reminded)")
        return

    results_df = pd.DataFrame(results)

    success_count = (results_df["send_status"] == "success").sum()
    logger.info("=" * 60)
    logger.info("  CREDENTIAL REMINDER RUN COMPLETE")
    logger.info(f"  Run ID:  {run_id}")
    logger.info(f"  Sent:    {success_count}")
    logger.info(f"  Failed:  {len(results_df) - success_count}")
    logger.info("=" * 60)

    # Reached only on a real run (not test/dry-run) whose results were saved. Success email only
    # when something actually went out and nothing failed; otherwise stay silent.
    if success_count >= 1 and success_count == len(results_df):
        send_status_email(gmail_service, parse_status_recipients(STATUS_RECIPIENTS_RAW))


if __name__ == "__main__":
    main(project_id=BQ_PROJECT_ID, results_table=RESULTS_TABLE)