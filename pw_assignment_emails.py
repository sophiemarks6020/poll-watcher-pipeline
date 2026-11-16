"""
pw_send_assignment_emails.py

Stage 2 of the poll watcher credentialing pipeline: the first email a
volunteer gets once they have a real, confirmed assignment.

Design notes:
- ONE COMBINED EMAIL PER VOLUNTEER (decision, 2026-09-03): a multi-county
  volunteer gets a single email listing every shift across every county
  they're assigned to, not one email per county. This is the opposite of
  the Stage 4/5 letter decision (separate letters per county) -- the two
  don't have to match, since a letter is a physical/PDF artifact handed
  to a specific county's coordinator, while this email goes to the
  volunteer directly and they only need to see it once.
- SOURCE TABLES: eligibility (needs_confirmation / needs_human_review)
  comes from commons.pw_credential_status, which is one row per
  (van_id, county) -- see pw_ingest_assignments.py. Shift-level content
  (date/time/location for the email body and .ics) comes from
  commons.pw_credential_shifts, a separate table with no per-row state,
  full-replaced every ingestion run. This script joins the two: dedupe
  pw_credential_status to one row per van_id for contact info/gating,
  then pull ALL of that van_id's rows from pw_credential_shifts for
  content, regardless of county.
- HUMAN REVIEW GATE (CHANGED 2026-09-17): multi-county volunteers
  (needs_human_review = TRUE) used to be excluded from the eligible-to-
  send query entirely, staying excluded until someone cleared the flag
  by van_id. They're now INCLUDED in that same query, but routed
  differently in main(): first a real Gmail draft is attempted (needs
  gmail.compose, confirmed NOT authorized for this service account as
  of 2026-09-12), and if that fails, the rendered email is actually SENT
  (gmail.send, confirmed working) to REVIEW_EMAIL_ADDRESS
  (assignments@example.org) instead of the volunteer, subject
  prefixed "[REVIEW NEEDED]". Neither path flips needs_confirmation --
  it stays TRUE so the *next* run, after someone manually clears
  needs_human_review, finds this volunteer again and sends the real
  email through the normal path, which does flip it. The manual
  "clear needs_human_review by van_id" step is unchanged from before.
  is_multi_county is still pulled and logged to the results table for
  audit visibility either way.
- DEDUPE BY VAN_ID: since pw_credential_status is (van_id, county)
  grain, a naive "everyone with needs_confirmation = TRUE" query would
  return multiple rows for the same multi-county volunteer. This script
  dedupes to one row per van_id using QUALIFY ROW_NUMBER() (arbitrary
  but deterministic tiebreak -- picks the alphabetically-first county's
  row for contact info, since contact fields are assumed identical
  across a volunteer's rows per pw_ingest_assignments.py's own
  assumption, not yet verified against real data).
- TEMPLATE: SINGLE TEMPLATE ONLY (CHANGED 2026-09-06, mirrors the same
  decision made for Stage 5). The sheet's "Multiple Assignment
  Confirmation Email" composition is no longer read by this script --
  every volunteer gets "Single Assignment Confirmation Email",
  regardless of shift or county count. the VoPro lead will update that
  content block later to phrase things as "shift(s)"/plural-aware.
  MULTI_SHIFT_TEMPLATE_NAME has been removed since nothing reads it.
- POLLING LOCATION IS LOOP-SAFE (CHANGED 2026-09-06): since there's no
  longer a separate multi-shift template to fall back on Shift List
  for, Polling Location is now built as a bullet list for anyone with
  more than one shift instead of being left blank. A true single-shift
  volunteer still gets exactly one line -- no visual change for the
  common case. See build_polling_location_value().
- ON SUCCESSFUL SEND: needs_confirmation is flipped to FALSE for ALL of
  that van_id's rows in pw_credential_status (every county), not just
  one -- a combined email confirms everything at once. Written via
  UPDATE ... WHERE van_id = X, not a MERGE, since this is a simple
  state flip after a real-world side effect (the email going out),
  not a data-sync operation.
- RETRY BEHAVIOR: no separate "already sent" dedup table needed --
  needs_confirmation itself IS the dedup. A failed send leaves it TRUE,
  so the volunteer is naturally picked up again next run. A results/
  audit table is still written for visibility into failures, following
  the same run_id/run_datetime/send_status/error_reason shape used by
  send_training_reminders.py.
- SWAPPABLE SEND LAYER: send_email() wraps the Gmail API the same way
  send_training_reminders.py does. If a different provider is needed
  later, that's the one function to replace.

FIXES PORTED 2026-09-05 (found + verified via pw_send_assignment_email_preview.py):
- RENDERER: send_email() used to do
  "<br><br>".join(body.split("\n\n")), which only breaks at
  double-newline paragraph boundaries and silently swallows single
  newlines -- headers ran into their paragraph text, multi-line
  bullet lists collapsed into one run-on sentence. Replaced with a
  local to_html() (per-line <br>, bullet-block <ul><li> detection),
  same pattern as pw_post_training_emails.py and
  pw_send_credential_reminders.py.
- MERGE FIELD NAMING: the real template (confirmed via screenshots)
  references "Polling Location"/"Polling Location Address" and THREE
  distinct Google Form link fields (*|Google Form Link Click Here|*,
  *|...Yes|*, *|...No|*), not one generic "Confirmation Link". Also
  needs "Credential Dates", "Poll Watcher Manual Link", and "On Demand
  Training" -- none of which existed here before.
- assignment_merge_values() now returns (values, form_links) instead
  of a single dict -- the three Google Form links need the real
  per-volunteer confirmation link baked into the <a href> at send
  time, which plain merge-field text substitution can't do. main()
  and send_email() updated to match.

CHANGED 2026-09-06:
- Single-template-only, per direction to build as if a teammate approves
  collapsing Single/Multiple into one template (Cecilia already on
  board; a teammate confirmation pending). MULTI_SHIFT_TEMPLATE_NAME removed.
- Polling Location merge value now loop-safe (see build_polling_location_value).
- is_multi_county logged on every results row (all outcomes, not just
  success) for VoPro audit visibility into the human-review gate.

OPEN, NOT YET CONFIRMED:
- event_date parsing is locked to a teammate's confirmed format ("%Y-%m-%d",
  matching fmt_date() in credential_letters_script.ipynb, known-working
  against real production LBJ exports). start_time/end_time parsing is
  still genuinely open -- a teammate's script never parses time at all, it
  only ever displays the raw strings as-is, so there's no confirmed
  evidence on time format the way there is for date. See
  parse_shift_datetimes()'s docstring for detail.
- Sheet-based template composition tab name/shape assumed to follow the
  same transposed pattern as send_training_reminders.py's reminder tab
  (row1 = template name, row2 = subject, row3+ = block list). Confirmed
  against the real sheet (Email Composition - 26 General).
- a teammate's confirmation on collapsing Single/Multiple templates -- if he
  says no, MULTI_SHIFT_TEMPLATE_NAME and the is_multi_county branch in
  main() need to come back.
- confirmation_sent_at column still needs adding to pw_credential_status's
  real schema -- flip_confirmation_sent() assumes it already exists.
"""

import base64
import json
import logging
import os
import re
import html as html_lib
import urllib.parse
import uuid
from datetime import date, datetime, timezone
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from ics import Calendar, Event
from dateutil import parser as dateutil_parser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- config -----------------------------------------------------------

BQ_PROJECT_ID = os.getenv("project", "your-gcp-project")
DATASET = "commons"

STATUS_TABLE = os.getenv("status_table", f"{DATASET}.pw_credential_status")
SHIFTS_TABLE = os.getenv("shifts_table", f"{DATASET}.pw_credential_shifts")
RESULTS_TABLE = os.getenv("results_table", f"{DATASET}.pw_assignment_email_results")

SPREADSHEET_ID = os.getenv("spreadsheet_id")
CONTENT_BLOCKS_TAB = os.getenv("content_blocks_tab", "Content Blocks")
# Confirmed against real sheet (Email Composition - 26 General):
# https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID
ASSIGNMENT_EMAIL_TAB = os.getenv("assignment_email_tab", "4 Assignment Emails")
# CHANGED 2026-09-06: single template only. MULTI_SHIFT_TEMPLATE_NAME removed.
SINGLE_SHIFT_TEMPLATE_NAME = os.getenv("single_shift_template_name", "Single Assignment Confirmation Email")

SENDER_ADDRESS = os.getenv("sender_address", "voterprotection@example.org")

# ADDED (2026-09-17) -- multi-county human-review routing. Try building a
# real Gmail draft first (needs gmail.compose, confirmed NOT authorized for
# this service account's domain-wide delegation as of 2026-09-12 -- real
# unauthorized_client error seen testing pw_county_email.py). If that
# fails, fall back to actually SENDING the rendered email (gmail.send,
# already confirmed working) to this review inbox instead of the
# volunteer, so a human can see the real content either way. Neither path
# flips needs_confirmation -- see main()'s multi-county branch.
REVIEW_EMAIL_ADDRESS = os.getenv("review_email_address", "assignment@example.org")  # TEMPORARY test default (VoPro review mailbox) -- real default is assignments@example.org

# ADDED (2026-10-02) -- DRY_RUN=true renders every email and logs who WOULD
# receive it (volunteer, or REVIEW_EMAIL_ADDRESS for multi-county), but sends
# nothing, flips no flags, and writes no results rows.
DRY_RUN = (os.getenv("DRY_RUN") or os.getenv("dry_run") or "false").lower() == "true"  # portal params are lowercase env vars

EASTERN = ZoneInfo("America/New_York")

# Static fields the real template needs -- same constants/pattern as
# pw_post_training_emails.py / pw_send_credential_reminders.py.
CREDENTIAL_DATES = os.getenv(
    "credential_dates",
    "\u25cf For Early Voting Shifts: Wednesday, October 7, 2026\n"
    "\u25cf For Election Day Shifts: Wednesday, October 28, 2026",
)
POLL_WATCHER_MANUAL_LINK = os.getenv("poll_watcher_manual_link", "https://gadems.info/PWManual")
ON_DEMAND_TRAINING_LINK = os.getenv("on_demand_training_link", "https://gadems.info/OD")
LBJ_WALKTHROUGH_LINK = os.getenv("lbj_walkthrough_link", "https://gadems.info/LBJvideo")
# ADDED 2026-09-25 -- VoPro turned the Poll Watcher FAQ in the shared "Resources"
# content block into a *|Poll Watcher FAQ|* merge field (source: the "4 Assignments"
# tab of their Data Mapping sheet). URL supplied by the user 2026-09-25 (a Drive file
# link -- must be shared "anyone with the link" or volunteers will hit an access request).
# If the setting is ever cleared, the field renders as plain "Poll Watcher FAQ" text
# (never a raw *|...|*) and a warning is logged.
POLL_WATCHER_FAQ_LINK = os.getenv(
    "poll_watcher_faq_link",
    "https://drive.google.com/file/d/YOUR_FILE_ID/view",
).strip()
# ADDED (2026-09-15) -- found missing via a real test send: "My Voter Page -
# GA Secretary of State" in the shared "Resources" content block is plain
# text, not a *|...|* merge field, so it needs a literal string replace
# rather than a merge-field substitution. pw_send_credential_emails.py
# (Stage 5) already had this exact fix for the same shared block; ported
# over here since Stage 2 was missing it.
MY_VOTER_PAGE_LINK = os.getenv("my_voter_page_link", "https://mvp.sos.ga.gov/s/")

# Confirmed 2026-09-05 via a real "Get pre-filled link" URL from the
# actual Poll Watcher Assignment Confirmation form: "Is everything
# above correct?" is entry.425267742, same entry ID for both Yes and
# No, just a different value appended.
YES_NO_ENTRY_ID = "entry.425267742"

# Local to_html() rendering constants -- see fixes note in module
# docstring. Same HEADER_LINES as pw_send_credential_reminders.py since
# both scripts pull from the same Content Blocks / "4 Assignment
# Emails" tab and share content-block structure.
BULLET = "\u25cf"
# (the URL pattern stops at the bold markers below so a marker can never land inside a link)
_URL_RE = re.compile(r"(https?://[^\s]+)")
HEADER_LINES = {
    "Credentials",
    "Resources",
    "Communications During Your Shift",
    "LBJ Practice",
    "LBJ",
    # ADDED 2026-09-25 (VoPro formatting request) -- the "Below Single" block's
    # heading line is now bold.
    "Below Please Find Your Assigned Poll Watching Location(s):",
}
# ADDED 2026-09-25 (VoPro's second formatting request, the VoPro lead's screenshot) -- bold
# INSIDE a line of the intro paragraph ("P4 Introduction M1"). Each entry is
# (text to find, the part of it to bold): the words YES and No are bolded but not
# their quote marks.
BOLD_PHRASES = [
    ("click the appropriate link to confirm", "click the appropriate link to confirm"),
    ('"YES"', "YES"),
    ('"No"', "No"),
]

# ADDED 2026-10-23 (the VoPro lead: "block was missing bold format in the test, please ensure all
# formatting matches what is in the content block") -- bold_source=sheet reads the bold VoPro
# applied inside each Content Blocks cell (same approach as pw_send_credential_emails.py) and
# the email shows exactly that bold. In that mode BOLD_PHRASES is NOT applied (the sheet is the
# source of truth); HEADER_LINES still bold a whole heading line. Default "code" = unchanged.
BOLD_SOURCE = os.getenv("bold_source", "code").strip().lower()
if BOLD_SOURCE not in ("code", "sheet"):
    raise SystemExit(f"REFUSING TO RUN: bold_source must be 'code' or 'sheet', got {BOLD_SOURCE!r}")
# Private-use markers carrying the sheet's bold through merge-field substitution; to_html()
# turns them into <b>...</b>.
BOLD_OPEN, BOLD_CLOSE = "", ""
# Merge fields and the "My Voter Page" phrase get swapped for links later, so bold is widened
# to cover them whole rather than letting a marker split one (the swap would silently fail).
_MY_VOTER_PHRASE = "My Voter Page - GA Secretary of State"
_KEEP_WHOLE_RE = re.compile(r"\*\|.+?\|\*|" + re.escape(_MY_VOTER_PHRASE))


def _strip_bold_markers(text: str) -> str:
    return text.replace(BOLD_OPEN, "").replace(BOLD_CLOSE, "")


def _bold_tags(escaped: str) -> str:
    return escaped.replace(BOLD_OPEN, "<b>").replace(BOLD_CLOSE, "</b>")


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
        out.append(BOLD_OPEN + seg + BOLD_CLOSE if flags[i] and seg.strip() else seg)
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


# CHANGED 2026-09-21 -- confirmation form links. VoPro makes a Google Form for each
# email round and provides its pre-filled link with placeholder words in it (same
# scheme as the VoPro lead's spreadsheet formula). That link goes in this setting; leave
# it unset and the three link spots in the email are simply left blank.
#   AvailGood -> Yes / No     VolFirstName / VolLastName / VolPhone / VolEmail -> the volunteer's values
# Default is the real "General 2026 - Early Voting - Poll Watcher Assignment
# Confirmation Form" link (entry IDs pulled from the form's own page source,
# 2026-09-22) -- VoPro's real form for this round, not a stand-in. Override
# with the confirmation_form_link_template job parameter for a future round's
# form, or set it to "" to blank out the link spots entirely.
FORM_LINK_TEMPLATE = os.getenv(
    "confirmation_form_link_template",
    "https://docs.google.com/forms/d/e/YOUR_FORM_ID/viewform"
    "?usp=pp_url&entry.1506133897=VolFirstName&entry.649573070=VolLastName"
    "&entry.873335829=VolEmail&entry.1037899488=VolPhone&entry.427926058=AvailGood",
).strip()
_FORM_PLACEHOLDER_RE = re.compile(r"VolFirstName|VolLastName|VolPhone|VolEmail|AvailGood")


def _clean(value) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    return str(value).strip()


def build_form_link_variants(template: str, volunteer_row) -> dict:
    """
    The real template references THREE link fields:
      *|Google Form Link Click Here|*  -- nothing pre-answered (AvailGood left blank)
      *|Google Form Link Yes|*         -- pre-answers Yes
      *|Google Form Link No|*          -- pre-answers No
    Built by swapping the placeholder words in `template` for this volunteer's
    values, URL-encoded (so spaces, apostrophes and a "+" in an email are safe).
    All placeholders are replaced in one pass, so a value can never be mistaken
    for a placeholder. Returns {} when no template is configured.
    """
    if not template:
        return {}
    values = {
        "VolFirstName": _clean(volunteer_row.get("first_name")),
        "VolLastName": _clean(volunteer_row.get("last_name")),
        "VolPhone": _clean(volunteer_row.get("phone")),
        "VolEmail": _clean(volunteer_row.get("email")),
    }

    def fill(answer: str) -> str:
        def repl(m):
            key = m.group(0)
            if key == "AvailGood":
                return answer
            return urllib.parse.quote(values[key], safe="")
        return _FORM_PLACEHOLDER_RE.sub(repl, template)

    return {
        "Google Form Link Click Here": fill(""),
        "Google Form Link Yes": fill("Yes"),
        "Google Form Link No": fill("No"),
    }


# --- bigquery: eligible volunteers + their shifts ------------------------

def get_eligible_volunteers(project_id: str) -> pd.DataFrame:
    """
    One row per van_id (deduped), for anyone who needs an assignment
    email.

    CHANGED (2026-09-17): previously excluded needs_human_review = TRUE
    (multi-county) volunteers entirely -- they now come through this
    same query too, since main() routes them to the review path (draft
    attempt, falling back to a review-inbox send) instead of skipping
    them. is_multi_county still selected to drive that branch, and still
    logged to results either way for audit visibility.

    Tiebreak on dedup is arbitrary (alphabetically-first county) but
    deterministic -- contact fields are assumed identical across a
    volunteer's (van_id, county) rows, per pw_ingest_assignments.py's
    own stated assumption. Not yet verified against real data.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT van_id, first_name, last_name, email, phone,
               confirmation_link, is_multi_county
        FROM `{project_id}.{STATUS_TABLE}`
        WHERE needs_confirmation = TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY van_id
            ORDER BY county ASC
        ) = 1
    """
    df = client.query(query).to_dataframe()
    logger.info(f"{len(df)} volunteer(s) eligible for assignment email "
                f"(needs_confirmation=TRUE)")
    return df


def get_shifts_for_volunteers(project_id: str, van_ids: list) -> pd.DataFrame:
    """
    Every shift row (across every county) for the given van_ids.
    Empty van_ids list returns an empty frame without querying.
    """
    if not van_ids:
        return pd.DataFrame(columns=[
            "van_id", "county", "event_date", "start_time", "end_time",
            "location", "location_address",
        ])

    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT van_id, county, event_date, start_time, end_time,
               location, location_address
        FROM `{project_id}.{SHIFTS_TABLE}`
        WHERE van_id IN UNNEST(@van_ids)
        ORDER BY van_id, event_date, start_time
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("van_ids", "STRING", van_ids)]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    logger.info(f"{len(df)} shift row(s) pulled for {len(van_ids)} volunteer(s)")
    return df


# --- shift parsing --------------------------------------------------------

def parse_shift_datetimes(event_date: str, start_time: str, end_time: str):
    """
    Raw LBJ export strings -> (start_dt, end_dt) in Eastern time.

    DATE: strict "%Y-%m-%d", matching a teammate's fmt_date() in
    credential_letters_script.ipynb exactly -- that's real, confirmed
    behavior since his script has actually run successfully against
    production LBJ exports.

    TIME: still dateutil's fuzzy parser, NOT confirmed against real
    data. a teammate's script never parses time at all -- it just
    concatenates the raw "Start Time"/"End Time" strings directly into
    display text (f'{start} - {end}'), since a paper letter doesn't
    need a real datetime object. That means his script gives us zero
    evidence about time format; unlike the date, this is a genuine
    open question, not something we can borrow confidence from.

    Returns (None, None) if either can't be parsed -- caller should
    handle that by skipping the .ics event for this shift rather than
    failing the whole send, and log it so it surfaces for a data-quality
    check upstream.
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
        logger.warning(f"Could not parse shift time ({start_time!r}, {end_time!r}) "
                        f"for date {event_date!r}: {e}")
        return None, None


def format_shift_display(event_date: str, start_time: str, end_time: str) -> str:
    """Human-readable shift line for the email body, e.g.
    'Tuesday, October 13, 2026, 8:00 AM - 4:00 PM'. Falls back to the
    raw strings if parsing fails, rather than dropping the shift from
    the email entirely -- better an ugly line than a silently missing
    assignment. Cross-platform safe (avoids %-d/%-I, Unix-only strftime
    flags that raise ValueError on Windows)."""
    start_dt, end_dt = parse_shift_datetimes(event_date, start_time, end_time)
    if start_dt is None:
        return f"{event_date}, {start_time} - {end_time}"

    def fmt(dt):
        return dt.strftime("%d").lstrip("0"), dt.strftime("%I").lstrip("0")

    start_day, start_hour = fmt(start_dt)
    _, end_hour = fmt(end_dt)
    date_part = f"{start_dt.strftime('%A, %B')} {start_day}, {start_dt.strftime('%Y')}"
    time_part = f"{start_hour}:{start_dt.strftime('%M %p')} - {end_hour}:{end_dt.strftime('%M %p')}"
    return f"{date_part}, {time_part}"


# --- ics --------------------------------------------------------------

def build_shifts_ics(van_id: str, shifts: pd.DataFrame):
    """
    One combined .ics with one VEVENT per shift, across all of this
    volunteer's counties. Shifts that fail to parse are skipped (logged
    in parse_shift_datetimes) rather than breaking the whole calendar
    file -- a volunteer with 3 good shifts and 1 bad one should still
    get a usable .ics for the 3 good ones.
    """
    cal = Calendar()
    for _, row in shifts.iterrows():
        start_dt, end_dt = parse_shift_datetimes(row["event_date"], row["start_time"], row["end_time"])
        if start_dt is None or end_dt is None:
            continue

        event = Event()
        # UID includes county + date + location so two shifts on the
        # same day in different counties don't collide in a calendar.
        event.uid = f"pw-cred-{van_id}-{row['county']}-{row['event_date']}-{row['location']}@example.org"
        event.name = f"Poll Watcher Shift - {row['county']} County"
        event.begin = start_dt
        event.end = end_dt
        event.location = row.get("location_address") or row.get("location") or ""
        description_parts = [f"Location: {row['location']}"]
        if row.get("location_address"):
            description_parts.append(row["location_address"])
        event.description = "\n".join(description_parts)
        cal.events.add(event)

    if not cal.events:
        return None
    return cal.serialize().encode("utf-8")


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
            n_bold = sum(1 for t in blocks.values() if BOLD_OPEN in t)
            logger.info(f"Read {len(blocks)} content blocks from '{CONTENT_BLOCKS_TAB}' "
                        f"({n_bold} with bold formatting from the sheet)")
            return blocks
        except Exception as e:
            logger.warning(f"Could not read content-block formatting ({e}) -- using plain text, "
                           f"so NO bold from the sheet this run")
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


def read_assignment_composition():
    """
    Same transposed pattern as send_training_reminders.py's
    read_reminder_composition: row1 = template name, row2 = subject,
    row3+ = ordered block names.
    """
    import itertools

    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{ASSIGNMENT_EMAIL_TAB}!B:Z",
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

    logger.info(f"Read {len(composition)} assignment templates from "
                f"'{ASSIGNMENT_EMAIL_TAB}': {list(composition.keys())}")
    return composition


def apply_merge_fields(text: str, values: dict) -> str:
    def repl(m):
        return values.get(m.group(1).strip(), m.group(0))
    return re.sub(r"\*\|(.+?)\|\*", repl, text)


def build_email_from_composition(template_name, composition, content_blocks, values):
    if template_name not in composition:
        logger.warning(f"Assignment template '{template_name}' not found in "
                        f"'{ASSIGNMENT_EMAIL_TAB}' -- skipping this send")
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


def build_shift_list_text(shifts: pd.DataFrame) -> str:
    """
    Bullet-style plain text list of every shift, grouped implicitly by
    the ORDER BY in get_shifts_for_volunteers (van_id, event_date,
    start_time) -- shifts appear chronologically, county included on
    each line since a multi-county volunteer needs to know which is
    which.
    """
    lines = []
    for _, row in shifts.iterrows():
        display = format_shift_display(row["event_date"], row["start_time"], row["end_time"])
        lines.append(f"\u25cf {row['county']} County \u2014 {display} \u2014 {row['location']}")
    return "\n".join(lines)


def build_polling_location_value(shifts: pd.DataFrame) -> str:
    """
    ADDED 2026-09-06 alongside the single-template-only change. Builds
    the merge value substituted into the sheet's literal
    "*|Polling Location|*" placeholder in the (now only) Single
    Assignment block. Loop-safe from the start rather than waiting on
    the sheet content to be edited to support more than one shift:

    - Exactly one shift -> one line, matching what the block looks like
      today -- no visual change for the common case.
    - More than one shift (same county, multiple shifts, OR multiple
      counties) -> one bulleted line per shift, county included on each
      line so a multi-county volunteer can tell shifts apart.

    Same pattern as build_polling_location_value() in
    pw_send_credential_emails.py (Stage 5).
    """
    if len(shifts) == 1:
        row = shifts.iloc[0]
        display = format_shift_display(row["event_date"], row["start_time"], row["end_time"])
        # CHANGED 2026-09-20 -- county now shown on single-shift emails too.
        return f"{row['county']} County \u2014 {row['location']} ({row['location_address']}) \u2014 {display}"

    lines = []
    for _, row in shifts.iterrows():
        display = format_shift_display(row["event_date"], row["start_time"], row["end_time"])
        lines.append(
            f"{BULLET} {row['county']} County \u2014 {row['location']} "
            f"({row['location_address']}) \u2014 {display}"
        )
    return "\n".join(lines)


def assignment_merge_values(volunteer_row, shifts: pd.DataFrame):
    """
    Returns (values, form_links) -- NOT a single dict. The three
    Google Form links need the real per-volunteer confirmation link
    baked into the <a href> at send time, which apply_merge_fields
    (plain text substitution) can't do, so they're returned separately
    for send_email() to swap in.

    CHANGED 2026-09-06: Polling Location now built via
    build_polling_location_value() for every volunteer, single-shift or
    not -- previously left blank for multi-shift volunteers since the
    (now-removed) multi-shift template used Shift List instead.
    """
    counties = sorted(shifts["county"].unique().tolist())

    values = {
        "First Name": str(volunteer_row.get("first_name") or ""),
        "Election Name": "2026 General Election",
        # ADDED (2026-09-15) -- found missing via a real test send: these
        # three showed up unresolved (*|OnDuring|* etc.) in the actual
        # email body. Same static values already used for the identical
        # sentence in pw_send_credential_emails.py's credential_merge_values()
        # -- copied from there for consistency, not guessed.
        "OnDuring": "during",
        "Election_Type": "Early Voting",
        "Election_Date": "November 3rd,",
        "Shift List": build_shift_list_text(shifts),
        "County List": ", ".join(counties),
        "Shift Count": str(len(shifts)),
        "Credential Dates": CREDENTIAL_DATES,
        "Polling Location": build_polling_location_value(shifts),
        "Polling Location Address": "",  # folded into Polling Location -- see build_polling_location_value()
    }
    # NOTE: "Poll Watcher Manual Link", "On Demand Training", and the
    # three Google Form Link fields are deliberately NOT added here --
    # they get the named-link replace treatment in send_email() instead
    # of a raw-URL substitution. Leaving them out of this dict means
    # apply_merge_fields() finds no matching key and leaves the literal
    # "*|...|*" placeholder text untouched in the body.

    form_links = build_form_link_variants(FORM_LINK_TEMPLATE, volunteer_row)

    return values, form_links


# --- rendering (local to_html, correct HEADER_LINES for this template) --

def _linkify(text):
    return _URL_RE.sub(r'<a href="\1">\1</a>', text)


def to_html(body):
    """
    Per-line <br> joins + bullet-block <ul><li> detection. FIXED
    2026-09-05 -- was "<br><br>".join(body.split("\n\n")), which only
    breaks at double-newline paragraph boundaries and silently
    swallows single newlines (a bare \n is invisible in HTML). Same
    pattern as pw_post_training_emails.py / pw_send_credential_reminders.py.
    """
    out = []
    for block in body.split("\n\n"):
        lines = block.split("\n")
        nonblank = [l for l in lines if l.strip()]
        if nonblank and all(_strip_bold_markers(l).lstrip().startswith(BULLET) for l in nonblank):
            items = ""
            for l in nonblank:
                text = l.replace(BULLET, "", 1).strip()
                items += "<li>" + _bold_tags(_linkify(html_lib.escape(text))) + "</li>"
            out.append("<ul>" + items + "</ul>")
        else:
            rendered = []
            for l in lines:
                plain = _strip_bold_markers(l)
                if plain.strip() in HEADER_LINES:
                    # whole heading bold, built from the plain text so sheet bold can't nest (<b><b>)
                    esc = "<b>" + _linkify(html_lib.escape(plain)) + "</b>"
                else:
                    esc = _bold_tags(_linkify(html_lib.escape(l)))
                    if BOLD_SOURCE == "code":
                        for found, bold in BOLD_PHRASES:
                            ef, eb = html_lib.escape(found), html_lib.escape(bold)
                            esc = esc.replace(ef, ef.replace(eb, "<b>" + eb + "</b>", 1))
                rendered.append(esc)
            out.append("<p>" + "<br>".join(rendered) + "</p>")
    return "".join(out)


# --- gmail (same pattern as send_training_reminders.py) -----------------

def get_gmail_service():
    service_account_info = json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    delegated_credentials = credentials.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated_credentials)


def get_gmail_compose_service():
    """ADDED (2026-09-17) -- separate service object, gmail.compose scope
    only, for the multi-county draft attempt. Kept separate from
    get_gmail_service() (gmail.send) rather than requesting both scopes
    on one credentials object -- domain-wide delegation authorization is
    checked per exact scope set, so mixing an authorized and an
    unauthorized scope in one request risks failing to get a token at
    all, rather than failing cleanly at the specific API call that needs
    the missing scope. Building this service is expected to succeed even
    though the actual drafts().create() call is expected to fail -- the
    scope check happens lazily, at the API call, not at token/service
    creation (confirmed via the real unauthorized_client error seen
    testing pw_county_email.py)."""
    service_account_info = json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/gmail.compose"],
    )
    delegated_credentials = credentials.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated_credentials)


def _build_assignment_message(to_email, subject, body, form_links, content_blocks, ics_attachment=None) -> str:
    """Shared MIME construction for both send_email() and
    create_assignment_draft() -- identical message either way, only the
    final API call differs. Split out (2026-09-17) when the draft path
    was added."""
    html_body = to_html(body)

    # Named-link replacements. Same pattern as pw_post_training_emails.py /
    # pw_send_credential_reminders.py: left OUT of assignment_merge_values()
    # on purpose so apply_merge_fields leaves the literal placeholder
    # untouched, then swapped here for a real named <a> link instead of
    # a raw URL.
    html_body = html_body.replace(
        "*|On Demand Training|*", f'<a href="{ON_DEMAND_TRAINING_LINK}">On Demand Training</a>'
    )
    html_body = html_body.replace(
        "*|LBJ Walkthrough|*", f'<a href="{LBJ_WALKTHROUGH_LINK}">LBJ Walkthrough</a>'
    )
    html_body = html_body.replace(
        "*|Poll Watcher Manual Link|*", f'<a href="{POLL_WATCHER_MANUAL_LINK}">Poll Watcher Manual Link</a>'
    )
    if "*|Poll Watcher FAQ|*" in html_body:
        if POLL_WATCHER_FAQ_LINK:
            html_body = html_body.replace(
                "*|Poll Watcher FAQ|*",
                f'<a href="{html_lib.escape(POLL_WATCHER_FAQ_LINK, quote=True)}">Poll Watcher FAQ</a>',
            )
        else:
            logger.warning("*|Poll Watcher FAQ|* is in the email but poll_watcher_faq_link is not set "
                           "-- rendering plain text, no link")
            html_body = html_body.replace("*|Poll Watcher FAQ|*", "Poll Watcher FAQ")
    # ADDED (2026-09-15) -- "My Voter Page - GA Secretary of State" is
    # plain text in the Resources block, not a *|...|* merge field, so it
    # needs a literal replace, not a merge-field substitution. Ported
    # from pw_send_credential_emails.py, which already had this fix.
    html_body = html_body.replace(
        "My Voter Page - GA Secretary of State",
        f'<a href="{MY_VOTER_PAGE_LINK}">My Voter Page - GA Secretary of State</a>',
    )
    # REMOVED (2026-09-13, per direction) -- the three Google Form
    # confirmation links used to render as real clickable <a> tags here.
    # Deliberately stripped to empty string instead of a link -- this is
    # a real behavior change (volunteers no longer get a way to request
    # a shift change via this email), not a testing-only swap. The
    # surrounding sentence in the real sheet's content blocks was
    # written assuming these links exist (e.g. "confirm using the link
    # below") -- that copy still needs updating separately; this only
    # removes the link itself, so the placeholder text just disappears
    # rather than showing a broken/dead link.
    # CHANGED 2026-09-21 -- the links are back, built per volunteer from the
    # confirmation_form_link_template setting (see build_form_link_variants()).
    # With no template configured, the spots stay blank as before.
    # CORRECTED 2026-09-22 -- anchor text now pulled from the real sheet's own
    # "Google Form Link Click Here/Yes/No" content blocks ("YES", "YES, THIS
    # WORKS FOR ME!", "No, I need to change or cancel.") instead of the
    # hardcoded "Click Here"/"Yes"/"No" used until now -- those three rows in
    # Content Blocks ARE the anchor text VoPro wrote, per the sheet's own Data
    # Source column ("anchor text for the URL found in the Yes_Link column").
    # CHANGED 2026-09-25 (VoPro formatting request via the VoPro lead) -- the link text is
    # fixed here in code, not read from the sheet: Yes / No are the long sentences
    # VoPro asked for, and "Click Here" stays as reviewed. (The sheet's own "Google
    # Form Link ..." blocks still hold older wording -- "YES", "YES, THIS WORKS FOR
    # ME!", "No, I need to change or cancel." -- and are deliberately not used.)
    for key, link_text in (("Google Form Link Click Here", "Click Here"),
                           ("Google Form Link Yes", "Yes! My assignment(s) works for me!"),
                           ("Google Form Link No", "No, I need to change or cancel my assignment(s).")):
        url = form_links.get(key) if form_links else None
        html_body = html_body.replace(
            f"*|{key}|*",
            f'<a href="{html_lib.escape(url, quote=True)}">{html_lib.escape(link_text)}</a>' if url else "",
        )

    if ics_attachment is None:
        message = MIMEText(html_body, "html")
    else:
        message = MIMEMultipart()
        message.attach(MIMEText(html_body, "html"))

        # Same pattern as pw_training_reminder_email.py / pw_send_credential_emails.py,
        # which show both the calendar card and the attachment in Gmail. (Reverted
        # 2026-09-20 -- an application/octet-stream variant lost the calendar card.)
        ics_part = MIMEBase("text", "calendar", method="PUBLISH", name="assignment.ics")
        ics_part.set_payload(ics_attachment)
        encoders.encode_base64(ics_part)
        ics_part.add_header("Content-Disposition", "attachment", filename="assignment.ics")
        message.attach(ics_part)

    message["to"] = to_email
    message["subject"] = subject
    return base64.urlsafe_b64encode(message.as_bytes()).decode()


def send_email(service, to_email, subject, body, form_links, content_blocks, ics_attachment=None):
    raw = _build_assignment_message(to_email, subject, body, form_links, content_blocks, ics_attachment)
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()


def create_assignment_draft(service, to_email, subject, body, form_links, content_blocks, ics_attachment=None):
    """ADDED (2026-09-17) -- multi-county review path, attempted first.
    Expected to fail with unauthorized_client until gmail.compose is
    authorized -- see get_gmail_compose_service()'s docstring. Caller
    (main()) catches the failure and falls back to send_email() addressed
    to REVIEW_EMAIL_ADDRESS instead."""
    raw = _build_assignment_message(to_email, subject, body, form_links, content_blocks, ics_attachment)
    return service.users().drafts().create(userId="me", body={"message": {"raw": raw}}).execute()


# --- state update on success ---------------------------------------------

def flip_confirmation_sent(project_id: str, van_id: str, run_datetime: str):
    """
    A combined email confirms ALL of this volunteer's counties at once,
    so every one of their pw_credential_status rows gets flipped, not
    just one. Plain UPDATE, not a MERGE -- this is a state transition
    triggered by a real side effect (the email went out), not a data
    sync from an external source.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        UPDATE `{project_id}.{STATUS_TABLE}`
        SET needs_confirmation = FALSE,
            confirmation_sent_at = @run_datetime
        WHERE van_id = @van_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_datetime", "TIMESTAMP", run_datetime),
            bigquery.ScalarQueryParameter("van_id", "STRING", van_id),
        ]
    )
    client.query(query, job_config=job_config).result()


# --- main --------------------------------------------------------------

def main(project_id: str = BQ_PROJECT_ID):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting assignment email send -- run_id={run_id}")

    content_blocks = read_content_blocks()
    composition = read_assignment_composition()
    if not composition:
        logger.warning("No assignment email templates found in the sheet -- nothing to do")
        return

    volunteers = get_eligible_volunteers(project_id)
    if volunteers.empty:
        logger.info("No volunteers eligible for assignment email this run")
        return

    van_ids = volunteers["van_id"].tolist()
    all_shifts = get_shifts_for_volunteers(project_id, van_ids)

    if DRY_RUN:
        logger.info("*** DRY RUN -- nothing will be sent, flagged, or written ***")
        gmail_service = gmail_compose_service = None
    else:
        gmail_service = get_gmail_service()
        gmail_compose_service = get_gmail_compose_service()
    results = []
    dry_run_counts = {"to_volunteer": 0, "to_review_inbox": 0, "no_email": 0}

    for _, volunteer in volunteers.iterrows():
        van_id = volunteer["van_id"]
        is_multi_county = bool(volunteer.get("is_multi_county"))
        shifts = all_shifts[all_shifts["van_id"] == van_id]

        if shifts.empty:
            logger.warning(f"No shift rows found for van_id={van_id} in {SHIFTS_TABLE} "
                            f"(needs_confirmation=TRUE but no matching shifts -- "
                            f"skipping, needs manual check)")
            results.append({
                "van_id": van_id, "email": volunteer.get("email"),
                "send_status": "no_shifts_found", "error_reason": "no matching rows in pw_credential_shifts",
                "shift_count": 0, "is_multi_county": is_multi_county,
                "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        # CHANGED 2026-09-06: single template only, no is_multi_county
        # branch on template choice (the gate on WHETHER a multi-county
        # volunteer is eligible to be here at all still lives upstream
        # in get_eligible_volunteers()'s needs_human_review filter).
        values, form_links = assignment_merge_values(volunteer, shifts)
        subject, body = build_email_from_composition(
            SINGLE_SHIFT_TEMPLATE_NAME, composition, content_blocks, values
        )
        if subject is None:
            results.append({
                "van_id": van_id, "email": volunteer.get("email"),
                "send_status": "template_missing",
                "error_reason": f"template '{SINGLE_SHIFT_TEMPLATE_NAME}' not found",
                "shift_count": len(shifts), "is_multi_county": is_multi_county,
                "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        if DRY_RUN:
            if is_multi_county:
                dest, key = REVIEW_EMAIL_ADDRESS, "to_review_inbox"
            elif volunteer.get("email"):
                dest, key = volunteer.get("email"), "to_volunteer"
            else:
                dest, key = "(NO EMAIL ON FILE -- would not send)", "no_email"
            dry_run_counts[key] += 1
            logger.info(f"[DRY RUN] van_id={van_id} {volunteer.get('first_name')} "
                        f"{volunteer.get('last_name')} -> {dest} | subject: {subject} | "
                        f"{len(shifts)} shift(s), is_multi_county={is_multi_county}")
            continue

        try:
            ics_attachment = build_shifts_ics(van_id, shifts)
        except Exception as e:
            logger.warning(f"Could not build .ics for van_id={van_id}: {e} -- sending without attachment")
            ics_attachment = None

        # ADDED (2026-09-17) -- multi-county human-review routing. Neither
        # path below flips needs_confirmation: it stays TRUE so that once
        # a human clears needs_human_review (still a manual BigQuery
        # UPDATE), the *next* run finds this volunteer again and sends
        # the real email through the normal single-county path below --
        # which is the one that actually flips needs_confirmation.
        if is_multi_county:
            try:
                create_assignment_draft(gmail_compose_service, volunteer.get("email"), subject, body,
                                         form_links, content_blocks, ics_attachment=ics_attachment)
                logger.info(f"Drafted assignment email for review (van_id={van_id}, "
                            f"would go to {volunteer.get('email')})")
                results.append({
                    "van_id": van_id, "email": volunteer.get("email"),
                    "send_status": "drafted_for_review", "error_reason": None,
                    "shift_count": len(shifts), "is_multi_county": is_multi_county,
                    "run_id": run_id, "run_datetime": run_datetime,
                })
                continue
            except Exception as e:
                logger.warning(f"Draft creation failed for van_id={van_id} ({e}) -- "
                                f"falling back to review-inbox send")

            try:
                review_subject = f"[REVIEW NEEDED] {subject}"
                send_email(gmail_service, REVIEW_EMAIL_ADDRESS, review_subject, body, form_links,
                           content_blocks, ics_attachment=ics_attachment)
                logger.info(f"Sent review copy for van_id={van_id} to {REVIEW_EMAIL_ADDRESS} "
                            f"(would go to {volunteer.get('email')})")
                results.append({
                    "van_id": van_id, "email": volunteer.get("email"),
                    "send_status": "sent_for_review", "error_reason": None,
                    "shift_count": len(shifts), "is_multi_county": is_multi_county,
                    "run_id": run_id, "run_datetime": run_datetime,
                })
            except Exception as e:
                logger.error(f"Failed to send review copy for van_id={van_id}: {e}")
                results.append({
                    "van_id": van_id, "email": volunteer.get("email"),
                    "send_status": "review_send_error", "error_reason": str(e),
                    "shift_count": len(shifts), "is_multi_county": is_multi_county,
                    "run_id": run_id, "run_datetime": run_datetime,
                })
            continue

        to_email = volunteer.get("email")
        if not to_email:
            logger.error(f"No email on file for van_id={van_id} -- cannot send")
            results.append({
                "van_id": van_id, "email": None,
                "send_status": "no_email", "error_reason": "no email on file",
                "shift_count": len(shifts), "is_multi_county": is_multi_county,
                "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        try:
            send_email(gmail_service, to_email, subject, body, form_links, content_blocks, ics_attachment=ics_attachment)
            logger.info(f"Sent assignment email to {to_email} (van_id={van_id}, "
                        f"{len(shifts)} shift(s), is_multi_county={is_multi_county}, "
                        f"template={SINGLE_SHIFT_TEMPLATE_NAME})")
            flip_confirmation_sent(project_id, van_id, run_datetime)
            results.append({
                "van_id": van_id, "email": to_email,
                "send_status": "success", "error_reason": None,
                "shift_count": len(shifts), "is_multi_county": is_multi_county,
                "run_id": run_id, "run_datetime": run_datetime,
            })
        except Exception as e:
            logger.error(f"Failed to send assignment email to {to_email} (van_id={van_id}): {e}")
            results.append({
                "van_id": van_id, "email": to_email,
                "send_status": "send_error", "error_reason": str(e),
                "shift_count": len(shifts), "is_multi_county": is_multi_county,
                "run_id": run_id, "run_datetime": run_datetime,
            })

    if DRY_RUN:
        logger.info("=" * 60)
        logger.info("  DRY RUN COMPLETE -- no emails sent, no flags changed")
        logger.info(f"  Eligible volunteers:           {len(volunteers)}")
        logger.info(f"  Would email volunteer:         {dry_run_counts['to_volunteer']}")
        logger.info(f"  Would go to review inbox:      {dry_run_counts['to_review_inbox']}")
        logger.info(f"  No email on file (skipped):    {dry_run_counts['no_email']}")
        logger.info(f"  Skipped (no shifts/template):  {len(results)}")
        logger.info("=" * 60)
        return

    results_df = pd.DataFrame(results)
    results_table_schema = [
        {"name": "van_id", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "send_status", "type": "STRING"},
        {"name": "error_reason", "type": "STRING"},
        {"name": "shift_count", "type": "INTEGER"},
        {"name": "is_multi_county", "type": "BOOLEAN"},
        {"name": "run_id", "type": "STRING"},
        {"name": "run_datetime", "type": "TIMESTAMP"},
    ]
    pandas_gbq.to_gbq(
        results_df,
        destination_table=RESULTS_TABLE,
        project_id=project_id,
        if_exists="append",
        table_schema=results_table_schema,
        progress_bar=True,
    )
    logger.info(f"Wrote {len(results_df)} result rows to {RESULTS_TABLE}")

    # Review-routed sends (sent_for_review / drafted_for_review) worked as
    # designed -- count them separately instead of as failures.
    success_count = (results_df["send_status"] == "success").sum()
    review_count = results_df["send_status"].isin(["sent_for_review", "drafted_for_review"]).sum()
    logger.info("=" * 60)
    logger.info("  ASSIGNMENT EMAIL RUN COMPLETE")
    logger.info(f"  Run ID:  {run_id}")
    logger.info(f"  Sent:    {success_count}")
    logger.info(f"  Review:  {review_count} (multi-county, sent to review inbox/draft)")
    logger.info(f"  Failed:  {len(results_df) - success_count - review_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main(project_id=BQ_PROJECT_ID)