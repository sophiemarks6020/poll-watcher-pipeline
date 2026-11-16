"""
pw_send_credential_emails.py

Stage 5 of the poll watcher credentialing pipeline: the badge/credential
letter email, sent to a volunteer once their assignment is confirmed
(Stage 3, VoPro's manual confirmation-loop review) and Stage 4's county
PDF/CSV packets have gone out.

Design decisions locked in 2026-09-06:
- ONE EMAIL PER VOLUNTEER, TOTAL (not per county). A multi-county
  volunteer gets a single email with N separate credential letter PDF
  attachments -- one real PDF per county, matching how a teammate's
  letter-generation script already produces one individual PDF per
  (volunteer, county) before stitching county-level master files for
  Stage 4. This script attaches the individual, not the stitched, PDF.
- SINGLE TEMPLATE ONLY, NO MULTI BRANCH (team-confirmed 2026-09-07).
  The sheet's "Single Assignment Credential Email" composition is used
  for every volunteer, single-shift or not.
- SHIFT/LOCATION RENDERING IS LOOP-SAFE. Builds the merge value for
  "Polling Location" as a bullet list (one line per shift, covering
  every shift across every county for this volunteer).
- DRAFTS, NOT SENDS. VoPro reviews and sends each one manually after
  confirming the per-county PDF/CSV folders from Stage 4 are correct.
  Uses Gmail's drafts().create, never messages().send.
  KNOWN BLOCKER (2026-09-09): drafts().create() currently fails with
  unauthorized_client -- the service account's domain-wide delegation
  is authorized for the gmail.send scope but NOT gmail.compose. Needs
  a Workspace admin to add https://www.googleapis.com/auth/gmail.compose
  to the authorized scopes list before this function will work at all.
- HUMAN REVIEW GATE: reuses pw_credential_status.needs_human_review.
  ASSUMPTION, NOT CONFIRMED.
- BADGE PICKUP FIELDS: real pickup timing/site data is unconfirmed for
  every county. Defaults to "tbd" via BADGE_PICKUP_MODE.

FIXED 2026-09-09 (found via TIER2 test preview send):
- credential_merge_values() Election_Type/OnDuring/Election_Date
  wording bug ("Poll Watcher on Poll Watcher", duplicated year).
- CREDENTIAL_EMAIL_TAB default corrected to "4 Assignment Emails".
- create_credential_draft() was missing the LBJ Walkthrough and My
  Voter Page named-link handling.

ADDED 2026-09-09: .ics calendar attachment. This was part of the
original design intent (per the VoPro lead's transcript notes: the same
shift should be represented as one calendar event carried across the
assignment, credential, and reminder emails) but was never actually
built into this script -- confirmed missing during Stage 5 checklist
review. Built to match pw_send_assignment_emails.py's build_shifts_ics()
exactly, including using the SAME uid scheme
("pw-cred-{van_id}-{county}-{event_date}-{location}@...") on purpose,
so a calendar app sees the same event across emails rather than a
fresh one each time -- matches the original intent even though the
"cache and update instead of duplicate" mechanism the VoPro lead described
was explicitly punted on as too complex for this build.

REQUIRES A NEW COLUMN on pw_credential_status (not yet added):
    credential_draft_created_at  TIMESTAMP, nullable

REQUIRES A NEW/CONFIRMED SOURCE for individual credential letter PDFs
per (van_id, county). a teammate's script generates these already but the
automated pipeline wiring (Stage 4 checklist item) isn't done -- see
get_credential_letter_pdf()'s docstring.

OPEN, NOT YET CONFIRMED:
- Gate condition for "eligible for credential email" (needs_credential_email
  column doesn't exist yet).
- Mechanics of VoPro's human review/sign-off step.
- The real sheet's Subject row for "Single Assignment Credential
  Email" is currently blank -- confirmed via TIER2 test. Sheet-side fix.
"""

import base64
import json
import logging
import os
import re
import html as html_lib
import urllib.parse
import uuid
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
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
RESULTS_TABLE = os.getenv("results_table", f"{DATASET}.pw_credential_email_results")
# ADDED 2026-09-21 -- who is ready for a credential email. A BigQuery view over
# the status table and this script's own results table (see get_eligible_volunteers()).
QUEUE_VIEW = os.getenv("queue_view", f"{DATASET}.pw_credential_email_queue")

SPREADSHEET_ID = os.getenv("spreadsheet_id")
CONTENT_BLOCKS_TAB = os.getenv("content_blocks_tab", "Content Blocks")
CREDENTIAL_EMAIL_TAB = os.getenv("credential_email_tab", "4 Assignment Emails")
SINGLE_CREDENTIAL_TEMPLATE_NAME = os.getenv(
    "single_credential_template_name", "Single Assignment Credential Email"
)

SENDER_ADDRESS = os.getenv("sender_address", "voterprotection@example.org")
EASTERN = ZoneInfo("America/New_York")

# ADDED 2026-09-22, same pattern as pw_county_email.py -- when true, logs what
# each volunteer's email would look like (recipient, subject, attachments,
# confirmation links) without touching Gmail or writing to the results table.
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# ADDED 2026-09-21 -- drafts are impossible (gmail.compose is not authorized), so
# this script SENDS. Single-county volunteers get their credential email
# directly. Multi-county volunteers who are not yet approved (pw_credential_email_approvals
# / the queue view's "approved" column) get a "[REVIEW NEEDED]" copy sent to this
# review inbox instead; once approved, they get the real email.
REVIEW_EMAIL_ADDRESS = os.getenv("review_email_address", "assignment@example.org")  # TEMPORARY test default -- real default is assignments@example.org

# ADDED 2026-10-07 -- TEST MODE. When test_recipient is set, EVERY email goes to that one
# address instead of the volunteer, with "[TEST - real address: ...]" in the subject, and
# the results row is recorded as "test_sent" (NOT "sent") so the volunteer stays in the
# queue and still gets their real email later. Leave unset for a real send. Point
# queue_view at a small view (a handful of volunteers) so only those are sent.
TEST_RECIPIENT = os.getenv("test_recipient", "").strip()
# ADDED 2026-10-28 -- in test mode, only the first N queued volunteers are emailed (to
# test_recipient). 0 / unset = no limit, i.e. the old behaviour. Only applies with test_recipient.
TEST_LIMIT = int(os.getenv("test_limit", "0") or 0)
# ADDED 2026-10-29 -- in test mode, email ONLY these volunteers (comma-separated VAN IDs) to
# test_recipient, e.g. test_van_id=100000002. Must be in the queue. Unset = no filter.
TEST_VAN_IDS = {v.strip() for v in os.getenv("test_van_id", "").split(",") if v.strip()}
# ADDED 2026-10-29 -- REAL sends only: email at most this many queued volunteers per run (0 / unset
# = no cap). Results are written to BigQuery only when a run ends, so a run cut off midway would
# email people without recording it; capping each run keeps every run short. Run it repeatedly --
# anyone already sent is excluded from the queue.
SEND_LIMIT = int(os.getenv("send_limit", "0") or 0)

# Until get_credential_letter_pdf() is wired to a real source it returns no PDFs. A
# real send to a volunteer without their letter attached is wrong, so by default a
# volunteer with any missing PDF is skipped (review copies are still sent, flagged).
# Set allow_missing_pdf=true ONLY for testing.
ALLOW_MISSING_PDF = os.getenv("allow_missing_pdf", "false").lower() == "true"

# ADDED 2026-09-22, CORRECTED same day -- the real sheet's "Assignment Confirmation"
# content block already uses *|Google Form Link Click Here|*/*|...Yes|*/*|...No|* --
# the SAME three merge fields Stage 2 uses, just reused here with a credential-receipt
# framing in the surrounding intro paragraph (P4 Introduction M2: "click the link that
# begins with YES to confirm receipt of your letter"). So this reuses Stage 2's exact
# field names and placeholder word (AvailGood), just with its own job setting so this
# stage's form link can be configured independently. Default is empty (link spots
# stay blank) until VoPro provides a real form for this round.
# 2026-09-25: default is now VoPro's credential-confirmation form (link + placeholders
# supplied by the VoPro lead for the Stage 5 test). Override with the
# credential_confirmation_form_link_template job parameter for a later round's form.
CREDENTIAL_FORM_LINK_TEMPLATE = os.getenv(
    "credential_confirmation_form_link_template",
    "https://docs.google.com/forms/d/e/YOUR_FORM_ID/viewform"
    "?usp=pp_url&entry.1506133897=VolFirstName&entry.649573070=VolLastName"
    "&entry.873335829=VolEmail&entry.1037899488=VolPhone&entry.427926058=AvailGood",
).strip()
_CRED_FORM_PLACEHOLDER_RE = re.compile(r"VolFirstName|VolLastName|VolPhone|VolEmail|AvailGood")


def _clean(value) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    return str(value).strip()


def build_credential_form_link_variants(template: str, volunteer_row) -> dict:
    """
    Same substitution approach as pw_assignment_emails.py's build_form_link_variants()
    -- swaps placeholder words for this volunteer's values, URL-encoded, in one pass.
    Returns {} when no template is configured (link spots render blank).
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
        return _CRED_FORM_PLACEHOLDER_RE.sub(repl, template)

    return {
        "Google Form Link Click Here": fill(""),
        "Google Form Link Yes": fill("Yes"),
        "Google Form Link No": fill("No"),
    }

POLL_WATCHER_MANUAL_LINK = os.getenv("poll_watcher_manual_link", "https://gadems.info/PWManual")
ON_DEMAND_TRAINING_LINK = os.getenv("on_demand_training_link", "https://gadems.info/OD")
LBJ_WALKTHROUGH_LINK = os.getenv("lbj_walkthrough_link", "https://gadems.info/LBJvideo")
# ADDED 2026-10-29 -- new "LBJ On Demand" bullet in the Resources EDay block (its link lives on the
# sheet cell, which this script does not read), so the phrase is linked here.
# (Re-pushed 2026-10-30 so the Cloud Source mirror picks this commit up.)
LBJ_ON_DEMAND_LINK = os.getenv("lbj_on_demand_link", "https://gadems.info/lbj-OD")
# ADDED 2026-09-25 -- the shared "Resources" block now has a *|Poll Watcher FAQ|* merge
# field (VoPro's Data Mapping sheet, "4 Assignments" tab). Same URL as pw_assignment_emails.py.
# CHANGED 2026-10-29 -- default is now the Election Day FAQ short link (the early-voting FAQ was a
# Drive file). Override with the poll_watcher_faq_link job parameter for any other round.
POLL_WATCHER_FAQ_LINK = os.getenv(
    "poll_watcher_faq_link",
    "https://gadems.info/PWFAQEDay",
).strip()
MY_VOTER_PAGE_LINK = os.getenv("my_voter_page_link", "https://mvp.sos.ga.gov/s/")

# CHANGED 2026-09-25 -- "real" is now the default: each county's badge pickup blurbs come
# from the BRE merge sheet (same tab/columns Stage 4 uses). "tbd" = the generic
# "county has not yet provided instructions" message for every county.
BADGE_PICKUP_MODE = os.getenv("badge_pickup_mode", "real")
BRE_MERGE_TAB = os.getenv("bre_merge_tab", "BRE")

# ADDED 2026-10-23 -- badge_source=eday builds the three badge blurbs from the Election Day
# columns of the BRE tab ("EDay Pick up Time" / "Election Day Badge Info" for the time,
# "EDay Pick Up Location Name"/"Address" for the location) instead of from the three
# "Badge Pick Up ... Blurb" columns, which still hold early-voting text. Default "standard"
# changes nothing. See _eday_badge_blurbs().
BADGE_SOURCE = os.getenv("badge_source", "standard").strip().lower()
if BADGE_SOURCE not in ("standard", "eday"):
    raise SystemExit(f"REFUSING TO RUN: badge_source must be 'standard' or 'eday', got {BADGE_SOURCE!r}")

# ADDED 2026-09-29 -- per-volunteer nearest-pickup-location, built from a
# separate BigQuery table (van_id, county, nearest_pickup_location,
# distance_miles) computed offline via the geolocation query, NOT built
# by this script. Currently only covers Fulton/DeKalb (the two counties
# with more than one pickup site). A volunteer/county with no row here
# (no geocoded home address, or a county outside Fulton/DeKalb) simply
# gets the existing county-level BRE blurb with no personalized line --
# this is additive, never a replacement for the BRE text.
NEAREST_PICKUP_TABLE = os.getenv("nearest_pickup_table", "commons.pw_volunteer_nearest_pickup")
# ADDED 2026-10-07 -- name of the Fulton/DeKalb pickup-sites TAB in the same spreadsheet as the
# BRE merge sheet (SPREADSHEET_ID). One row per site: County, Badge Pick Up Time/Location/Plan
# Blurb, Pick Up Location Name, Pick up Location Address, Pick up Time, Notes. When set, that
# tab is the SOURCE OF TRUTH for the pickup text of every volunteer in NEAREST_PICKUP_COUNTIES:
# a volunteer matched to a site (via NEAREST_PICKUP_TABLE) gets that site's row; one who could
# not be located gets every site in their county listed. Quote names with spaces, like
# bre_merge_tab (e.g. 'Badge Pickups'). Unset = feature off (BRE / BigQuery text as before).
PICKUP_SITES_TAB = os.getenv("pickup_sites_tab", "").strip()
# Alternative to the tab NAME: the tab's gid (the number after "gid=" in the sheet's URL). The
# name is looked up at run time. Used only when pickup_sites_tab is not set.
PICKUP_SITES_GID = os.getenv("pickup_sites_tab_gid", "").strip()
# CONFIRMED 2026-09-29 by Sophie: the nearest-pickup-location lookup is
# ONLY attempted for these counties. A volunteer's other counties always
# fall through to the plain BRE blurb, regardless of what NEAREST_PICKUP_TABLE
# happens to contain -- this is an explicit code-level guard, not just an
# artifact of the table currently only holding Fulton/DeKalb rows.
# ADDED 2026-10-23: with badge_source=eday the personal nearest-site lookup is OFF unless
# nearest_pickup_counties is set explicitly, because the assigned-site hours it carries are
# early-voting hours.
NEAREST_PICKUP_COUNTIES = set(
    c.strip() for c in os.getenv(
        "nearest_pickup_counties", "" if BADGE_SOURCE == "eday" else "Fulton,DeKalb"
    ).split(",") if c.strip()
)

# Where the individual credential letter PDFs live in Drive: the folder that contains
# one subfolder per county (i.e. the uploaded credentials_output folder). Share it with
# the service account (Viewer). Unset = no PDFs are attached.
CREDENTIALS_FOLDER_ID = os.getenv("credentials_folder_id", "").strip()

BULLET = "\u25cf"
# Private-use markers carrying bold formatting read from the sheet's content blocks
# through merge-field substitution; to_html() turns them into <b>...</b>.
BOLD_OPEN, BOLD_CLOSE = "\ue000", "\ue001"
_URL_RE = re.compile(r"(https?://[^\s\ue000\ue001]+)")
HEADER_LINES = {
    "Credentials",
    "Resources",
    "Communications During Your Shift",
    "LBJ Practice",
    "LBJ",
    "Badge and Credential Letter",
    "Badge and Credential Letter(s)",
    "BADGE PICK UP INSTRUCTIONS",
    "Below Please Find Your Assigned Poll Watching Location(s):",
}


def build_service_account_info() -> dict:
    return json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))


sa_info = build_service_account_info()

sheets_creds = service_account.Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)
sheets_service = build("sheets", "v4", credentials=sheets_creds)


# --- bigquery: eligible volunteers + their shifts ------------------------

def get_eligible_volunteers(project_id: str) -> pd.DataFrame:
    """
    One row per van_id, read from the QUEUE_VIEW (CHANGED 2026-09-21).
    The view lists volunteers who have been sent the assignment email,
    aren't held for multi-county review (needs_human_review), and have
    no successful draft/send recorded in this script's results table --
    so the results log is what stops a volunteer being handled twice.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT van_id, first_name, last_name, email, phone, is_multi_county, approved
        FROM `{project_id}.{QUEUE_VIEW}`
    """
    df = client.query(query).to_dataframe()
    logger.info(f"{len(df)} volunteer(s) eligible for credential email draft")
    return df


def get_shifts_for_volunteers(project_id: str, van_ids: list) -> pd.DataFrame:
    """Every shift row (every county) for the given van_ids."""
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
        ORDER BY van_id, county, event_date, start_time
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("van_ids", "STRING", van_ids)]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    logger.info(f"{len(df)} shift row(s) pulled for {len(van_ids)} volunteer(s)")
    return df


def _normalize_county(name: str) -> str:
    """Match key between Drive/letter folder names and the BRE sheet's County column
    (same rule pw_county_email.py uses)."""
    return re.sub(r"[\s_]+", " ", name).strip().lower()


# --- credential letter PDFs (read from Drive) -------------------------------

_drive_service = None
_drive_children_cache = {}


def _get_drive_service():
    global _drive_service
    if _drive_service is None:
        drive_creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        _drive_service = build("drive", "v3", credentials=drive_creds)
    return _drive_service


def _drive_children(folder_id: str) -> list:
    """Every non-trashed child (id, name, mimeType) of a Drive folder, cached per run."""
    if folder_id in _drive_children_cache:
        return _drive_children_cache[folder_id]
    service = _get_drive_service()
    items, token = [], None
    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=1000, pageToken=token,
            supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute()
        items.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    _drive_children_cache[folder_id] = items
    return items


def get_credential_letter_pdf(van_id: str, county: str) -> bytes | None:
    """
    CHANGED 2026-09-25 (was a stub). Returns the individual credential letter PDF
    for this (van_id, county) from the Drive folder in CREDENTIALS_FOLDER_ID, laid
    out as <folder>/<County>/<Last>_<First>_<VANID>.pdf (what credential_letters.py
    writes). The county folder is matched loosely (case/underscores) and the file by
    its trailing _<VANID>.pdf. Returns None -- and the caller flags a missing PDF --
    when the folder setting is unset or nothing matches.
    """
    if not CREDENTIALS_FOLDER_ID:
        logger.warning(
            f"credentials_folder_id is not set -- no PDF for van_id={van_id}, county={county}"
        )
        return None
    try:
        county_key = _normalize_county(county)
        is_folder = lambda f: f["mimeType"] == "application/vnd.google-apps.folder"  # noqa: E731
        root_children = _drive_children(CREDENTIALS_FOLDER_ID)
        county_folder = next(
            (f for f in root_children if is_folder(f) and _normalize_county(f["name"]) == county_key),
            None,
        )
        if county_folder is None:
            # The setting may point at the folder that CONTAINS the uploaded
            # credentials_output folder -- look one level deeper before giving up.
            for sub in (f for f in root_children if is_folder(f)):
                county_folder = next(
                    (f for f in _drive_children(sub["id"])
                     if is_folder(f) and _normalize_county(f["name"]) == county_key),
                    None,
                )
                if county_folder is not None:
                    break
        if county_folder is None:
            logger.warning(f"No '{county}' subfolder in the credentials Drive folder "
                           f"(van_id={van_id})")
            return None
        suffix = f"_{van_id}.pdf"
        match = next(
            (f for f in _drive_children(county_folder["id"])
             if f["name"].endswith(suffix) and not f["name"].startswith("all_assignments_")),
            None,
        )
        if match is None:
            logger.warning(f"No letter ending {suffix} in the {county} folder")
            return None
        return _get_drive_service().files().get_media(
            fileId=match["id"], supportsAllDrives=True
        ).execute()
    except Exception as e:
        logger.error(f"Drive lookup failed for van_id={van_id}, county={county}: {e}")
        return None


# --- nearest pickup location (per-volunteer, Fulton/DeKalb only) -----------
# ADDED 2026-09-29. This table is built OFFLINE by a separate geolocation
# query (volunteer home coordinates -> nearest pickup site, straight-line
# distance) -- this script only reads it. See NEAREST_PICKUP_TABLE above.

_nearest_pickup_by_key = None


def read_nearest_pickup_table(project_id: str) -> dict:
    """(van_id, county) -> (nearest_pickup_location, distance_miles), read once per run.
    Missing table (not built yet, or this environment has none) is treated as "no
    personalized data available" rather than an error -- every caller must still work
    with the plain BRE blurb alone."""
    global _nearest_pickup_by_key
    if _nearest_pickup_by_key is not None:
        return _nearest_pickup_by_key
    client = bigquery.Client(project=project_id)
    # CHANGED 2026-10-07: SELECT * so the optional per-site text columns
    # (time_blurb, location_blurb) are picked up when the table has them. Older
    # tables with only the four original columns still work.
    query = f"""
        SELECT *
        FROM `{project_id}.{NEAREST_PICKUP_TABLE}`
    """
    try:
        df = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{NEAREST_PICKUP_TABLE} doesn't exist -- no personalized pickup locations this run")
        _nearest_pickup_by_key = {}
        return _nearest_pickup_by_key

    def _txt(v):
        if v is None or (not isinstance(v, str) and pd.isna(v)):
            return ""
        return str(v).strip()

    by_key = {
        (str(row.van_id), row.county): (
            row.nearest_pickup_location, row.distance_miles,
            _txt(getattr(row, "time_blurb", "")), _txt(getattr(row, "location_blurb", "")),
        )
        for row in df.itertuples()
    }
    logger.info(f"Read {len(by_key)} nearest-pickup-location row(s) from {NEAREST_PICKUP_TABLE}")
    _nearest_pickup_by_key = by_key
    return by_key


def get_nearest_pickup_line(van_id, county: str, project_id: str):
    """A one-line personalized addition ('Based on your address, your nearest pickup
    location is...') for this volunteer+county, or None if there's no match -- no
    geocoded home address, or a county outside NEAREST_PICKUP_COUNTIES."""
    if county not in NEAREST_PICKUP_COUNTIES:
        return None
    match = read_nearest_pickup_table(project_id).get((str(van_id), county))
    if match is None:
        return None
    location, distance_miles = match[0], match[1]
    return (
        f"Based on your address, your nearest pickup location is: {location} "
        f"(about {distance_miles:.1f} miles away)."
    )


def get_nearest_pickup_site_text(van_id, county: str, project_id: str):
    """ADDED 2026-10-07: the matched pickup site's OWN (time_blurb, location_blurb, miles),
    or None when there's no match or the table has no per-site text. Lets the email show
    that site's real hours and pickup instructions instead of the county-level BRE text."""
    if county not in NEAREST_PICKUP_COUNTIES:
        return None
    match = read_nearest_pickup_table(project_id).get((str(van_id), county))
    if match is None or len(match) < 4:
        return None
    time_blurb, location_blurb = match[2], match[3]
    if not (time_blurb or location_blurb):
        return None
    return time_blurb, location_blurb, match[1]


# --- pickup sites TAB (same spreadsheet as the BRE merge sheet) -------------

_pickup_tab_rows = None


def read_pickup_sites_tab() -> list:
    """Rows of the pickup-sites tab as {header: value} dicts, read once per run. Empty list
    when the tab isn't configured or can't be read (an error is logged -- callers then fall
    back to the BRE / BigQuery text)."""
    global _pickup_tab_rows
    if _pickup_tab_rows is not None:
        return _pickup_tab_rows
    out = []
    if PICKUP_SITES_TAB or PICKUP_SITES_GID:
        try:
            tab = PICKUP_SITES_TAB
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
            logger.error(f"Could not read pickup sites tab {PICKUP_SITES_TAB or PICKUP_SITES_GID!r}: {e} -- "
                         f"falling back to the BRE / BigQuery pickup text")
        logger.info(f"Read {len(out)} pickup site row(s) from tab {PICKUP_SITES_TAB or 'gid ' + PICKUP_SITES_GID}")
    _pickup_tab_rows = out
    return out


def _pickup_site_key(county: str, address: str) -> tuple:
    """(county, street number, ZIP): robust to the punctuation / 'GA' / line-break differences
    between the same address typed in the tab and stored in BigQuery."""
    address = address or ""
    num = re.search(r"\b\d+\b", address)
    zips = re.findall(r"\b\d{5}\b", address)
    return (_normalize_county(county), num.group(0) if num else "", zips[-1] if zips else "")


def _blurbs_from_pickup_tab(county: str, van_id=None, project_id: str = None):
    """The three badge-pickup blurbs for ONE county built from the pickup-sites tab, or None
    when the tab has nothing for this county (caller falls back)."""
    rows = [r for r in read_pickup_sites_tab()
            if _normalize_county(r.get("County", "")) == _normalize_county(county)]
    if not rows:
        return None

    match = None
    if van_id is not None and project_id is not None:
        match = read_nearest_pickup_table(project_id).get((str(van_id), county))
    if match:
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

    # No personal match (couldn't be located, or the site isn't in the tab). Badge pickup is
    # assigned by location, so we do NOT invent text here -- main() holds these volunteers
    # back (see needs_pickup_match) until they have coordinates.
    return None


def needs_pickup_match(van_id, counties: list, project_id: str) -> list:
    """Counties (Fulton/DeKalb) where this volunteer has NO assigned pickup site yet. Empty
    when the pickup tab isn't configured, or the volunteer is fully matched. Such volunteers
    are skipped, not emailed, and stay in the queue for a later run."""
    if not (PICKUP_SITES_TAB or PICKUP_SITES_GID):
        return []
    nearest = read_nearest_pickup_table(project_id)
    return [c for c in counties
            if c in NEAREST_PICKUP_COUNTIES and (str(van_id), c) not in nearest]


# --- badge pickup blurbs (BRE merge sheet) ----------------------------------

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


def read_bre_merge_sheet() -> dict:
    """County (normalized) -> that county's row as {header: value}, read once per run."""
    global _bre_by_county
    if _bre_by_county is not None:
        return _bre_by_county
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        # the Election Day columns sit to the right of column W
        range=f"{BRE_MERGE_TAB}!A:AZ" if BADGE_SOURCE == "eday" else f"{BRE_MERGE_TAB}!A:W",
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
    logger.info(f"Read {len(by_county)} county row(s) from BRE merge tab '{BRE_MERGE_TAB}' "
                f"(badge_source={BADGE_SOURCE})")
    _bre_by_county = by_county
    return by_county


def _eday_badge_blurbs(county: str, row: dict) -> dict:
    """Election Day blurbs for ONE county row of the BRE tab (badge_source=eday).

    Time:     "Your badge is available <EDay Pick up Time>" -- or, when that is blank,
              <Election Day Badge Info>. Neither filled in = the generic TBD text (the
              county has not given Election Day times), logged as a warning.
    Location: the EDay location name + address when either is filled in; otherwise the
              county's existing Location blurb (locations are mostly unchanged from early voting).
    Plan:     the county's existing Plan blurb (generic reminder text), unchanged.
    The sheet's wording is used as typed -- fix typos/wording on the sheet, not here."""
    def cell(name: str) -> str:
        return str(row.get(name, "") or "").strip()

    time_text = cell("EDay Pick up Time") or cell("Election Day Badge Info")
    if not time_text:
        logger.warning(f"{county}: no Election Day badge time on BRE tab '{BRE_MERGE_TAB}' -- using TBD badge text")
        return dict(_BADGE_TBD)
    if time_text[-1] not in ".!?":
        time_text += "."
    name, address = cell("EDay Pick Up Location Name"), cell("EDay Pick up Location Address")
    if name or address:
        location = "Your badge pick up location is\n" + "\n".join(x for x in (name, address) if x)
    else:
        location = cell("Badge Pick Up Location Blurb")
    return {
        "Badge Pick Up Time Blurb": f"Your badge is available {time_text}",
        "Badge Pick Up Location Blurb": location,
        "Badge Pick Up Plan Blurb": cell("Badge Pick Up Plan Blurb"),
    }


def get_badge_pickup_blurbs(county: str, van_id=None, project_id: str = None) -> dict:
    """
    The three merge values the "Credentials Attached" block needs, for ONE county.
    "real" mode (default): that county's Time/Location/Plan blurbs from the BRE merge
    sheet; a county missing from the sheet, or with all three blank, falls back to the
    generic TBD text (and logs a warning) so every email still carries a badge blurb.

    ADDED 2026-09-29: with van_id/project_id given, a personalized "your nearest
    pickup location is..." line is appended to the Location blurb when one exists
    (see get_nearest_pickup_line) -- additive only, never replaces the BRE text, and
    silently does nothing when there's no match (no geocoded address, or a county
    outside Fulton/DeKalb).
    """
    if BADGE_PICKUP_MODE == "tbd":
        blurbs = dict(_BADGE_TBD)
    elif BADGE_PICKUP_MODE != "real":
        raise ValueError(f"badge_pickup_mode must be 'real' or 'tbd', got {BADGE_PICKUP_MODE!r}")
    else:
        # ADDED 2026-10-07: for Fulton/DeKalb the pickup-sites TAB is the source of truth
        # (see PICKUP_SITES_TAB). Used only when configured AND the tab has rows for this
        # county; otherwise everything below behaves exactly as before.
        if (PICKUP_SITES_TAB or PICKUP_SITES_GID) and county in NEAREST_PICKUP_COUNTIES:
            from_tab = _blurbs_from_pickup_tab(county, van_id=van_id, project_id=project_id)
            if from_tab:
                return from_tab
        row = read_bre_merge_sheet().get(_normalize_county(county))
        if row is None:
            logger.warning(f"{county}: not in BRE merge tab '{BRE_MERGE_TAB}' -- using TBD badge text")
            blurbs = dict(_BADGE_TBD)
        elif BADGE_SOURCE == "eday":
            blurbs = _eday_badge_blurbs(county, row)
        else:
            blurbs = {k: str(row.get(k, "") or "").strip() for k in _BADGE_KEYS}
            if not any(blurbs.values()):
                logger.warning(f"{county}: BRE row has no badge pickup blurbs -- using TBD badge text")
                blurbs = dict(_BADGE_TBD)

    if van_id is not None and project_id is not None:
        nearest_line = get_nearest_pickup_line(van_id, county, project_id)
        if nearest_line:
            # CHANGED 2026-09-30, per a teammate/the VoPro lead's follow-up feedback --
            # now clears the BRE sheet's Time Blurb too, not just Location.
            # The Time Blurb in BRE is written for ONE specific county
            # office address (tied to the "Pick up Time"/"Pick up Location
            # Address" columns in that same row) -- since the geolocated
            # address can be a DIFFERENT pickup site with different (unknown)
            # hours, showing BRE's hours next to it was actively misleading,
            # not just redundant. Plan Blurb is left alone -- it's a generic
            # reminder ("YOU MUST PICK UP...") with nothing location-specific
            # in it, so it stays accurate regardless of which site is shown.
            # Real per-site hours are expected to come from a separate CSV
            # once VoPro confirms them (not built yet -- see conversation
            # 2026-09-30). Only applies when a personalized match actually
            # exists; no match (ungeocoded volunteer) still falls back to
            # the full BRE content untouched.
            blurbs["Badge Pick Up Time Blurb"] = ""
            blurbs["Badge Pick Up Location Blurb"] = nearest_line
            # CHANGED 2026-10-07: when the matched site carries its own hours and
            # pickup instructions (pw_badge_pickup_sites -> nearest table), show
            # those instead -- the site's real time blurb plus its location
            # blurb with the distance, rather than the generic line + blank time.
            site = get_nearest_pickup_site_text(van_id, county, project_id)
            if site:
                time_blurb, location_blurb, miles = site
                blurbs["Badge Pick Up Time Blurb"] = time_blurb
                blurbs["Badge Pick Up Location Blurb"] = (
                    f"{location_blurb}\n(Based on your address, this is your nearest pickup "
                    f"location, about {miles:.1f} miles away.)"
                ).strip()
    return blurbs


def badge_pickup_values_for_counties(counties: list, van_id=None, project_id: str = None) -> dict:
    """
    One county: that county's blurbs, unchanged. Several counties: every county's pickup
    information under its own bold "<County> County:" label, all placed in the Time
    field (Location and Plan are left empty) so each county's letter has its badge
    pickup instructions right alongside it.
    """
    if len(counties) == 1:
        return get_badge_pickup_blurbs(counties[0], van_id=van_id, project_id=project_id)
    groups = []
    for county in counties:
        b = get_badge_pickup_blurbs(county, van_id=van_id, project_id=project_id)
        top = "\n".join(x for x in (b["Badge Pick Up Time Blurb"], b["Badge Pick Up Location Blurb"]) if x)
        group = f"{BOLD_OPEN}{county} County:{BOLD_CLOSE}" + (f"\n{top}" if top else "")
        if b["Badge Pick Up Plan Blurb"]:
            group += f"\n\n{b['Badge Pick Up Plan Blurb']}"
        groups.append(group)
    return {
        "Badge Pick Up Time Blurb": "\n\n".join(groups),
        "Badge Pick Up Location Blurb": "",
        "Badge Pick Up Plan Blurb": "",
    }


# --- shift parsing (shared by display text AND .ics) -----------------------

def parse_shift_datetimes(event_date: str, start_time: str, end_time: str):
    """
    ADDED 2026-09-09: split out of format_shift_display() so both the
    display text and build_shifts_ics() share one parsing path --
    matches pw_send_assignment_emails.py's structure exactly.

    Raw LBJ export strings -> (start_dt, end_dt) in Eastern time, or
    (None, None) on parse failure. Caller decides fallback behavior:
    format_shift_display() falls back to raw strings; build_shifts_ics()
    skips that shift's VEVENT rather than failing the whole calendar.
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
    """Human-readable shift line. Falls back to raw strings if parsing
    fails. Cross-platform safe (avoids %-d/%-I)."""
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


def build_polling_location_value(shifts: pd.DataFrame) -> str:
    """
    Loop-safe Polling Location merge value.
    - Exactly one shift -> one line.
    - More than one shift -> one bulleted line per shift, county
      included on each line.
    "Polling Location Address" is left blank -- the address is folded
    into each bullet line here instead.
    """
    if len(shifts) == 1:
        row = shifts.iloc[0]
        display = format_shift_display(row["event_date"], row["start_time"], row["end_time"])
        # CHANGED 2026-09-22 -- county now shown on single-shift emails too,
        # same fix as pw_assignment_emails.py's build_polling_location_value().
        return f"{row['county']} County \u2014 {row['location']} ({row['location_address']}) \u2014 {display}"

    lines = []
    for _, row in shifts.iterrows():
        display = format_shift_display(row["event_date"], row["start_time"], row["end_time"])
        lines.append(
            f"{BULLET} {row['county']} County \u2014 {row['location']} "
            f"({row['location_address']}) \u2014 {display}"
        )
    return "\n".join(lines)


# --- .ics ---------------------------------------------------------------

def build_shifts_ics(van_id: str, shifts: pd.DataFrame):
    """
    ADDED 2026-09-09. One combined .ics with one VEVENT per shift,
    across all of this volunteer's counties. Shifts that fail to parse
    are skipped (logged in parse_shift_datetimes) rather than breaking
    the whole calendar file.

    Uses the SAME uid scheme as pw_send_assignment_emails.py's
    build_shifts_ics() on purpose -- the VoPro lead's original design intent
    was for the same shift to be one calendar event carried across the
    assignment, credential, and reminder emails (updating, not
    duplicating), even though the caching mechanism to fully realize
    that was explicitly punted on as too complex. Matching UIDs is the
    part of that intent this build can still honor.
    """
    cal = Calendar()
    for _, row in shifts.iterrows():
        start_dt, end_dt = parse_shift_datetimes(row["event_date"], row["start_time"], row["end_time"])
        if start_dt is None or end_dt is None:
            continue

        event = Event()
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


# --- sheet reading (same transposed pattern as Stage 2/6) -----------------

# Merge fields (*|...|*) and the literal "My Voter Page" phrase get swapped for links
# later, so their text must stay unbroken. If the sheet's bold starts or stops INSIDE one
# of them, bold markers would split it and the swap would silently fail -- so the bold is
# widened to cover the whole field instead (see _mark_bold_runs).
_MY_VOTER_PHRASE = "My Voter Page - GA Secretary of State"
_KEEP_WHOLE_RE = re.compile(r"\*\|.+?\|\*|" + re.escape(_MY_VOTER_PHRASE))


def _mark_bold_runs(text: str, runs: list, cell_bold: bool) -> str:
    """
    Insert bold markers into a cell's text from the Sheets API's textFormatRuns (each run
    starts at startIndex, in UTF-16 code units, and lasts until the next run starts; a run
    without its own "bold" setting inherits the cell-level bold).

    Bold is decided per character first. Any merge field / link phrase the bold touches is
    made bold as a whole, so no marker ever lands inside one. Markers are then written one
    line at a time, so a bold stretch that spans a line break stays balanced.
    """
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


def read_content_blocks():
    """
    Content Blocks tab (A = name, B = text) as {name: text}. CHANGED 2026-09-25: also reads
    the bold formatting VoPro applied inside each block's cell and keeps it as markers in the
    text, so it survives merge-field substitution and comes out as <b>...</b> in the email.
    If that richer read fails for any reason, falls back to plain text (no bold from the sheet).
    """
    blocks = {}
    try:
        resp = sheets_service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID,
            ranges=[f"{CONTENT_BLOCKS_TAB}!A:B"],
            includeGridData=True,
            fields=("sheets(data(rowData(values(formattedValue,textFormatRuns,"
                    "effectiveFormat(textFormat(bold))))))"),
        ).execute()
        rows = resp["sheets"][0]["data"][0].get("rowData", [])
        for row in rows:
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
                    f"({n_bold} with bold formatting)")
        return blocks
    except Exception as e:
        logger.warning(f"Could not read content-block formatting ({e}) -- using plain text")

    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{CONTENT_BLOCKS_TAB}!A:B",
    ).execute()
    for r in resp.get("values", []):
        name = (r[0].strip() if len(r) > 0 and r[0] else "")
        content = (r[1] if len(r) > 1 and r[1] else "")
        if name and content:
            blocks[name] = content
    logger.info(f"Read {len(blocks)} content blocks from '{CONTENT_BLOCKS_TAB}' (plain text)")
    return blocks


def read_credential_composition():
    """Only the single-template column is read -- per team-confirmed
    decision, the Multiple Assignment column is not consulted."""
    import itertools

    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{CREDENTIAL_EMAIL_TAB}!B:Z",
    ).execute()
    rows = resp.get("values", [])
    transposed = list(itertools.zip_longest(*rows, fillvalue=""))

    composition = {}
    for col in transposed:
        name = col[0]
        if name != SINGLE_CREDENTIAL_TEMPLATE_NAME:
            continue
        subject = col[1] if len(col) > 1 else ""
        blocks = [cell for cell in col[2:] if cell != ""]
        composition[name] = {"subject": subject, "blocks": blocks}

    logger.info(f"Read credential template '{SINGLE_CREDENTIAL_TEMPLATE_NAME}' "
                f"from '{CREDENTIAL_EMAIL_TAB}': found={bool(composition)}")
    return composition


def apply_merge_fields(text: str, values: dict) -> str:
    def repl(m):
        return values.get(m.group(1).strip(), m.group(0))
    return re.sub(r"\*\|(.+?)\|\*", repl, text)


def build_email_from_composition(composition, content_blocks, values):
    comp = composition.get(SINGLE_CREDENTIAL_TEMPLATE_NAME)
    if comp is None:
        logger.warning(f"Template '{SINGLE_CREDENTIAL_TEMPLATE_NAME}' not found in "
                        f"'{CREDENTIAL_EMAIL_TAB}' -- skipping this send")
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
    subject = _strip_bold_markers(subject)  # a subject line can't carry formatting
    return subject, body


def credential_merge_values(volunteer_row, shifts: pd.DataFrame, project_id: str = None) -> dict:
    """Election_Type/OnDuring/Election_Date fixed 2026-09-09 -- see
    module docstring."""
    counties = sorted(shifts["county"].unique().tolist())
    badge_blurbs = badge_pickup_values_for_counties(
        counties, van_id=volunteer_row.get("van_id"), project_id=project_id
    )

    values = {
        "First Name": str(volunteer_row.get("first_name") or ""),
        "Last Name": str(volunteer_row.get("last_name") or ""),
        "Election Name": "2026 General Election",
        "Election_Date": "November 3rd,",
        "Election_Type": "Early Voting",
        "OnDuring": "during",
        "Letter Date": datetime.now(EASTERN).strftime("%B %d, %Y"),
        "Polling Location": build_polling_location_value(shifts),
        "Polling Location Address": "",
        **badge_blurbs,
    }
    return values


# --- rendering (reused to_html pattern) -----------------------------------

def _linkify(text):
    return _URL_RE.sub(r'<a href="\1">\1</a>', text)


def _bold_tags(escaped: str) -> str:
    """Turn the bold markers carried from the sheet into real <b>...</b> tags."""
    return escaped.replace(BOLD_OPEN, "<b>").replace(BOLD_CLOSE, "</b>")


def _strip_bold_markers(text: str) -> str:
    return text.replace(BOLD_OPEN, "").replace(BOLD_CLOSE, "")


def to_html(body):
    """Same per-line <br> + bullet-block <ul><li> renderer as Stage 2/6, plus (2026-09-25)
    the bold formatting carried over from the sheet's content blocks."""
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
                    # A header line is bold as a whole. Built from the plain text so bold
                    # already set in the sheet doesn't nest (<b><b>), which renders extra heavy.
                    esc = "<b>" + _linkify(html_lib.escape(plain)) + "</b>"
                else:
                    esc = _bold_tags(_linkify(html_lib.escape(l)))
                rendered.append(esc)
            out.append("<p>" + "<br>".join(rendered) + "</p>")
    return "".join(out)


# --- gmail: draft creation, not send --------------------------------------

def get_gmail_service():
    """
    CHANGED 2026-09-21: uses gmail.send (authorized). The old gmail.compose
    scope is NOT authorized for domain-wide delegation (unauthorized_client,
    2026-09-09), so this script sends instead of creating drafts.
    """
    service_account_info = json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    delegated_credentials = credentials.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated_credentials)


def send_credential_email(service, to_email, subject, body, county_pdfs: dict,
                           content_blocks: dict, ics_attachment=None, form_links: dict = None):
    """
    county_pdfs: {county_name: pdf_bytes}. SENDS ONE email (CHANGED
    2026-09-21 from creating a draft) with one PDF attachment per county
    that had a real letter available, plus one .ics calendar attachment
    covering every shift (ADDED 2026-09-09) if provided.
    """
    html_body = to_html(body)
    html_body = html_body.replace(
        "*|On Demand Training|*", f'<a href="{ON_DEMAND_TRAINING_LINK}">On Demand Training</a>'
    )
    html_body = html_body.replace(
        "*|Poll Watcher Manual Link|*", f'<a href="{POLL_WATCHER_MANUAL_LINK}">Poll Watcher Manual Link</a>'
    )
    html_body = html_body.replace(
        "*|LBJ Walkthrough|*", f'<a href="{LBJ_WALKTHROUGH_LINK}">LBJ Walkthrough</a>'
    )
    html_body = html_body.replace(
        "*|Poll Watcher FAQ|*",
        f'<a href="{html_lib.escape(POLL_WATCHER_FAQ_LINK, quote=True)}">Poll Watcher FAQ</a>'
        if POLL_WATCHER_FAQ_LINK else "Poll Watcher FAQ",
    )
    html_body = html_body.replace(
        "My Voter Page - GA Secretary of State",
        f'<a href="{MY_VOTER_PAGE_LINK}">My Voter Page - GA Secretary of State</a>',
    )
    # ADDED 2026-10-29 -- the "Resources EDay" block now has the literal words "Poll Watcher EDay
    # FAQ" (its link lives on the sheet cell, which this script does not read), so the phrase is
    # linked here, same as the My Voter Page phrase above.
    html_body = html_body.replace(
        "Poll Watcher EDay FAQ",
        f'<a href="{html_lib.escape(POLL_WATCHER_FAQ_LINK, quote=True)}">Poll Watcher EDay FAQ</a>'
        if POLL_WATCHER_FAQ_LINK else "Poll Watcher EDay FAQ",
    )
    # ADDED 2026-10-29 -- the new "LBJ On Demand" bullet (bold comes from the sheet).
    html_body = html_body.replace(
        "LBJ On Demand",
        f'<a href="{html_lib.escape(LBJ_ON_DEMAND_LINK, quote=True)}">LBJ On Demand</a>'
        if LBJ_ON_DEMAND_LINK else "LBJ On Demand",
    )
    # ADDED 2026-09-22 -- reuses Stage 2's real "Google Form Link Click Here/Yes/No"
    # merge fields (the real sheet's "Assignment Confirmation" block already has them).
    # CHANGED 2026-09-25 -- link text is fixed in code, same wording as
    # pw_assignment_emails.py (VoPro's formatting request): the sheet's own "Google
    # Form Link ..." blocks are deliberately not used for the link text.
    for key, link_text in (("Google Form Link Click Here", "Click Here"),
                           ("Google Form Link Yes", "Yes! My assignment(s) works for me!"),
                           ("Google Form Link No", "No, I need to change or cancel my assignment(s).")):
        url = form_links.get(key) if form_links else None
        html_body = html_body.replace(
            f"*|{key}|*",
            f'<a href="{html_lib.escape(url, quote=True)}">{html_lib.escape(link_text)}</a>' if url else "",
        )

    message = MIMEMultipart()
    message.attach(MIMEText(html_body, "html"))

    attached_counties = []
    for county, pdf_bytes in county_pdfs.items():
        if pdf_bytes is None:
            continue
        part = MIMEApplication(pdf_bytes, _subtype="pdf")
        part.add_header(
            "Content-Disposition", "attachment",
            filename=f"Credential_Letter_{county.replace(' ', '_')}.pdf",
        )
        message.attach(part)
        attached_counties.append(county)

    if ics_attachment is not None:
        ics_part = MIMEBase("text", "calendar", method="PUBLISH", name="credential.ics")
        ics_part.set_payload(ics_attachment)
        encoders.encode_base64(ics_part)
        ics_part.add_header("Content-Disposition", "attachment", filename="credential.ics")
        message.attach(ics_part)

    message["to"] = to_email
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()

    sent = service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()

    return sent, attached_counties


# --- main ------------------------------------------------------------

def main(project_id: str = BQ_PROJECT_ID):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting credential email run (sends, no drafts) -- run_id={run_id} (DRY_RUN={DRY_RUN})")
    if TEST_RECIPIENT:
        logger.warning(f"TEST MODE: every email goes to {TEST_RECIPIENT}, recorded as 'test_sent' -- "
                       f"no volunteer is marked sent")

    content_blocks = read_content_blocks()
    composition = read_credential_composition()
    if not composition:
        logger.warning("Credential email template not found in the sheet -- nothing to do")
        return

    volunteers = get_eligible_volunteers(project_id)
    if volunteers.empty:
        logger.info("No volunteers eligible for a credential email this run")
        return

    if TEST_RECIPIENT and TEST_VAN_IDS:
        volunteers = volunteers[volunteers["van_id"].astype(str).isin(TEST_VAN_IDS)]
        logger.warning(f"TEST MODE: only van_id(s) {sorted(TEST_VAN_IDS)} -- {len(volunteers)} found in the queue")
        if volunteers.empty:
            logger.warning("None of the test_van_id volunteers are in the queue -- nothing to do")
            return
    if TEST_RECIPIENT and TEST_LIMIT > 0:
        logger.warning(f"TEST MODE: limiting to the first {TEST_LIMIT} of {len(volunteers)} queued volunteer(s)")
        volunteers = volunteers.head(TEST_LIMIT)
    if not TEST_RECIPIENT and SEND_LIMIT > 0 and len(volunteers) > SEND_LIMIT:
        logger.warning(f"SEND LIMIT: handling only the first {SEND_LIMIT} of {len(volunteers)} queued "
                       f"volunteer(s) this run -- run it again for the rest")
        volunteers = volunteers.head(SEND_LIMIT)

    van_ids = volunteers["van_id"].tolist()
    all_shifts = get_shifts_for_volunteers(project_id, van_ids)

    gmail_service = None if DRY_RUN else get_gmail_service()
    results = []

    for _, volunteer in volunteers.iterrows():
        van_id = volunteer["van_id"]
        shifts = all_shifts[all_shifts["van_id"] == van_id]

        if shifts.empty:
            logger.warning(f"No shift rows found for van_id={van_id} -- skipping, needs manual check")
            results.append({
                "van_id": van_id, "email": volunteer.get("email"),
                "draft_status": "no_shifts_found", "error_reason": "no matching rows in pw_credential_shifts",
                "counties_attached": 0, "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        values = credential_merge_values(volunteer, shifts, project_id=project_id)
        subject, body = build_email_from_composition(composition, content_blocks, values)
        if subject is None:
            results.append({
                "van_id": van_id, "email": volunteer.get("email"),
                "draft_status": "template_missing", "error_reason": "credential template not found",
                "counties_attached": 0, "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        counties = sorted(shifts["county"].unique().tolist())
        county_pdfs = {c: get_credential_letter_pdf(van_id, c) for c in counties}

        try:
            ics_attachment = build_shifts_ics(van_id, shifts)
        except Exception as e:
            logger.warning(f"Could not build .ics for van_id={van_id}: {e} -- sending without attachment")
            ics_attachment = None

        to_email = volunteer.get("email")
        # CHANGED 2026-09-21 -- routing. A multi-county volunteer who is not
        # yet approved gets a review copy at REVIEW_EMAIL_ADDRESS; everyone
        # else (single-county, or multi-county once approved) gets the real
        # email. The results row (status "sent") is what removes a volunteer
        # from QUEUE_VIEW; review copies leave them in the queue.
        hold_for_review = bool(volunteer.get("is_multi_county")) and not bool(volunteer.get("approved"))
        missing_pdfs = [c for c in counties if county_pdfs.get(c) is None]

        if not hold_for_review and not to_email:
            logger.error(f"No email on file for van_id={van_id} -- cannot send")
            results.append({
                "van_id": van_id, "email": None,
                "draft_status": "no_email", "error_reason": "no email on file",
                "counties_attached": 0, "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        if not hold_for_review and missing_pdfs and not ALLOW_MISSING_PDF:
            logger.warning(f"Skipping van_id={van_id}: no credential letter PDF for {missing_pdfs} "
                            f"-- not sending an email without the letter")
            results.append({
                "van_id": van_id, "email": to_email,
                "draft_status": "missing_pdf", "error_reason": f"missing PDF for {missing_pdfs}",
                "counties_attached": 0, "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        # ADDED 2026-10-07: badge pickup for Fulton/DeKalb is assigned by the volunteer's
        # location. No coordinates yet -> no assigned site -> don't email them (they stay in
        # the queue; re-run step2_nearest_pickup.sql once coordinates exist, then run again).
        unassigned = [] if hold_for_review else needs_pickup_match(van_id, counties, project_id)
        if unassigned:
            logger.warning(f"Skipping van_id={van_id}: no assigned badge pickup site for {unassigned} "
                           f"(not geocoded yet) -- stays in the queue")
            results.append({
                "van_id": van_id, "email": to_email,
                "draft_status": "no_pickup_match", "error_reason": f"no pickup site for {unassigned}",
                "counties_attached": 0, "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        recipient = REVIEW_EMAIL_ADDRESS if hold_for_review else to_email
        send_subject = subject
        if hold_for_review:
            prefix = "[REVIEW NEEDED - MISSING PDF] " if missing_pdfs else "[REVIEW NEEDED] "
            send_subject = f"{prefix}{subject}"

        if TEST_RECIPIENT:
            recipient = TEST_RECIPIENT
            send_subject = f"[TEST - real address: {to_email}] {send_subject}"

        form_links = build_credential_form_link_variants(CREDENTIAL_FORM_LINK_TEMPLATE, volunteer)

        if DRY_RUN:
            link_status = "with confirmation links" if form_links else "NO confirmation links (template not set)"
            # ADDED 2026-10-07: show which pickup text this volunteer's email will carry
            # (first lines of the Location blurb), so the geolocated site can be checked
            # in the dry run: a matched Fulton/DeKalb volunteer shows their own site and
            # distance; everyone else shows the county's BRE text.
            _loc = " / ".join(
                x.strip() for x in str(values.get("Badge Pick Up Location Blurb", "") or "").splitlines() if x.strip()
            )[:170]
            _time = " / ".join(
                x.strip() for x in str(values.get("Badge Pick Up Time Blurb", "") or "").splitlines() if x.strip()
            )[:110]
            logger.info(f"[DRY RUN] Would send to={recipient} subject={send_subject!r} "
                        f"attached_counties={[c for c in counties if county_pdfs.get(c) is not None]} "
                        f"missing_pdfs={missing_pdfs} {link_status} "
                        f"(hold_for_review={hold_for_review}, would go to {to_email}) "
                        f"time=[{_time}] pickup=[{_loc}]")
            results.append({
                "van_id": van_id, "email": to_email,
                "draft_status": "dry_run_ok",
                "error_reason": f"missing PDF for {missing_pdfs}" if missing_pdfs else None,
                "counties_attached": len(counties) - len(missing_pdfs),
                "run_id": run_id, "run_datetime": run_datetime,
            })
            continue

        try:
            _, attached_counties = send_credential_email(
                gmail_service, recipient, send_subject, body, county_pdfs, content_blocks,
                ics_attachment=ics_attachment, form_links=form_links,
            )
            if hold_for_review:
                logger.info(f"Sent review copy for van_id={van_id} to {recipient} "
                            f"(would go to {to_email}; {len(attached_counties)} PDF(s) attached)")
                status = "sent_for_review"
            else:
                logger.info(f"Sent credential email to {recipient} (van_id={van_id}, "
                            f"{len(counties)} count{'y' if len(counties)==1 else 'ies'}, "
                            f"{len(attached_counties)} PDF(s) attached)")
                status = "sent"
            if TEST_RECIPIENT:
                status = "test_sent"
            results.append({
                "van_id": van_id, "email": to_email,
                "draft_status": status,
                "error_reason": f"missing PDF for {missing_pdfs}" if missing_pdfs else None,
                "counties_attached": len(attached_counties), "run_id": run_id, "run_datetime": run_datetime,
            })
        except Exception as e:
            logger.error(f"Failed to send credential email for van_id={van_id} to {recipient}: {e}")
            results.append({
                "van_id": van_id, "email": to_email,
                "draft_status": "review_send_error" if hold_for_review else "send_error",
                "error_reason": str(e),
                "counties_attached": 0, "run_id": run_id, "run_datetime": run_datetime,
            })

    results_df = pd.DataFrame(results)
    results_table_schema = [
        {"name": "van_id", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "draft_status", "type": "STRING"},
        {"name": "error_reason", "type": "STRING"},
        {"name": "counties_attached", "type": "INTEGER"},
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

    sent_count = (results_df["draft_status"] == "sent").sum()
    review_count = (results_df["draft_status"] == "sent_for_review").sum()
    logger.info("=" * 60)
    logger.info("  CREDENTIAL EMAIL RUN COMPLETE")
    logger.info(f"  Run ID:          {run_id}")
    logger.info(f"  Sent:            {sent_count}")
    logger.info(f"  Sent for review: {review_count}")
    logger.info(f"  Not sent:        {len(results_df) - sent_count - review_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main(project_id=BQ_PROJECT_ID)