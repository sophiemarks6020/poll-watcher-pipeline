import os
import json
import base64
import logging
import uuid
import re
import itertools
import html as html_lib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas_gbq
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- config -----------------------------------------------------------

BQ_PROJECT_ID = os.getenv("project")
SPREADSHEET_ID = os.getenv("spreadsheet_id")

CONTENT_BLOCKS_TAB = os.getenv("content_blocks_tab", "Content Blocks")
ON_DEMAND_TRAINING_LINK = os.getenv(
    "on_demand_training_link",
    "https://gadems.info/OD",
)
# Per the VoPro lead, 2026-09-04. Also defined on the 4 Assignments
# tab of the Data Mapping sheet -- worth confirming the two stay in sync.
LBJ_WALKTHROUGH_LINK = os.getenv(
    "lbj_walkthrough_link",
    "https://gadems.info/LBJvideo",
)

# Real tab, same spreadsheet, same transposed pattern as "1 Confirmation
# Emails" / "2 Reminder Emails": row1 = template name (matches template_id
# values written by the SQL script), row2 = subject, row3+ = block list.
POST_TRAINING_TAB = os.getenv("post_training_tab", "3 Post-Training Emails")

POST_TRAINING_STATUS_TABLE = os.getenv("post_training_status_table", "commons.pw_post_training_status_daily")
RESULTS_TABLE = os.getenv("result_table_name", "commons.pw_post_training_email_results")

SENDER_ADDRESS = "voterprotection@example.org"

# Same static fields send_confirmation_emails.py's STATIC_MERGE_FIELDS uses.
POLL_WATCHER_MANUAL_LINK = os.getenv("poll_watcher_manual_link", "https://gadems.info/PWManual")
POLL_WATCHER_FAQ_LINK = os.getenv("poll_watcher_faq_link", "https://gadems.info/PWFAQ")
ASSIGNMENT_DATES = os.getenv(
    "assignment_dates",
    "\u25cf For Early Voting Shifts: Friday, October 2, 2026\n"
    "\u25cf For Election Day Shifts: Friday, October 23, 2026",
)
CREDENTIAL_DATES = os.getenv(
    "credential_dates",
    "\u25cf For Early Voting Shifts: Wednesday, October 7, 2026\n"
    "\u25cf For Election Day Shifts: Wednesday, October 28, 2026",
)
TRAINING_DEADLINE_EVIP = os.getenv("training_deadline_evip", "Monday, September 28, 2026")
TRAINING_DEADLINE_EDAY = os.getenv("training_deadline_eday", "Monday, October 19, 2026")
# Real reschedule form link, confirmed.
RESCHEDULING_FORM_LINK = os.getenv(
    "rescheduling_form_link",
    "https://gadems.info/resched26",
)

CONTENT_IS_PREFORMATTED_HTML = os.getenv("content_is_preformatted_html", "false").strip().lower() == "true"

BULLET = "\u25cf"
_URL_RE = re.compile(r"(https?://[^\s]+)")

HEADER_LINES = {
    "Location Assignment",
    "Credentials",
    "Resources",
    "Communications During Your Shift",
    "Training Deadlines and Legal Requirement",
}


def build_service_account_info() -> dict:
    return json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))


sa_info = build_service_account_info()

sheets_creds = service_account.Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)
sheets_service = build("sheets", "v4", credentials=sheets_creds)


# --- sheet reading ------------------------------------------------------

def read_content_blocks():
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


def read_post_training_composition():
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{POST_TRAINING_TAB}!A:Z",
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

    logger.info(f"Read {len(composition)} post-training templates from '{POST_TRAINING_TAB}': {list(composition.keys())}")
    return composition


# --- html / merge fields -------------------------------------------------

def clean(value):
    return "" if pd.isna(value) else str(value)


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
                if l.strip() in HEADER_LINES:
                    esc = "<b>" + esc + "</b>"
                rendered.append(esc)
            out.append("<p>" + "<br>".join(rendered) + "</p>")
    return "".join(out)


def apply_merge_fields(text, values):
    def repl(m):
        return values.get(m.group(1).strip(), m.group(0))
    return re.sub(r"\*\|(.+?)\|\*", repl, text)


def build_email_from_composition(template_name, composition, content_blocks, values):
    if template_name not in composition:
        logger.warning(f"Post-training template '{template_name}' not found in '{POST_TRAINING_TAB}' — skipping this send")
        return None, None

    comp = composition[template_name]
    join_sep = "" if CONTENT_IS_PREFORMATTED_HTML else "\n\n"
    body = join_sep.join(
        apply_merge_fields(content_blocks[b], values)
        for b in comp["blocks"]
        if b in content_blocks
    )
    missing = [b for b in comp["blocks"] if b not in content_blocks]
    if missing:
        logger.warning(f"Template '{template_name}' references missing content block(s): {missing}")

    subject = apply_merge_fields(comp["subject"], values)
    return subject, body


def merge_values(row):
    return {
        "First Name": clean(row.get("first_name", "")),
        "Election Name": "2026 General Election",
        "Assignment Dates": ASSIGNMENT_DATES,
        "Credential Dates": CREDENTIAL_DATES,
        "Poll Watcher Manual Link": POLL_WATCHER_MANUAL_LINK,
        "Poll Watcher FAQ": POLL_WATCHER_FAQ_LINK,
        "Training Deadlines EVIP": TRAINING_DEADLINE_EVIP,
        "Training Deadlines EDay": TRAINING_DEADLINE_EDAY,
        "Rescheduling Form Link": RESCHEDULING_FORM_LINK,
    }


# --- bigquery ------------------------------------------------------------

def get_post_training_status(project_id):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT myc_van_id, email, first_name, event_id, event_status_name, template_id
        FROM `{project_id}.{POST_TRAINING_STATUS_TABLE}`
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Found {len(df)} resolved post-training status row(s)")
    return df


def get_already_sent(project_id, results_table):
    """
    Dedup key is (myc_van_id, event_id, template_id) — if a status later
    flips (e.g. a No Show correction to Completed), that produces a
    different template_id, which is a new key, so the person correctly
    gets the new email rather than staying silently skipped forever.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT myc_van_id, event_id, template_id
        FROM `{project_id}.{results_table}`
        WHERE send_status = 'success'
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{results_table} doesn't exist yet — treating as first run")
        return set()
    return set(
        (row["myc_van_id"], row["event_id"], row["template_id"])
        for _, row in result.iterrows()
    )


# --- gmail -----------------------------------------------------------

def get_gmail_service():
    service_account_info = json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    delegated_credentials = credentials.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated_credentials)


def send_email(service, to_email, subject, body):
    html_body = body if CONTENT_IS_PREFORMATTED_HTML else to_html(body)
    html_body = html_body.replace(
        "*|On Demand Training|*",
        f'<a href="{ON_DEMAND_TRAINING_LINK}">On Demand Training</a>',
    )
    # Per the VoPro lead, 2026-09-04: Resources Content Block added a
    # new *|LBJ Walkthrough|* field, styled the same as On Demand
    # Training above (asterisk-pipe field + dash explanation in the
    # sheet, not a bare-URL merge field like Poll Watcher Manual Link).
    # Same pattern: left OUT of merge_values() on purpose, so
    # apply_merge_fields leaves the literal placeholder untouched, then
    # swapped here for a real named link rather than a raw URL.
    html_body = html_body.replace(
        "*|LBJ Walkthrough|*",
        f'<a href="{LBJ_WALKTHROUGH_LINK}">LBJ Walkthrough</a>',
    )
    message = MIMEText(html_body, "html")
    message["to"] = to_email
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()


# --- main --------------------------------------------------------------

def main(project_id, results_table):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting post-training email send — run_id={run_id}")

    content_blocks = read_content_blocks()
    composition = read_post_training_composition()
    if not composition:
        logger.warning("No post-training templates found in the sheet — nothing to do")
        return

    status_df = get_post_training_status(project_id)
    already_sent = get_already_sent(project_id, results_table)

    gmail_service = get_gmail_service()
    results = []

    for _, row in status_df.iterrows():
        key = (row["myc_van_id"], row["event_id"], row["template_id"])
        if key in already_sent:
            continue

        template_name = row["template_id"]
        values = merge_values(row)
        subject, body = build_email_from_composition(template_name, composition, content_blocks, values)
        if subject is None:
            continue

        try:
            send_email(gmail_service, row["email"], subject, body)
            logger.info(f"Sent '{template_name}' to {row['email']} for event {row['event_id']}")
            results.append({
                "myc_van_id": row["myc_van_id"],
                "record_id": row["email"],
                "event_id": row["event_id"],
                "event_status_name": row["event_status_name"],
                "template_id": template_name,
                "send_status": "success",
                "error_reason": None,
                "run_id": run_id,
                "run_datetime": run_datetime,
            })
        except Exception as e:
            logger.error(f"Failed to send '{template_name}' to {row['email']}: {e}")
            results.append({
                "myc_van_id": row["myc_van_id"],
                "record_id": row["email"],
                "event_id": row["event_id"],
                "event_status_name": row["event_status_name"],
                "template_id": template_name,
                "send_status": "send_error",
                "error_reason": str(e),
                "run_id": run_id,
                "run_datetime": run_datetime,
            })

    if not results:
        logger.info("No post-training emails due this run")
        return

    results_df = pd.DataFrame(results)
    results_table_schema = [
        {"name": "myc_van_id", "type": "INTEGER"},
        {"name": "record_id", "type": "STRING"},
        {"name": "event_id", "type": "INTEGER"},
        {"name": "event_status_name", "type": "STRING"},
        {"name": "template_id", "type": "STRING"},
        {"name": "send_status", "type": "STRING"},
        {"name": "error_reason", "type": "STRING"},
        {"name": "run_id", "type": "STRING"},
        {"name": "run_datetime", "type": "TIMESTAMP"},
    ]
    pandas_gbq.to_gbq(
        results_df,
        destination_table=results_table,
        project_id=project_id,
        if_exists="append",
        table_schema=results_table_schema,
        progress_bar=True,
    )
    logger.info(f"Wrote {len(results_df)} result rows to {results_table}")


if __name__ == "__main__":
    main(
        project_id=BQ_PROJECT_ID,
        results_table=RESULTS_TABLE,
    )