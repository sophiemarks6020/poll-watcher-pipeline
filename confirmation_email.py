import os
import json
import base64
import logging
import uuid
import json
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

BQ_PROJECT_ID = os.getenv("project")


SOURCE_VIEW = "commons.pw_confirmation_email_template"
RESULTS_TABLE = os.getenv("result_table_name", "commons.pw_confirmation_email_results")

SENDER_ADDRESS = "voterprotection@example.org"
# SA_KEY_PATH = os.getenv("ga_dems_service_account_json")

SPREADSHEET_ID = os.getenv("spreadsheet_id")
TAB = "Content Blocks"

def build_service_account_info() -> dict:
    return json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))

sa_info = build_service_account_info()

creds = service_account.Credentials.from_service_account_info(
    sa_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

logger.info("Service account email: %s", sa_info["client_email"])

service = build("sheets", "v4", credentials=creds)
resp = service.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range=f"{TAB}!A:B",
).execute()
rows = resp.get("values", [])

# Instantiate empty dict to hold content blocks
CONTENT_BLOCKS = {}

# Read the content blocks from the spreadsheet and store them in a dictionary
for r in rows:
    name = (r[0].strip() if len(r) > 0 and r[0] else "")
    content = (r[1] if len(r) > 1 and r[1] else "")
    if name and content:
        CONTENT_BLOCKS[name] = content

print(f"\nRead {len(rows)} rows, built {len(CONTENT_BLOCKS)} content blocks.\n")
for check in [
    "P1 Introduction"
    , "P1 County Democratic Party"
    , "P1 Introduction Verification"
    , "Verification in Progress"
    , "On Demand Confirmation"
    , "Zoom Confirmation"
    , "Training/False"
    , "Location Assignment"
    , "Credentials"
    , "Contact Info"
    , "SOC Vol Contact Info"
    , "Training Reminder"
    , "Training Deadlines"
    , "Shifts"
    , "Review Instructions"
    , "Resources"
    , "Assignment Dates"
    , "Credential Dates"
    , "Communications"
    , "Closing"
              ]:
    status = "FOUND" if check in CONTENT_BLOCKS else "MISSING"
    preview = CONTENT_BLOCKS.get(check, "")[:60].replace("\n", " ⏎ ")
    print(f"{status}: {check} — {preview}")

# 1 Confirmation Emails sheet that we're pulling from the connected Google Sheet
CE_TAB = "1 Confirmation Emails"

service = build("sheets", "v4", credentials=creds)
resp = service.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID, range=f"{CE_TAB}!B:K",
).execute()
rows = resp.get("values", [])

# Pad every row to match the longest row (fillvalue="") before transposing
transposed = list(itertools.zip_longest(*rows, fillvalue=""))

# Instantiate empty dict to hold template composition
TEMPLATE_COMPOSITION = {}

# Build TEMPLATE_COMPOSITION from spreadhsheet rows and transposed rows-into-cols
for col in transposed:
    key = col[0]
    if key == "":
        continue  # skip any extra padding column with no header

    subject = col[1] if len(col) > 1 else ""
    block = [cell for cell in col[2:] if cell != ""]

    TEMPLATE_COMPOSITION[key] = {"subject": subject, "blocks": block}


STATIC_MERGE_FIELDS = {
    "election_name": "2026 General Election",
    "poll_watcher_manual_link": "https://gadems.info/PWManual",
    "poll_watcher_faq_link": "https://gadems.info/PWFAQ",
    "assignment_dates": (
        "\u25cf For Early Voting Shifts: Friday, October 2, 2026\n"
        "\u25cf For Election Day Shifts: Friday, October 23, 2026"
    ),
    "credential_dates": (
        "\u25cf For Early Voting Shifts: Wednesday, October 7, 2026\n"
        "\u25cf For Election Day Shifts: Wednesday, October 28, 2026"
    ),
    "training_deadline_evip": "Monday, September 28, 2026",
    "training_deadline_eday": "Monday, October 19, 2026",
}


def clean(value):
    """
    Converts pandas NaN (which .format() renders as the literal string
    "nan") to an empty string. Applied to every per-row merge field so
    this bug class can't resurface field-by-field.
    """
    return "" if pd.isna(value) else str(value)


# HTML-ify the text, linkify any URLs, and wrap bullet lists in <ul>...</ul>
BULLET = "\u25cf"

HEADER_LINES = {
    "Location Assignment",
    "Credentials",
    "Resources",
    "Communications During Your Shift",
    "LBJ Tips",
    "Contact Information",
    "Contact Information on File:",
    "Training Deadlines and Legal Requirement",
    "Registered Training Date and Zoom Link",
}

# Static label prefixes to bold on an otherwise per-recipient line (label
# is fixed, the merge-field value after it isn't, so it can't go in
# HEADER_LINES like a fully static line can).
BOLD_LABEL_PREFIXES = (
    "County of Residence:",
)

_URL_RE = re.compile(r"(https?://[^\s]+)")

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
                bold_prefix = next((p for p in BOLD_LABEL_PREFIXES if l.startswith(p)), None)
                if bold_prefix:
                    rest = l[len(bold_prefix):]
                    esc = "<b>" + html_lib.escape(bold_prefix) + "</b>" + _linkify(html_lib.escape(rest))
                else:
                    esc = _linkify(html_lib.escape(l))
                    if l.strip() in HEADER_LINES:
                        esc = "<b>" + esc + "</b>"
                rendered.append(esc)
            out.append("<p>" + "<br>".join(rendered) + "</p>")
    return "".join(out)

def build_template(template_name, row):
    comp = TEMPLATE_COMPOSITION[template_name]
    values = {
        "First Name": clean(row.get("first_name", "")),
        "Election Name": "2026 General Election",
        "Email": clean(row.get("email", "")),
        "Cell Phone Number": clean(row.get("cell_phone_number", "")),
        "Street Address": clean(row.get("street_address", "")),
        "City": clean(row.get("city", "")),
        "State": clean(row.get("state", "")),
        "Zip Code": clean(row.get("zip_code", "")),
        "County of Residence": clean(row.get("county_of_residence", "")),
        "How far from your home are you willing to travel while volunteering?": clean(row.get("travel_distance", "")),
        "Select Training Session": clean(row.get("select_training_session", "")),
        "Shift Dates": "\n".join(
            f"\u25cf {line}" for line in clean(row.get("shift_dates", "")).split("\n") if line.strip()
            ),
        "ZOOM_LINK": clean(row.get("zoom_link", "")),
        "Poll Watcher Manual Link": "https://gadems.info/PWManual",
        "Poll Watcher FAQ": "https://gadems.info/PWFAQ",
        "Assignment Dates": STATIC_MERGE_FIELDS["assignment_dates"],
        "Credential Dates": STATIC_MERGE_FIELDS["credential_dates"],
        "Training Deadlines EVIP": "Monday, September 28, 2026",
        "Training Deadlines EDay": "Monday, October 19, 2026",
        "Rescheduling Form Link": "https://gadems.info/resched26",
        "On Demand Training": "https://gadems.info/OD",
        "ON-DEMAND_LINK": "https://gadems.info/OD",
    }
    def repl(m):
        return values.get(m.group(1).strip(), m.group(0))   # unknown label stays visible
    body = "\n\n".join(
        re.sub(r"\*\|(.+?)\|\*", repl, CONTENT_BLOCKS[b]) for b in comp["blocks"]
    )
    return comp["subject"], body


def get_gmail_service():
    service_account_info = json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    delegated_credentials = credentials.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated_credentials)


def get_recipients(project_id):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.{SOURCE_VIEW}` WHERE confirmation_template IS NOT NULL"
    return client.query(query).to_dataframe()


def get_last_sent_timestamps(project_id, results_table):
    """
    Returns {(email, template_id): last_successful_send_datetime}. Used
    with each recipient's real form submission_timestamp to distinguish
    "already got the correct email, nothing's changed" from "resubmitted,
    needs a fresh send" -- no manual BigQuery deletes needed.
    """
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT record_id, template_id, MAX(run_datetime) AS last_sent
        FROM `{project_id}.{results_table}`
        WHERE send_status = 'success'
        GROUP BY record_id, template_id
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{results_table} doesn't exist yet — treating as first run")
        return {}
    return {
        (row["record_id"], row["template_id"]): pd.to_datetime(row["last_sent"])
        for _, row in result.iterrows()
    }


def needs_send(row, last_sent_map):
    """
    True if this person should get an email right now:
      - they've never successfully received THIS template before, or
      - their form submission_timestamp is newer than the last time
        they successfully received this exact template (i.e. they
        resubmitted since)
    False if they already got this exact template and haven't
    resubmitted since -- correctly skipped, no manual action needed.
    """
    key = (row["email"], row["confirmation_template"])
    last_sent = last_sent_map.get(key)
    if last_sent is None:
        return True
    submitted_at = row.get("submission_timestamp")
    if submitted_at is None or pd.isna(submitted_at):
        return False

    # Google Forms writes naive local-time strings (Eastern) into the
    # Timestamp column. pd.to_datetime(..., utc=True) on a naive string
    # does NOT convert from local time -- it labels the naive value as
    # if it were already UTC, which is wrong by several hours. Must
    # explicitly localize as Eastern first, then convert.
    submitted_at = pd.to_datetime(submitted_at)
    if submitted_at.tzinfo is None:
        submitted_at = submitted_at.tz_localize("America/New_York").tz_convert("UTC")
    else:
        submitted_at = submitted_at.tz_convert("UTC")

    last_sent_utc = last_sent.tz_localize("UTC") if last_sent.tzinfo is None else last_sent
    return submitted_at > last_sent_utc


def send_email(service, to_email, subject, body):
    message = MIMEText(to_html(body), "html")
    message["to"] = to_email
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()


def main(project_id, results_table):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting confirmation email send — run_id={run_id}")

    gmail_service = get_gmail_service()
    recipients_df = get_recipients(project_id)
    logger.info(f"Found {len(recipients_df)} recipients with a template match")

    last_sent_map = get_last_sent_timestamps(project_id, results_table)
    before_count = len(recipients_df)
    recipients_df = recipients_df[
        recipients_df.apply(lambda r: needs_send(r, last_sent_map), axis=1)
    ]
    logger.info(f"Filtered out {before_count - len(recipients_df)} recipients already sent this exact template with no resubmission since — {len(recipients_df)} remaining")

    results = []
    for _, row in recipients_df.iterrows():
        template_name = row["confirmation_template"]

        if template_name not in TEMPLATE_COMPOSITION:
            logger.warning(f"No template found for '{template_name}' — skipping {row['email']}")
            results.append({
                "record_id": row["email"],
                "template_id": template_name,
                "send_status": "skipped",
                "error_reason": "no_template_found",
                "run_id": run_id,
                "run_datetime": run_datetime,
            })
            continue

        try:
            subject, body = build_template(template_name, row)
            send_email(gmail_service, row["email"], subject, body)
            logger.info(f"Sent '{template_name}' to {row['email']}")
            results.append({
                "record_id": row["email"],
                "template_id": template_name,
                "send_status": "success",
                "error_reason": None,
                "run_id": run_id,
                "run_datetime": run_datetime,
            })
        except Exception as e:
            logger.error(f"Failed to send to {row['email']}: {e}")
            results.append({
                "record_id": row["email"],
                "template_id": template_name,
                "send_status": "send_error",
                "error_reason": str(e),
                "run_id": run_id,
                "run_datetime": run_datetime,
            })

    results_df = pd.DataFrame(results)
    pandas_gbq.to_gbq(
        results_df,
        destination_table=results_table,
        project_id=project_id,
        if_exists="append",
        progress_bar=True,
    )
    logger.info(f"Wrote {len(results_df)} result rows to {results_table}")


if __name__ == "__main__":
    main(
        project_id=BQ_PROJECT_ID,
        results_table=RESULTS_TABLE,
    )