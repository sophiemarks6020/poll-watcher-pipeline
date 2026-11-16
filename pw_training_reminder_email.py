import os
import json
import base64
import logging
import uuid
import re
import itertools
import html as html_lib
from datetime import datetime, timezone, date
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
from zoneinfo import ZoneInfo
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from ics import Calendar, Event
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

# Real tab, same spreadsheet, same transposed pattern as "1 Confirmation
# Emails": row1 = template name, row2 = subject, row3+ = block list.
# No day-offset row in it — offsets live in code below instead.
REMINDER_TAB = os.getenv("reminder_tab", "2 Reminder Emails")
ZOOM_TEMPLATE_NAME = os.getenv("zoom_template_name", "Zoom Training Reminder Email")
ON_DEMAND_TEMPLATE_NAME = os.getenv("on_demand_template_name", "On Demand Training Reminder Email")

# 1 day before, per the VoPro lead's confirmed answer.
ZOOM_REMINDER_OFFSETS = [1]

SCHEDULED_TRAININGS_TABLE = os.getenv("scheduled_trainings_table", "commons.pw_scheduled_trainings_daily")
RESULTS_TABLE = os.getenv("result_table_name", "commons.pw_training_reminder_results")

# Fixed calendar dates, not per-person offsets — 2 days before the EVIP
# and EDay training deadlines, per the VoPro lead.
ON_DEMAND_TRIGGERS = [
    {"key": "on_demand_evip", "trigger_date": date(2026, 9, 26), "deadline_display": "12:00 p.m. (noon) on September 28, 2026"},
    {"key": "on_demand_eday", "trigger_date": date(2026, 10, 17), "deadline_display": "12:00 p.m. (noon) on October 19, 2026"},
]

SENDER_ADDRESS = "voterprotection@example.org"

# Assumes one shared Zoom link for every session — confirm if that changes.
ZOOM_LINK = os.getenv("zoom_link", "https://us06web.zoom.us/j/89476925858?pwd=QP3pAvBbvb0ANtajrUKCecasQP0Drz.1")
ON_DEMAND_LINK = os.getenv("on_demand_link", "https://dpg.thinkific.com/products/courses/runoff-poll-watcher-training")

# Same deadlines send_confirmation_emails.py's STATIC_MERGE_FIELDS uses.
TRAINING_DEADLINE_EVIP = os.getenv("training_deadline_evip", "Monday, September 28, 2026")
TRAINING_DEADLINE_EDAY = os.getenv("training_deadline_eday", "Monday, October 19, 2026")

# Set both to true only if "Content Blocks Helper" is confirmed as the real source.
CONTENT_IS_PREFORMATTED_HTML = os.getenv("content_is_preformatted_html", "false").strip().lower() == "true"

BULLET = "\u25cf"
BOLD_MARKER_START = "\x01"
BOLD_MARKER_END = "\x02"
_URL_RE = re.compile(r"(https?://[^\s]+)")

HEADER_LINES = {
    "Location Assignment",
    "Credentials",
    "Resources",
    "Communications During Your Shift",
    "Training Reminder",
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


# --- ics --------------------------------------------------------------

EASTERN = ZoneInfo("America/New_York")

def to_eastern(dt):
    if dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=EASTERN)

def build_multi_event_ics(events):
    cal = Calendar()
    for e in events:
        if not e.get("uid") or e.get("begin") is None or e.get("end") is None:
            continue
        event = Event()
        event.uid = e["uid"]
        event.name = e.get("title") or "Poll Watcher Commitment"
        event.begin = to_eastern(e["begin"])
        event.end = to_eastern(e["end"])
        if e.get("description"):
            event.description = e["description"]
        if e.get("location"):
            event.location = e["location"]
        cal.events.add(event)
    if not cal.events:
        return None
    return cal.serialize().encode("utf-8")

def build_ics_attachment(uid, title, begin, end, description="", location=""):
    return build_multi_event_ics([{
        "uid": uid, "title": title, "begin": begin, "end": end,
        "description": description, "location": location,
    }])

def build_zoom_ics(row):
    """
    Builds the .ics attachment for one Zoom training shift. The UID
    format (event_id-event_shift_id) is the same one the confirmation
    email will use for the same training, so a volunteer who imports
    both ends up with one calendar entry, not two.
    """
    uid = f"pw-{row['event_id']}-{row['event_shift_id']}@example.org"
    return build_ics_attachment(
        uid=uid,
        title=row.get("event_name") or "Poll Watcher Training",
        begin=row["shift_start_datetime"],
        end=row["shift_end_datetime"],
        description=f"Zoom link: {ZOOM_LINK}",
        location=ZOOM_LINK,
    )


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


def read_reminder_composition():
    """
    Transposed tab, one template per column: row1 = template name,
    row2 = subject, row3+ = ordered block names. No offset row — that
    lives in code (ZOOM_REMINDER_OFFSETS / ON_DEMAND_TRIGGERS).
    """
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{REMINDER_TAB}!B:Z",
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

    logger.info(f"Read {len(composition)} reminder templates from '{REMINDER_TAB}': {list(composition.keys())}")
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
                esc = esc.replace(BOLD_MARKER_START, "<b>").replace(BOLD_MARKER_END, "</b>")
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
        logger.warning(f"Reminder template '{template_name}' not found in '{REMINDER_TAB}' — skipping this send")
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


def zoom_merge_values(row, days_before):
    return {
        "First Name": clean(row.get("first_name", "")),
        "Election Name": "2026 General Election",
        "Training Date": clean(row.get("training_date_display", "")),
        "Days Before": str(days_before),
        "EVENT_START": clean(row.get("event_start_display", "")),
        "ZOOM_LINK": ZOOM_LINK,
        "ON-DEMAND_LINK": ON_DEMAND_LINK,
        "Training Deadlines EVIP": TRAINING_DEADLINE_EVIP,
        "Training Deadlines EDay": TRAINING_DEADLINE_EDAY,
    }


def on_demand_merge_values(first_name, deadline_display):
    # NOTE: "On Demand Reminder" block currently hardcodes "September 28"
    # as plain text — needs a *|Training Deadline|* swap in the sheet
    # before the EDay trigger will print the right date.
    return {
        "First Name": clean(first_name),
        "Election Name": "2026 General Election",
        "ON-DEMAND_LINK": ON_DEMAND_LINK,
        "Training Deadline": deadline_display,
    }

def bold_on_demand_deadline_phrase(body, deadline_display):
    pattern = re.compile(
        r"please complete training by 12:00 pm \(noon\) on \w+ \d{1,2}, \d{4}",
        re.IGNORECASE,
    )
    replacement = f"please complete training by {deadline_display}"
    return pattern.sub(replacement, body)


# --- bigquery ------------------------------------------------------------

def get_scheduled_trainings(project_id):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT myc_van_id, email, first_name, event_id, event_shift_id,
               event_name, event_date, shift_start_datetime, shift_end_datetime
        FROM `{project_id}.{SCHEDULED_TRAININGS_TABLE}`
        WHERE snapshot_date = CURRENT_DATE('America/New_York')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY myc_van_id
            ORDER BY event_date ASC
        ) = 1
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Found {len(df)} people with an upcoming confirmed training in today's snapshot")
    return df


def get_on_demand_incomplete(project_id):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT email, first_name, last_name
        FROM `{project_id}.commons.pw_confirmation_email_template`
        WHERE confirmation_template IN ('Confirmation 2: Non-SOC On Demand', 'Confirmation 3: Training Only OD')
          AND is_verified_trained = FALSE
    """
    df = client.query(query).to_dataframe()
    df = df.drop_duplicates(subset="email")
    logger.info(f"Found {len(df)} people who picked On Demand training and haven't completed it yet")
    return df


def get_on_demand_already_sent(project_id, results_table, reminder_key):
    # No myc_van_id/event_id for on-demand — dedup by email, event_id IS NULL
    # marks a results row as belonging to this reminder type.
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT record_id
        FROM `{project_id}.{results_table}`
        WHERE send_status = 'success'
          AND reminder_key = @reminder_key
          AND event_id IS NULL
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("reminder_key", "STRING", reminder_key)]
    )
    try:
        result = client.query(query, job_config=job_config).to_dataframe()
    except NotFound:
        logger.info(f"{results_table} doesn't exist yet — treating as first run")
        return set()
    return set(result["record_id"])


def get_already_sent(project_id, results_table):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT DISTINCT myc_van_id, reminder_key, event_id
        FROM `{project_id}.{results_table}`
        WHERE send_status = 'success'
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{results_table} doesn't exist yet — treating as first run")
        return set()
    return set(
        (row["myc_van_id"], row["reminder_key"], row["event_id"])
        for _, row in result.iterrows()
    )


def zoom_offsets_due(row, already_sent):
    """
    For one scheduled-training row, returns the list of day-offsets
    (from ZOOM_REMINDER_OFFSETS) due to fire right now. Uses <= rather
    than == so a missed run day doesn't skip an offset entirely.
    """
    event_date = row["event_date"]
    if isinstance(event_date, datetime):
        event_date = event_date.date()
    days_until = (event_date - date.today()).days

    if days_until < 0:
        return []

    due = []
    for offset in ZOOM_REMINDER_OFFSETS:
        if days_until > offset:
            continue
        reminder_key = f"zoom_{offset}d"
        key = (row["myc_van_id"], reminder_key, row["event_id"])
        if key in already_sent:
            continue
        due.append(offset)
    return due


# --- gmail -----------------------------------------------------------

def get_gmail_service():
    service_account_info = json.loads(os.getenv("GA_COORDINATED_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    delegated_credentials = credentials.with_subject(SENDER_ADDRESS)
    return build("gmail", "v1", credentials=delegated_credentials)


def send_email(service, to_email, subject, body, ics_attachment=None):
    html_body = body if CONTENT_IS_PREFORMATTED_HTML else to_html(body)

    if ics_attachment is None:
        message = MIMEText(html_body, "html")
    else:
        message = MIMEMultipart()
        message.attach(MIMEText(html_body, "html"))

        ics_part = MIMEBase("text", "calendar", method="PUBLISH", name="training.ics")
        ics_part.set_payload(ics_attachment)
        encoders.encode_base64(ics_part)
        ics_part.add_header("Content-Disposition", "attachment", filename="training.ics")
        message.attach(ics_part)

    message["to"] = to_email
    message["subject"] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return service.users().messages().send(userId="me", body={"raw": raw}).execute()


# --- main --------------------------------------------------------------

def main(project_id, results_table):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting training reminder send — run_id={run_id}")

    content_blocks = read_content_blocks()
    composition = read_reminder_composition()
    if not composition:
        logger.warning("No reminder templates found in the sheet — nothing to do")
        return

    scheduled = get_scheduled_trainings(project_id)
    already_sent = get_already_sent(project_id, results_table)

    gmail_service = get_gmail_service()
    results = []

    for _, row in scheduled.iterrows():
        due = zoom_offsets_due(row, already_sent)
        if not due:
            continue

        event_date = row["event_date"]
        if isinstance(event_date, datetime):
            event_date = event_date.date()
        row = dict(row)
        row["training_date_display"] = event_date.strftime("%A, %B %-d, %Y")

        shift_start = row.get("shift_start_datetime")
        if shift_start is not None and not pd.isna(shift_start):
            if hasattr(shift_start, "to_pydatetime"):
                shift_start = shift_start.to_pydatetime()
            row["event_start_display"] = shift_start.strftime("%A, %B %-d at %-I:%M %p")
        else:
            row["event_start_display"] = row["training_date_display"]
            logger.warning(f"No shift_start_datetime for event {row['event_id']} — EVENT_START falls back to just the date")

        for offset in due:
            reminder_key = f"zoom_{offset}d"
            values = zoom_merge_values(row, offset)
            subject, body = build_email_from_composition(ZOOM_TEMPLATE_NAME, composition, content_blocks, values)
            if subject is None:
                continue

            try:
                ics_attachment = build_zoom_ics(row)
            except Exception as e:
                logger.warning(f"Could not build .ics for event {row['event_id']}/{row.get('event_shift_id')}: {e} — sending without attachment")
                ics_attachment = None

            try:
                send_email(gmail_service, row["email"], subject, body, ics_attachment=ics_attachment)
                logger.info(f"Sent '{reminder_key}' to {row['email']} for event {row['event_id']}")
                results.append({
                    "myc_van_id": row["myc_van_id"],
                    "record_id": row["email"],
                    "reminder_key": reminder_key,
                    "event_id": row["event_id"],
                    "event_date": event_date.isoformat(),
                    "send_status": "success",
                    "error_reason": None,
                    "run_id": run_id,
                    "run_datetime": run_datetime,
                })
            except Exception as e:
                logger.error(f"Failed to send '{reminder_key}' to {row['email']}: {e}")
                results.append({
                    "myc_van_id": row["myc_van_id"],
                    "record_id": row["email"],
                    "reminder_key": reminder_key,
                    "event_id": row["event_id"],
                    "event_date": event_date.isoformat(),
                    "send_status": "send_error",
                    "error_reason": str(e),
                    "run_id": run_id,
                    "run_datetime": run_datetime,
                })

    for trigger in ON_DEMAND_TRIGGERS:
        if date.today() != trigger["trigger_date"]:
            continue

        logger.info(f"Today matches On Demand trigger '{trigger['key']}' — checking who's still incomplete")
        already_sent_od = get_on_demand_already_sent(project_id, results_table, trigger["key"])
        incomplete = get_on_demand_incomplete(project_id)

        for _, row in incomplete.iterrows():
            if row["email"] in already_sent_od:
                continue

            values = on_demand_merge_values(row["first_name"], trigger["deadline_display"])
            subject, body = build_email_from_composition(ON_DEMAND_TEMPLATE_NAME, composition, content_blocks, values)
            if subject is None:
                continue

            body = bold_on_demand_deadline_phrase(body, trigger["deadline_display"])

            try:
                send_email(gmail_service, row["email"], subject, body)
                logger.info(f"Sent On Demand reminder ('{trigger['key']}') to {row['email']}")
                results.append({
                    "myc_van_id": None,
                    "record_id": row["email"],
                    "reminder_key": trigger["key"],
                    "event_id": None,
                    "event_date": trigger["trigger_date"].isoformat(),
                    "send_status": "success",
                    "error_reason": None,
                    "run_id": run_id,
                    "run_datetime": run_datetime,
                })
            except Exception as e:
                logger.error(f"Failed to send On Demand reminder ('{trigger['key']}') to {row['email']}: {e}")
                results.append({
                    "myc_van_id": None,
                    "record_id": row["email"],
                    "reminder_key": trigger["key"],
                    "event_id": None,
                    "event_date": trigger["trigger_date"].isoformat(),
                    "send_status": "send_error",
                    "error_reason": str(e),
                    "run_id": run_id,
                    "run_datetime": run_datetime,
                })

    if not results:
        logger.info("No reminders due this run")
        return

    results_df = pd.DataFrame(results)
    results_table_schema = [
        {"name": "myc_van_id", "type": "INTEGER"},
        {"name": "record_id", "type": "STRING"},
        {"name": "reminder_key", "type": "STRING"},
        {"name": "event_id", "type": "INTEGER"},
        {"name": "event_date", "type": "DATE"},
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