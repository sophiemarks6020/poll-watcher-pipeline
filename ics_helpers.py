# ics_helpers.py: compartmentalized ics functions that our pw email scripts will
# use to build ics attachments (it's the same stuff pretty much every time, so better
# to have just one script called into the existing email scripts when it needs to
# create ics files)
from zoneinfo import ZoneInfo
from ics import Calendar, Event
import pandas as pd
from google.cloud import bigquery

EASTERN = ZoneInfo("America/New_York")


def to_eastern(dt):
    """
    BigQuery DATETIME columns come back from pandas-gbq as naive
    datetimes that already represent Eastern wall-clock time -- there's
    just no tzinfo attached to say so. ics.py assumes a naive datetime
    is UTC, so without this, every event would render 4-5 hours early
    on the recipient's calendar. If dt already has a tzinfo (e.g. from
    a TIMESTAMP column), we leave it alone -- it's already unambiguous.
    """
    if dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=EASTERN)


def build_multi_event_ics(events):
    """
    events: list of dicts, each with uid, title, begin, end, and
    optionally description/location. Serializes all of them into one
    Calendar as separate VEVENTs -- this is how one person with a
    training and multiple poll-watching shifts gets ONE .ics with
    several entries, rather than several separate .ics files.
    Any event missing uid/begin/end is silently skipped rather than
    raising, so one bad row doesn't block calendar entries for a
    person's other, valid shifts. Returns None if nothing valid was
    left to serialize.
    """
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
    """Single-event convenience wrapper, used by pw_training_reminder_email.py."""
    return build_multi_event_ics([{
        "uid": uid, "title": title, "begin": begin, "end": end,
        "description": description, "location": location,
    }])

def get_shift_details(project_id, table_name):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT
            s.email, s.first_name, s.last_name,
            s.eventId AS event_id, s.eventShiftId AS event_shift_id,
            s.shift_choice,
            es.datetime_offset_begin, es.datetime_offset_end
        FROM `{project_id}.{table_name}` s
        JOIN `your-gcp-project.vansync.event_shifts` es
            ON CAST(s.eventId AS STRING) = CAST(es.event_id AS STRING)
            AND CAST(s.eventShiftId AS STRING) = CAST(es.event_shift_id AS STRING)
    """
    df = client.query(query).to_dataframe()
    df["composite_key"] = df.apply(
        lambda r: make_composite_key(r["email"], r["first_name"], r["last_name"]),
        axis=1,
    )
    return df

def make_composite_key(email, first_name, last_name):
    return f"{str(email).strip().lower()}|{str(first_name).strip().lower()}|{str(last_name).strip().lower()}"


def build_shift_ics_lookup(shift_details_df):
    """
    Groups a shift_details_df (one row per shift, possibly several
    rows per person) by composite_key, and returns
    {composite_key: ics_bytes} -- one serialized, possibly multi-event
    .ics per person. Shared by confirmation_email.py and
    reschedule_email.py, since both need to look up "everything this
    person is currently signed up for" the same way.
    """
    lookup = {}
    for composite_key, group in shift_details_df.groupby("composite_key"):
        events = [
            {
                "uid": f"pw-{row['event_id']}-{row['event_shift_id']}@example.org",
                "title": row.get("shift_choice") or "Poll Watcher Commitment",
                "begin": row.get("datetime_offset_begin"),
                "end": row.get("datetime_offset_end"),
            }
            for _, row in group.iterrows()
            if not pd.isna(row.get("datetime_offset_begin"))
        ]
        ics_bytes = build_multi_event_ics(events)
        if ics_bytes is not None:
            lookup[composite_key] = ics_bytes
    return lookup