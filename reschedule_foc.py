import re
import pandas as pd
import pandas_gbq
import requests
import time
import os
import json
import base64
import logging
import uuid
import pygsheets
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

API_USER = "YOUR_VAN_API_USER"
state_code = os.getenv("state_code")
API_KEY_ENV_VAR = f"VAN_{state_code}_TOKEN"
BQ_PROJECT_ID = os.getenv("project")

# RESCHEDULE PIPELINE — reads from the reschedule/rescheduling response
# sheet, not the original intake sheet.
SHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit?gid=1572612340#gid=1572612340"
SHEET_TAB = "Form Responses 1"

ZOOM_LINK_SHEET_URL = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
ZOOM_LINK_TAB = "2 Reminder Emails"

VAN_FOC_ENDPOINT = "https://api.securevan.com/v4/people/findOrCreate"
MAX_RETRIES = 3
INITIAL_WAIT = 2.0
MAX_CROSS_RUN_ATTEMPTS = 6

TRAINING_EVENT_TYPE_ID = "771155"
TRAINING_ROLE_ID = "661061"

# RESCHEDULE PIPELINE — mirror/availability/audit are separate tables so
# a reschedule run never wipes the original intake pipeline's data
# (both write with if_exists="replace" on mirror/availability, which is
# a full overwrite, not an append). FOC_STATUS_TABLE stays the SAME
# shared table as the original intake pipeline on purpose: FOC success
# is a property of the PERSON (composite_key), not of which sheet they
# submitted through, so sharing it means someone already resolved in
# VAN via the original intake correctly gets skipped here instead of
# being re-attempted from scratch.
MIRROR_TABLE = "commons.pw_reschedule_mirror"  # REPLACE table action
AVAILABILITY_TABLE = "commons.pw_reschedule_availability_long"  # REPLACE
FOC_STATUS_TABLE = "commons.pw_foc_status"  # MERGE action -- SHARED with original intake pipeline
AUDIT_LOG_TABLE = "commons.pw_reschedule_pipeline_audit_log"  # separate APPEND table, so reschedule runs don't mix into the original audit trail

COLUMN_RENAME_MAP = {
    "Email Address": "email",
    "First Name": "first_name",
    "Last Name": "last_name",
    "Cell Phone Number": "phonenumber",
    "State": "state_code",
    "Street Address": "streetaddress",
    "City": "city",
    "ZIP Code": "ziporpostalcode",
    "Language": "language",
    "County of Residence": "county_of_residence",
    "Training Verification": "training_verification",
    "Select Training Session": "select_training_session",
    "Travel Distance": "desired_travel_distance",
    "SMS Opt In": "sms_opt_in",
    "Timestamp": "submission_timestamp",
}

GENERIC_COLUMN_PATTERN = re.compile(r"[^a-z0-9]+")
DATE_TIME_HEADER_PATTERN = re.compile(r"^Date/Time\s*\[(.+)\]$")
PAREN_DATE_PATTERN = re.compile(r"\(([^()]+)\)\s*$")


def extract_van_error_detail(response: requests.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return response.text
    errors = data.get("errors")
    if not errors:
        return str(data)
    parts = []
    for err in errors:
        code = err.get("code", "UNKNOWN")
        text = err.get("text", "")
        properties = ", ".join(err.get("properties", []))
        detail = f"[{code}] {text}"
        if properties:
            detail += f" (properties: {properties})"
        parts.append(detail)
    return "; ".join(parts)


def build_composite_key(email, first_name, last_name):
    parts = [
        str(email or "").strip().lower(),
        str(first_name or "").strip().lower(),
        str(last_name or "").strip().lower(),
    ]
    return "|".join(parts)


def normalize_column_name(header):
    normalized = GENERIC_COLUMN_PATTERN.sub("_", header.strip().lower()).strip("_")
    return normalized or "unnamed_column"


def parse_date_from_header(header):
    match = DATE_TIME_HEADER_PATTERN.match(header)
    if not match:
        return None
    inner = match.group(1).replace("\xa0", " ").strip()
    paren_match = PAREN_DATE_PATTERN.search(inner)
    date_text = paren_match.group(1) if paren_match else inner
    try:
        return dateutil_parser.parse(date_text, fuzzy=True).date()
    except (ValueError, OverflowError):
        return None


def classify_header(header):
    match = DATE_TIME_HEADER_PATTERN.match(header)
    if not match:
        return None, None
    inner = match.group(1).replace("\xa0", " ").strip()
    parsed = parse_date_from_header(header)
    if parsed is None:
        return None, None
    if "runoff election day" in inner.lower():
        return "runoff_election_day", parsed
    elif "election day" in inner.lower():
        return "election_day", parsed
    else:
        return "general", parsed


def build_availability_long(raw_df, id_col="composite_key"):
    header_map = {}
    for col in raw_df.columns:
        category, parsed_date = classify_header(col)
        if category is not None:
            header_map[col] = (category, parsed_date)

    date_cols = list(header_map.keys())
    melted = raw_df.melt(
        id_vars=[id_col], value_vars=date_cols,
        var_name="header", value_name="shift_choice",
    )

    melted["shift_choice"] = melted["shift_choice"].astype(str).str.strip()
    melted = melted[
        melted["shift_choice"].notna()
        & (melted["shift_choice"] != "")
        & (melted["shift_choice"] != "nan")
        & (melted["shift_choice"] != "I AM NOT AVAILABLE")
    ]

    melted["event_date"] = melted["header"].map(lambda h: header_map[h][1])
    melted["date_category"] = melted["header"].map(lambda h: header_map[h][0])

    return melted[[id_col, "event_date", "date_category", "shift_choice"]].reset_index(drop=True)


def normalize_phone(value):
    if not value or pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    if digits[0] in "01" or digits[3] in "01":
        return None
    return digits


def get_sheet_data():
    gc = pygsheets.authorize(service_account_env_var="GA_COORDINATED_SERVICE_ACCOUNT")
    sh = gc.open_by_url(SHEET_URL)
    wk = sh.worksheet_by_title(SHEET_TAB)
    raw_df = wk.get_as_df(include_tailing_empty=False)

    raw_df["composite_key"] = raw_df.apply(
        lambda r: build_composite_key(
            r.get("Email Address"), r.get("First Name"), r.get("Last Name")
        ),
        axis=1,
    )

    before_count = len(raw_df)
    raw_df["_parsed_timestamp"] = pd.to_datetime(raw_df["Timestamp"], errors="coerce")
    raw_df = raw_df.sort_values("_parsed_timestamp").drop_duplicates(
        subset="composite_key", keep="last"
    )
    raw_df = raw_df.drop(columns=["_parsed_timestamp"])
    deduped_count = before_count - len(raw_df)
    if deduped_count > 0:
        logger.info(f"Deduplicated {deduped_count} older resubmission(s) — keeping only each person's most recent answers")

    df = pd.DataFrame({"composite_key": raw_df["composite_key"]})
    for header, new_name in COLUMN_RENAME_MAP.items():
        df[new_name] = raw_df[header]

    generic_headers = [
        h for h in raw_df.columns
        if h not in COLUMN_RENAME_MAP and h != "composite_key"
        and classify_header(h)[0] is None
    ]
    for header in generic_headers:
        col_name = normalize_column_name(header)
        if col_name in df.columns:
            logger.warning(f"Generic column name collision for header {header!r} -> {col_name!r} — skipping to avoid overwriting an existing column")
            continue
        df[col_name] = raw_df[header]

    availability_long_df = build_availability_long(raw_df, id_col="composite_key")

    return df, availability_long_df


def get_zoom_link_map():
    gc = pygsheets.authorize(service_account_env_var="GA_COORDINATED_SERVICE_ACCOUNT")
    sh = gc.open_by_url(ZOOM_LINK_SHEET_URL)
    wk = sh.worksheet_by_title(ZOOM_LINK_TAB)
    values = wk.get_all_values(returnas="matrix")

    header = values[1]
    data_rows = values[3:]

    raw_df = pd.DataFrame(data_rows, columns=header)
    raw_df = raw_df[
        raw_df["answer_label"].notna()
        & (raw_df["answer_label"].str.strip() != "")
        & raw_df["event_id"].notna()
        & (raw_df["event_id"].str.strip() != "")
    ]
    return dict(zip(raw_df["answer_label"], raw_df["Link"]))

def get_training_event_shift_map(project_id):
    gc = pygsheets.authorize(service_account_env_var="GA_COORDINATED_SERVICE_ACCOUNT")
    sh = gc.open_by_url(ZOOM_LINK_SHEET_URL)
    wk = sh.worksheet_by_title(ZOOM_LINK_TAB)
    values = wk.get_all_values(returnas="matrix")

    header = values[1]
    data_rows = values[3:]
    raw_df = pd.DataFrame(data_rows, columns=header)
    raw_df = raw_df[
        raw_df["answer_label"].notna()
        & (raw_df["answer_label"].str.strip() != "")
        & raw_df["event_id"].notna()
        & (raw_df["event_id"].str.strip() != "")
    ]

    client = bigquery.Client(project=project_id)
    shifts = client.query(f"""
        SELECT es.event_id, es.event_shift_id
        FROM `your-gcp-project.vansync.event_shifts` es
        JOIN `your-gcp-project.vansync.events` e
          ON es.event_id = e.event_id
        JOIN `your-gcp-project.vansync.events_event_roles` eer
          ON es.event_id = eer.event_id
        WHERE CAST(e.event_type_id AS STRING) = '{TRAINING_EVENT_TYPE_ID}'
          AND CAST(eer.event_role_id AS STRING) = '{TRAINING_ROLE_ID}'
    """).to_dataframe()
    shift_map = dict(zip(shifts["event_id"].astype(str), shifts["event_shift_id"]))

    raw_df["shift_id"] = raw_df["event_id"].str.strip().map(shift_map)
    unmatched = raw_df[raw_df["shift_id"].isna()]
    if len(unmatched) > 0:
        logger.warning(
            f"No VAN training shift found for {len(unmatched)} session(s) "
            f"in the Reminder Emails sheet: {unmatched['answer_label'].tolist()}"
        )

    matched = raw_df.dropna(subset=["shift_id"])
    return dict(zip(
        matched["answer_label"],
        zip(matched["event_id"].astype(int), matched["shift_id"].astype(int))
    ))

def get_foc_status_map(project_id):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT composite_key, vanId, foc_success, foc_attempt_count
        FROM `your-gcp-project.{FOC_STATUS_TABLE}`
    """
    result = client.query(query).to_dataframe()
    return {
        row["composite_key"]: {
            "vanId": row["vanId"],
            "foc_success": row["foc_success"],
            "foc_attempt_count": row["foc_attempt_count"],
        }
        for _, row in result.iterrows()
    }


def construct_foc_payload(data):
    data_lower = {k.lower(): v for k, v in data.items()}
    payload = {}

    if "first_name" in data_lower and data_lower["first_name"]:
        payload["firstName"] = data_lower["first_name"]
    if "last_name" in data_lower and not pd.isna(data_lower["last_name"]):
        payload["lastName"] = data_lower["last_name"]
    if "myc_van_id" in data_lower and not pd.isna(data_lower["myc_van_id"]):
        payload["vanId"] = data_lower["myc_van_id"]

    if (
        "email" in data_lower
        and data_lower["email"]
        and not pd.isna(data_lower["email"])
    ):
        payload["emails"] = [{"email": data_lower["email"]}]

    if (
        "phonenumber" in data_lower
        and data_lower["phonenumber"]
        and not pd.isna(data_lower["phonenumber"])
    ):
        clean_phone = normalize_phone(data_lower["phonenumber"])
        if clean_phone:
            payload["phones"] = [
                {
                    "phoneNumber": clean_phone,
                    "phoneType": "Cell",
                }
            ]
        else:
            logger.warning(
                f"Invalid phone format for {data_lower.get('composite_key')}: "
                f"{data_lower['phonenumber']!r} — creating profile without phone"
            )

    address = {}
    if "streetaddress" in data_lower and not pd.isna(data_lower["streetaddress"]):
        address["addressLine1"] = data_lower["streetaddress"]
    if "city" in data_lower and not pd.isna(data_lower["city"]):
        address["city"] = data_lower["city"]
    if "state_code" in data_lower and not pd.isna(data_lower["state_code"]):
        address["stateOrProvince"] = data_lower["state_code"]
    if "ziporpostalcode" in data_lower and data_lower["ziporpostalcode"] and not pd.isna(data_lower["ziporpostalcode"]):
        address["zipOrPostalCode"] = data_lower["ziporpostalcode"]
    if address:
        address["countryCode"] = "US"
        address["type"] = "Home"
        payload["addresses"] = [address]

    return payload


def validate_record(row):
    data_lower = {k.lower(): v for k, v in row.items()}

    def present(key):
        return key in data_lower and data_lower[key] and not pd.isna(data_lower[key])

    missing = []
    if not present("composite_key"):
        missing.append("composite_key")
    if not present("first_name"):
        missing.append("firstName")
    if not present("last_name"):
        missing.append("lastName")
    if not present("email") and not present("phonenumber"):
        missing.append("email or phone")

    if missing:
        return False, f"missing required fields: {', '.join(missing)}"
    return True, None


def get_auth(state_code):
    api_user = API_USER
    api_key = os.getenv(API_KEY_ENV_VAR) + "|1"
    if api_user is None or api_key is None:
        raise ValueError(f"API credentials not found for state {state_code}")
    encoded = base64.b64encode(f"{api_user}:{api_key}".encode()).decode()
    return encoded


def find_or_create_person(row, max_retries=MAX_RETRIES, initial_wait=INITIAL_WAIT):
    auth = get_auth(row["state_code"])
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {auth}",
    }
    payload = construct_foc_payload(row)
    logger.info(f"[{row['state_code']}] Outgoing payload for {row.get('composite_key')}: {json.dumps(payload)}")
    base_result = {
        "composite_key": row["composite_key"],
        "state_code": row["state_code"],
        "van_attempted": True,
        "vanId": None,
        "response_code": None,
        "error_detail": None,
    }

    last_status_code = None
    last_error_detail = None

    for attempt in range(max_retries):
        try:
            response = requests.post(VAN_FOC_ENDPOINT, json=payload, headers=headers)
            if response.status_code in (200, 201):
                action = "matched" if response.status_code == 200 else "created"
                response_json = response.json()
                van_id = response_json.get("vanId")
                logger.info(f"[{row['state_code']}] Person {action} — vanId={van_id}")
                logger.info(f"[{row['state_code']}] Full response body: {json.dumps(response_json)}")
                return {**base_result, "vanId": van_id, "status": "success", "response_code": response.status_code}
            else:
                error_detail = extract_van_error_detail(response)
                wait = initial_wait * 2**attempt
                logger.warning(
                    f"[{row['state_code']}] Attempt {attempt + 1}/{max_retries} failed "
                    f"(HTTP {response.status_code}): {error_detail}. Retrying in {wait:.1f}s. Payload: {payload}"
                )
                last_status_code = response.status_code
                last_error_detail = error_detail
                time.sleep(wait)
        except requests.RequestException as e:
            wait = initial_wait * 2**attempt
            logger.warning(
                f"[{row['state_code']}] Request error on attempt {attempt + 1}/{max_retries}: {e}. "
                f"Retrying in {wait:.1f}s."
            )
            last_status_code = None
            last_error_detail = str(e)
            time.sleep(wait)

    logger.error(f"[{row['state_code']}] All {max_retries} attempts failed. Payload: {payload}")
    return {
        **base_result,
        "status": "api_error",
        "response_code": last_status_code,
        "error_detail": last_error_detail,
    }


def write_foc_status(status_rows, project_id):
    if not status_rows:
        logger.info("No pw_foc_status rows to write this run")
        return

    status_df = pd.DataFrame(status_rows)
    staging_table = f"{FOC_STATUS_TABLE}_staging_{uuid.uuid4().hex[:8]}"
    client = bigquery.Client(project=project_id)

    # Explicit schema -- without this, pandas_gbq auto-detects staging
    # table types from the dataframe's contents, and a run where every
    # vanId happens to be None (e.g. every FOC call failed) gets
    # inferred as STRING instead of INTEGER, which then fails BigQuery's
    # strict MERGE type checking against the real INT64 column.
    table_schema = [
        {"name": "composite_key", "type": "STRING"},
        {"name": "vanId", "type": "INTEGER"},
        {"name": "foc_success", "type": "BOOLEAN"},
        {"name": "foc_processed_at", "type": "TIMESTAMP"},
        {"name": "foc_attempt_count", "type": "INTEGER"},
        {"name": "foc_last_error", "type": "STRING"},
    ]

    pandas_gbq.to_gbq(
        status_df,
        destination_table=staging_table,
        project_id=project_id,
        if_exists="replace",
        table_schema=table_schema,
        progress_bar=False,
    )

    merge_query = f"""
        MERGE `{project_id}.{FOC_STATUS_TABLE}` T
        USING `{project_id}.{staging_table}` S
        ON T.composite_key = S.composite_key
        WHEN MATCHED THEN UPDATE SET
            vanId = S.vanId,
            foc_success = S.foc_success,
            foc_processed_at = S.foc_processed_at,
            foc_attempt_count = S.foc_attempt_count,
            foc_last_error = S.foc_last_error
        WHEN NOT MATCHED THEN INSERT (composite_key, vanId, foc_success, foc_processed_at, foc_attempt_count, foc_last_error)
        VALUES (composite_key, vanId, foc_success, foc_processed_at, foc_attempt_count, foc_last_error)
    """
    client.query(merge_query).result()
    client.query(f"DROP TABLE `{project_id}.{staging_table}`").result()
    logger.info(f"MERGEd {len(status_df)} row(s) into {FOC_STATUS_TABLE}")


def write_audit_log(audit_rows, project_id):
    if not audit_rows:
        logger.info("No audit log rows to write this run")
        return

    audit_df = pd.DataFrame(audit_rows)
    pandas_gbq.to_gbq(
        audit_df,
        destination_table=AUDIT_LOG_TABLE,
        project_id=project_id,
        if_exists="append",
        progress_bar=False,
    )
    logger.info(f"Appended {len(audit_df)} row(s) to {AUDIT_LOG_TABLE}")


def main(state_code, project_id):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting FOC — run_id={run_id}, source: Google Sheet")
    df, availability_long_df = get_sheet_data()
    logger.info(f"Loaded {len(df)} records from Sheet")
    logger.info(f"Built {len(availability_long_df)} long-format availability rows dynamically from Date/Time columns")

    zoom_link_map = get_zoom_link_map()
    df["zoom_link"] = df["select_training_session"].map(zoom_link_map)
    zoom_matched_count = df["zoom_link"].notna().sum()
    logger.info(f"Attached a zoom_link to {zoom_matched_count} record(s)")
    training_event_shift_map = get_training_event_shift_map(project_id)
    df["training_event_shift"] = df["select_training_session"].map(training_event_shift_map)
    df["training_event_id"] = df["training_event_shift"].apply(lambda x: x[0] if isinstance(x, tuple) else None)
    df["training_shift_id"] = df["training_event_shift"].apply(lambda x: x[1] if isinstance(x, tuple) else None)
    training_matched_count = df["training_event_id"].notna().sum()
    logger.info(f"Attached a training event/shift to {training_matched_count} record(s)")

    election_day_df = availability_long_df[
        availability_long_df["date_category"] == "election_day"
    ][["composite_key", "event_date", "shift_choice"]].rename(columns={
        "shift_choice": "election_day_availability",
        "event_date": "election_day_date",
    })
    mirror_df = df.merge(election_day_df, on="composite_key", how="left")

    mirror_df = mirror_df.replace(r'^\s*$', None, regex=True)

    pandas_gbq.to_gbq(
        mirror_df,
        destination_table=MIRROR_TABLE,
        project_id=project_id,
        if_exists="replace",
        progress_bar=True,
    )
    logger.info(f"Mirrored {len(mirror_df)} intake rows to {MIRROR_TABLE}")

    general_availability_df = availability_long_df[
        availability_long_df["date_category"].isin(["general", "election_day", "runoff_election_day"])
    ][["composite_key", "event_date", "shift_choice"]]

    pandas_gbq.to_gbq(
        general_availability_df,
        destination_table=AVAILABILITY_TABLE,
        project_id=project_id,
        if_exists="replace",
        progress_bar=True,
    )
    logger.info(f"Wrote {len(general_availability_df)} rows to {AVAILABILITY_TABLE}")

    status_map = get_foc_status_map(project_id)
    df["prior_foc_success"] = df["composite_key"].map(lambda k: status_map.get(k, {}).get("foc_success", False))
    df["prior_foc_attempt_count"] = df["composite_key"].map(lambda k: status_map.get(k, {}).get("foc_attempt_count", 0))
    df["myc_van_id"] = df["composite_key"].map(lambda k: status_map.get(k, {}).get("vanId"))

    already_successful = df["prior_foc_success"].fillna(False)
    at_ceiling = (~already_successful) & (df["prior_foc_attempt_count"].fillna(0) >= MAX_CROSS_RUN_ATTEMPTS)
    eligible_mask = (~already_successful) & (~at_ceiling)

    logger.info(f"{already_successful.sum()} record(s) already FOC-successful — skipping")
    logger.info(f"{at_ceiling.sum()} record(s) at the {MAX_CROSS_RUN_ATTEMPTS}-attempt ceiling — skipping, needs manual review")
    logger.info(f"{eligible_mask.sum()} record(s) eligible for a FOC attempt this run")

    eligible_df = df[eligible_mask].copy()

    duplicate_keys = set(
        eligible_df["composite_key"][eligible_df["composite_key"].duplicated(keep=False)].tolist()
    )
    if duplicate_keys:
        logger.error(f"Duplicate composite_key values found — these records will be skipped: {duplicate_keys}")

    status_rows = []
    audit_rows = []

    for index, row in eligible_df.iterrows():
        logger.debug(f"Processing record {index + 1}/{len(eligible_df)}")

        composite_key = row.get("composite_key")
        prior_attempt_count = int(row.get("prior_foc_attempt_count") or 0)

        if composite_key in duplicate_keys:
            audit_rows.append({
                "composite_key": composite_key, "stage": "foc", "run_id": run_id,
                "run_datetime": run_datetime, "success": False, "http_status_code": None,
                "error_detail": "duplicate composite_key this run", "attempt_number": prior_attempt_count + 1,
            })
            continue

        valid, reason = validate_record(row)
        if not valid:
            logger.error(f"[{row.get('state_code')}] Record {composite_key} failed validation — {reason}")
            status_rows.append({
                "composite_key": composite_key, "vanId": None, "foc_success": False,
                "foc_processed_at": run_datetime, "foc_attempt_count": prior_attempt_count + 1,
                "foc_last_error": reason,
            })
            audit_rows.append({
                "composite_key": composite_key, "stage": "foc", "run_id": run_id,
                "run_datetime": run_datetime, "success": False, "http_status_code": None,
                "error_detail": reason, "attempt_number": prior_attempt_count + 1,
            })
            continue

        result = find_or_create_person(row)
        succeeded = result["status"] == "success"
        status_rows.append({
            "composite_key": composite_key, "vanId": result.get("vanId"), "foc_success": succeeded,
            "foc_processed_at": run_datetime, "foc_attempt_count": prior_attempt_count + 1,
            "foc_last_error": None if succeeded else result.get("error_detail"),
        })
        audit_rows.append({
            "composite_key": composite_key, "stage": "foc", "run_id": run_id,
            "run_datetime": run_datetime, "success": succeeded,
            "http_status_code": result.get("response_code"),
            "error_detail": result.get("error_detail"), "attempt_number": prior_attempt_count + 1,
        })

    success_count = sum(1 for r in status_rows if r["foc_success"])
    fail_count = len(status_rows) - success_count
    logger.info("=" * 60)
    logger.info("  FOC RUN COMPLETE")
    logger.info(f"  Run ID:      {run_id}")
    logger.info(f"  Success:     {success_count}")
    logger.info(f"  Failed:      {fail_count}")
    logger.info(f"  Skipped:     {len(duplicate_keys)}")
    logger.info("=" * 60)

    write_foc_status(status_rows, project_id)
    write_audit_log(audit_rows, project_id)


if __name__ == "__main__":
    main(
        state_code=state_code,
        project_id=BQ_PROJECT_ID,
    )