import os
import re
import json
import base64
import logging
import uuid
import requests
import pandas as pd
import pandas_gbq
from datetime import datetime, timezone
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

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

SQ_STATUS_TABLE = "commons.pw_sq_status"
AUDIT_LOG_TABLE = "commons.pw_pipeline_audit_log"

SOC_SQ_ID = 756731
COUNTY_RESPONSE_IDS = {
    "Bartow": 3035660, "Bulloch": 3035661, "Carroll": 3035662, "Cherokee": 3035663,
    "Dade": 3035664, "Fannin": 3035665, "Fayette": 3035666, "Floyd": 3035667,
    "Glynn": 3035668, "Lowndes": 3035669, "Morgan": 3035670, "Muscogee": 3035671,
    "Rabun": 3035673, "Towns": 3035675, "Union": 3035677,
}

EDAY_SHIFT_TO_RESPONSE = {
    "MORNING shift (6:45am to 1:00pm)": "AM",
    "AFTERNOON/EVENING shift (1:00pm until 7:15pm)": "PM",
    "FULL DAY (6:45am to 7:15pm) (STRONGLY preferred)": "All Day",
}

TRAINING_VERIFIED_VALUE = "Yes, please verify and excuse me"


def normalize_travel_distance(value):
    if not value or pd.isna(value):
        return None
    v = str(value).strip()
    if v.lower().startswith("up to "):
        v = v[len("up to "):].strip()
    if "anywhere" in v.lower():
        return "Anywhere Statewide"
    return v


def get_sq_map(project_id):
    client = bigquery.Client(project=project_id)
    query = """
        SELECT form_field, sq_id, response_name, response_id
        FROM `your-gcp-project.commons.pw_survey_question_mapping`
        WHERE sq_id IS NOT NULL AND response_id IS NOT NULL
    """
    result = client.query(query).to_dataframe()
    sq_map = {}
    for _, row in result.iterrows():
        # Cast to int here, at the source -- pw_survey_question_mapping
        # stores sq_id/response_id as STRING, so without this every
        # non-SOC pair built from this map is a string tuple. That
        # silently broke dedup: current_status (read back from
        # pw_sq_status, genuine ints) never matched a string-keyed
        # pair, so every non-SOC SQ looked "new" and got reapplied
        # every single run. Only the hardcoded SOC_SQ_ID pair (a real
        # Python int already) deduped correctly, which is exactly what
        # this run's log shows.
        sq_map.setdefault(row["form_field"], {})[row["response_name"]] = (
            int(row["sq_id"]), int(row["response_id"])
        )
    return sq_map


def get_sq_candidates(project_id):
    """
    TEST ONLY: joins against pw_intake_mirror_test. Swap back to
    pw_intake_mirror for the real cutover.
    """
    client = bigquery.Client(project=project_id)
    query = """
        SELECT
            s.composite_key,
            s.vanId,
            m.state_code,
            m.desired_travel_distance,
            m.sms_opt_in,
            m.training_verification,
            m.election_day_availability,
            m.county_of_residence
        FROM `your-gcp-project.commons.pw_foc_status` s
        JOIN `your-gcp-project.commons.pw_intake_mirror` m
          ON s.composite_key = m.composite_key
        WHERE s.foc_success = TRUE
    """
    return client.query(query).to_dataframe()


def get_availability_flags(project_id):
    client = bigquery.Client(project=project_id)
    query = """
        SELECT DISTINCT composite_key
        FROM `your-gcp-project.commons.pw_availability_long`
    """
    result = client.query(query).to_dataframe()
    return set(result["composite_key"])


def build_sq_response_list(row, sq_map, evip_keys):
    pairs = []

    travel = row.get("desired_travel_distance")
    if travel and not pd.isna(travel):
        normalized_travel = normalize_travel_distance(travel)
        match = sq_map.get("travel_distance", {}).get(normalized_travel)
        if match:
            pairs.append(match)
        else:
            logger.warning(f"No SQ match for travel_distance value: {travel!r} (normalized: {normalized_travel!r})")

    sms = row.get("sms_opt_in")
    if sms and not pd.isna(sms) and str(sms).strip() != "":
        match = sq_map.get("sms_opt_in", {}).get("Opted In")
        if match:
            pairs.append(match)
        else:
            logger.warning("sms_opt_in present on form but no 'Opted In' SQ match found")

    eday_shift = row.get("election_day_availability")
    has_eday = bool(eday_shift) and not pd.isna(eday_shift)
    if has_eday:
        eday_response = EDAY_SHIFT_TO_RESPONSE.get(eday_shift)
        if eday_response:
            match = sq_map.get("election_day_availability", {}).get(eday_response)
            if match:
                pairs.append(match)
            else:
                logger.warning(f"No SQ match for election_day_availability response: {eday_response!r}")
        else:
            logger.warning(f"Unrecognized Election Day shift text, no mapping: {eday_shift!r}")

    # SOC volunteers don't fill out availability at all (the intake form
    # skips that section for them), so has_evip/has_eday are always False
    # for them structurally -- not a signal of real unavailability. Per
    # team decision (automations meeting), every SOC volunteer gets BOTH
    # role-pipeline SQs unconditionally, same as the original bulk
    # migration. Computed once here and reused below for the county SQ
    # itself, so there's a single source of truth for SOC status instead
    # of two separate checks that could drift apart.

    county = row.get("county_of_residence")
    soc_response_id = COUNTY_RESPONSE_IDS.get(county) if county and not pd.isna(county) else None
    is_soc = soc_response_id is not None

    has_evip = row["composite_key"] in evip_keys
    role_pipeline_map = {
        "role_pipeline_early_vote": has_evip or is_soc,
        "role_pipeline_election_day": has_eday or is_soc,
    }
    for sq_field, has_availability in role_pipeline_map.items():
        if has_availability:
            match = sq_map.get(sq_field, {}).get("Prospect")
            if match:
                pairs.append(match)
            else:
                logger.warning(f"No SQ match for {sq_field} response: 'Prospect'")

    if soc_response_id:
        pairs.append((SOC_SQ_ID, soc_response_id))

    return pairs


def get_current_sq_status(project_id, status_table):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT composite_key, sq_id, response_id
        FROM `your-gcp-project.{status_table}`
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{status_table} doesn't exist yet — treating as first run")
        return {}
    return {
        (row["composite_key"], row["sq_id"]): row["response_id"]
        for _, row in result.iterrows()
    }


def get_auth(state_code):
    api_user = API_USER
    api_key = os.getenv(API_KEY_ENV_VAR) + "|1"
    if api_user is None or api_key is None:
        raise ValueError(f"API credentials not found for state {state_code}")
    encoded = base64.b64encode(f"{api_user}:{api_key}".encode()).decode()
    return encoded


def apply_survey_questions(van_id, sq_pairs, auth):
    if not sq_pairs:
        return None
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {auth}",
    }
    url = f"https://api.securevan.com/v4/people/{van_id}/canvassResponses"
    payload = {
        "canvassContext": {"omitActivistCodeContactHistory": True},
        "responses": [
            {"type": "SurveyResponse", "surveyQuestionId": int(sq_id), "surveyResponseId": int(response_id)}
            for sq_id, response_id in sq_pairs
        ],
    }
    return requests.post(url, json=payload, headers=headers)


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


def process_person(row, sq_map, evip_keys, current_status, auth, run_id, run_datetime):
    composite_key, van_id = row["composite_key"], row["vanId"]
    desired_pairs = build_sq_response_list(row, sq_map, evip_keys)

    new_or_changed = [
        (sq_id, response_id) for sq_id, response_id in desired_pairs
        if current_status.get((composite_key, sq_id)) != response_id
    ]
    if not new_or_changed:
        return [], []

    status_rows, audit_rows = [], []

    def record(sq_id, response_id, succeeded, status_code, error_detail):
        status_rows.append({
            "composite_key": composite_key, "sq_id": sq_id, "response_id": response_id,
            "sq_applied": succeeded, "sq_processed_at": run_datetime, "sq_last_error": error_detail,
        })
        audit_rows.append({
            "composite_key": composite_key, "stage": "sq", "run_id": run_id,
            "run_datetime": run_datetime, "success": succeeded, "http_status_code": status_code,
            "error_detail": error_detail, "attempt_number": 1,
        })

    response = apply_survey_questions(van_id, new_or_changed, auth)
    if response.ok:
        logger.info(f"[{row['state_code']}] Applied {len(new_or_changed)} SQ(s) to vanId={van_id}")
        for sq_id, response_id in new_or_changed:
            record(sq_id, response_id, True, response.status_code, None)
        return status_rows, audit_rows

    logger.warning(f"[{row['state_code']}] Batched SQ call failed for vanId={van_id} — retrying individually")
    for sq_id, response_id in new_or_changed:
        single = apply_survey_questions(van_id, [(sq_id, response_id)], auth)
        succeeded = single.ok
        error_detail = None if succeeded else extract_van_error_detail(single)
        if not succeeded:
            logger.warning(f"[{row['state_code']}] SQ ({sq_id}, {response_id}) failed for vanId={van_id}: {error_detail}")
        record(sq_id, response_id, succeeded, single.status_code, error_detail)

    return status_rows, audit_rows


def write_sq_status(status_rows, project_id, status_table):
    if not status_rows:
        logger.info("No pw_sq_status rows to write this run")
        return

    status_df = pd.DataFrame(status_rows)

    # Force native int dtype -- table_schema alone only labels the
    # DESTINATION column's type, it doesn't convert the source data.
    # sq_id/response_id apparently come through as Python strings
    # (likely because pw_survey_question_mapping stores them as STRING
    # in BigQuery), and pyarrow refuses to auto-cast str -> int64 even
    # for numeric-looking strings -- this is what was actually causing
    # the ArrowInvalid error, not a missing schema declaration.
    status_df["sq_id"] = status_df["sq_id"].astype(int)
    status_df["response_id"] = status_df["response_id"].astype(int)

    staging_table = f"{status_table}_staging_{uuid.uuid4().hex[:8]}"
    client = bigquery.Client(project=project_id)

    table_schema = [
        {"name": "composite_key", "type": "STRING"},
        {"name": "sq_id", "type": "INTEGER"},
        {"name": "response_id", "type": "INTEGER"},
        {"name": "sq_applied", "type": "BOOLEAN"},
        {"name": "sq_processed_at", "type": "TIMESTAMP"},
        {"name": "sq_last_error", "type": "STRING"},
    ]

    pandas_gbq.to_gbq(
        status_df, destination_table=staging_table,
        project_id=project_id, if_exists="replace",
        table_schema=table_schema, progress_bar=False,
    )

    merge_query = f"""
        MERGE `{project_id}.{status_table}` T
        USING `{project_id}.{staging_table}` S
        ON T.composite_key = S.composite_key AND T.sq_id = S.sq_id
        WHEN MATCHED THEN UPDATE SET
            response_id = S.response_id,
            sq_applied = S.sq_applied,
            sq_processed_at = S.sq_processed_at,
            sq_last_error = S.sq_last_error
        WHEN NOT MATCHED THEN INSERT (composite_key, sq_id, response_id, sq_applied, sq_processed_at, sq_last_error)
        VALUES (composite_key, sq_id, response_id, sq_applied, sq_processed_at, sq_last_error)
    """
    client.query(merge_query).result()
    client.query(f"DROP TABLE `{project_id}.{staging_table}`").result()
    logger.info(f"MERGEd {len(status_df)} row(s) into {status_table}")


def write_audit_log(audit_rows, project_id, audit_table):
    if not audit_rows:
        logger.info("No audit log rows to write this run")
        return
    audit_df = pd.DataFrame(audit_rows)
    pandas_gbq.to_gbq(
        audit_df, destination_table=audit_table,
        project_id=project_id, if_exists="append", progress_bar=False,
    )
    logger.info(f"Appended {len(audit_df)} row(s) to {audit_table}")


def main(state_code, project_id):
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(f"Starting SQ — run_id={run_id}")

    df = get_sq_candidates(project_id)
    logger.info(f"Found {len(df)} successfully-FOC'd record(s) to check for survey questions")

    sq_map = get_sq_map(project_id)
    evip_keys = get_availability_flags(project_id)
    current_status = get_current_sq_status(project_id, SQ_STATUS_TABLE)

    all_status_rows, all_audit_rows = [], []
    for _, row in df.iterrows():
        auth = get_auth(row["state_code"])
        status_rows, audit_rows = process_person(row, sq_map, evip_keys, current_status, auth, run_id, run_datetime)
        all_status_rows.extend(status_rows)
        all_audit_rows.extend(audit_rows)

    success_count = sum(1 for r in all_status_rows if r["sq_applied"])
    logger.info("=" * 60)
    logger.info("  SQ RUN COMPLETE")
    logger.info(f"  Applied:     {success_count}")
    logger.info(f"  Failed:      {len(all_status_rows) - success_count}")
    logger.info("=" * 60)

    write_sq_status(all_status_rows, project_id, SQ_STATUS_TABLE)
    write_audit_log(all_audit_rows, project_id, AUDIT_LOG_TABLE)


if __name__ == "__main__":
    main(state_code=state_code, project_id=BQ_PROJECT_ID)