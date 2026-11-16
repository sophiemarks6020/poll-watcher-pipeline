import os
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

# RESCHEDULE PIPELINE — AC_STATUS_TABLE stays the SAME shared table as
# the original intake pipeline on purpose: whether an activist code has
# already been applied is a property of the PERSON (composite_key), not
# of which sheet they submitted through. AUDIT_LOG_TABLE is separate so
# reschedule-pipeline runs don't mix into the original intake audit
# trail (matches new_foc.py's reschedule version).
AC_STATUS_TABLE = "commons.pw_ac_status"  # SHARED with original intake pipeline
AUDIT_LOG_TABLE = "commons.pw_reschedule_pipeline_audit_log"  # separate APPEND table

OUT_OF_STATE_AC_ID = 5361384
OUT_OF_STATE_COUNTY_VALUE = "I'm not a GA Resident"
VOPRO_VOL_AC_ID = 5361436


def get_language_activist_codes(project_id):
    client = bigquery.Client(project=project_id)
    query = """
        SELECT form_value, activist_code_id
        FROM `your-gcp-project.commons.pw_activist_code_map`
        WHERE form_field = 'language'
    """
    result = client.query(query).to_dataframe()
    return dict(zip(result["form_value"], result["activist_code_id"]))


def get_ac_candidates(project_id):
    client = bigquery.Client(project=project_id)
    # CHANGED — RESCHEDULE PIPELINE: joins pw_reschedule_mirror instead
    # of pw_intake_mirror, since this pipeline's form answers land in
    # the reschedule-specific mirror table, not the original intake one.
    # pw_foc_status is still the shared table on purpose (see note above
    # AC_STATUS_TABLE).
    query = """
        SELECT
            s.composite_key,
            s.vanId,
            m.state_code,
            m.language,
            m.county_of_residence
        FROM `your-gcp-project.commons.pw_foc_status` s
        JOIN `your-gcp-project.commons.pw_reschedule_mirror` m
          ON s.composite_key = m.composite_key
        WHERE s.foc_success = TRUE
    """
    df = client.query(query).to_dataframe()

    language_ac_map = get_language_activist_codes(project_id)
    df["language_ac_id"] = df["language"].map(language_ac_map)
    df["is_out_of_state"] = df["county_of_residence"] == OUT_OF_STATE_COUNTY_VALUE

    return df


def get_already_applied(project_id, status_table):
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT composite_key, activist_code_id
        FROM `your-gcp-project.{status_table}`
        WHERE ac_applied = TRUE
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{status_table} doesn't exist yet — treating as first run")
        return set()
    return set(zip(result["composite_key"], result["activist_code_id"]))


def get_auth(state_code):
    api_user = API_USER
    api_key = os.getenv(API_KEY_ENV_VAR) + "|1"
    if api_user is None or api_key is None:
        raise ValueError(f"API credentials not found for state {state_code}")
    encoded = base64.b64encode(f"{api_user}:{api_key}".encode()).decode()
    return encoded


def apply_activist_code(van_id, activist_code_id, auth):
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {auth}",
    }
    url = f"https://api.securevan.com/v4/people/{van_id}/canvassResponses"
    payload = {
        "canvassContext": {"omitActivistCodeContactHistory": True},
        "responses": [{
            "type": "ActivistCode",
            "action": "Apply",
            "activistCodeId": activist_code_id,
        }],
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


def build_candidate_codes(row):
    codes = [VOPRO_VOL_AC_ID]
    if pd.notna(row.get("language_ac_id")):
        codes.append(int(row["language_ac_id"]))
    if bool(row.get("is_out_of_state")):
        codes.append(OUT_OF_STATE_AC_ID)
    return codes


def process_person(row, auth, already_applied, run_id, run_datetime):
    status_rows = []
    audit_rows = []
    composite_key, van_id = row["composite_key"], row["vanId"]

    for ac_id in build_candidate_codes(row):
        if (composite_key, ac_id) in already_applied:
            continue

        response = apply_activist_code(van_id, ac_id, auth)
        succeeded = response.ok
        error_detail = None if succeeded else extract_van_error_detail(response)

        if succeeded:
            logger.info(f"[{row['state_code']}] Applied AC {ac_id} to vanId={van_id}")
        else:
            logger.warning(f"[{row['state_code']}] Failed to apply AC {ac_id} to vanId={van_id}: {error_detail}")

        status_rows.append({
            "composite_key": composite_key,
            "activist_code_id": ac_id,
            "ac_applied": succeeded,
            "ac_processed_at": run_datetime,
            "ac_last_error": error_detail,
        })
        audit_rows.append({
            "composite_key": composite_key, "stage": "ac", "run_id": run_id,
            "run_datetime": run_datetime, "success": succeeded,
            "http_status_code": response.status_code, "error_detail": error_detail,
            "attempt_number": 1,
        })

    return status_rows, audit_rows


def write_ac_status(status_rows, project_id, status_table):
    """MERGE, same pattern as a teammate's write_foc_status -- update matches in place, insert new, leave everything else untouched."""
    if not status_rows:
        logger.info("No pw_ac_status rows to write this run")
        return

    status_df = pd.DataFrame(status_rows)
    staging_table = f"{status_table}_staging_{uuid.uuid4().hex[:8]}"
    client = bigquery.Client(project=project_id)

    # Explicit schema -- same reasoning as write_foc_status(): without
    # this, pandas_gbq auto-detects staging table types from the
    # dataframe's contents, which can guess wrong (e.g. STRING instead
    # of INTEGER) and fail BigQuery's strict MERGE type checking
    # against the real table's columns.
    table_schema = [
        {"name": "composite_key", "type": "STRING"},
        {"name": "activist_code_id", "type": "INTEGER"},
        {"name": "ac_applied", "type": "BOOLEAN"},
        {"name": "ac_processed_at", "type": "TIMESTAMP"},
        {"name": "ac_last_error", "type": "STRING"},
    ]

    pandas_gbq.to_gbq(
        status_df, destination_table=staging_table,
        project_id=project_id, if_exists="replace",
        table_schema=table_schema, progress_bar=False,
    )

    merge_query = f"""
        MERGE `{project_id}.{status_table}` T
        USING `{project_id}.{staging_table}` S
        ON T.composite_key = S.composite_key AND T.activist_code_id = S.activist_code_id
        WHEN MATCHED THEN UPDATE SET
            ac_applied = S.ac_applied,
            ac_processed_at = S.ac_processed_at,
            ac_last_error = S.ac_last_error
        WHEN NOT MATCHED THEN INSERT (composite_key, activist_code_id, ac_applied, ac_processed_at, ac_last_error)
        VALUES (composite_key, activist_code_id, ac_applied, ac_processed_at, ac_last_error)
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
    logger.info(f"Starting AC — run_id={run_id}")

    df = get_ac_candidates(project_id)
    logger.info(f"Found {len(df)} successfully-FOC'd record(s) to check for activist codes")

    already_applied = get_already_applied(project_id, AC_STATUS_TABLE)
    logger.info(f"Found {len(already_applied)} (composite_key, activist_code_id) pair(s) already applied")

    all_status_rows, all_audit_rows = [], []
    for _, row in df.iterrows():
        auth = get_auth(row["state_code"])
        status_rows, audit_rows = process_person(row, auth, already_applied, run_id, run_datetime)
        all_status_rows.extend(status_rows)
        all_audit_rows.extend(audit_rows)

    success_count = sum(1 for r in all_status_rows if r["ac_applied"])
    logger.info("=" * 60)
    logger.info("  AC RUN COMPLETE")
    logger.info(f"  Applied:     {success_count}")
    logger.info(f"  Failed:      {len(all_status_rows) - success_count}")
    logger.info("=" * 60)

    write_ac_status(all_status_rows, project_id, AC_STATUS_TABLE)
    write_audit_log(all_audit_rows, project_id, AUDIT_LOG_TABLE)


if __name__ == "__main__":
    main(state_code=state_code, project_id=BQ_PROJECT_ID)