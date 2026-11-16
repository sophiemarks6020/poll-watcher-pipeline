import base64
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound

import pandas as pd
import pandas_gbq
import requests
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# CONFIGURABLE VARIABLE ASSIGNMENT FROM PARAMETERS AND SECRETS
state_code = os.getenv("state_code")
BQ_PROJECT_ID = os.getenv("project")
BQ_SOURCE_TABLE = os.getenv("input_table_name")
BQ_RESULTS_TABLE = os.getenv("result_table_name")
API_USER = os.getenv("VAN_EXTENDED_API_USER")
API_KEY_ENV_VAR = f"VAN_{state_code}_EXTENDED_TOKEN"
# 1 = MyCampaign
DATABASE_MODE = os.getenv("database_mode", "1")

# ------------------------------------------------------------------------------

VAN_SIGNUPS_ENDPOINT = "https://api.securevan.com/v4/signups"
REQUIRED_COLUMNS = {"VANID", "eventId", "eventShiftId", "roleId", "statusId"}
OPTIONAL_COLUMNS = {"locationId"}
MAX_RETRIES = 3
INITIAL_WAIT = 2.0  # seconds, doubles on each retry

# ==== ADDED — resubmission-cancels-stale-signups feature ====
# Cancelled status, confirmed via Inspect Element on VoteBuilder's Edit
# Participant Status dropdown (<option value="5">Cancelled</option>).
CANCELLED_STATUS_ID = 5
SIGNUP_STATUS_TABLE = "commons.pw_signup_status"
PENDING_STALE_TABLE = "commons.pw_stale_signup_pending"
MAX_CREATE_ATTEMPTS = 3
# ==== ADDED — category-scoped cancellation ====
# Same constant new_foc.py uses to identify training events in VAN.
TRAINING_EVENT_TYPE_ID = "771155"
# ==============================================================


def get_auth() -> str:
    if state_code is None:
        raise ValueError("Environment variable 'state_code' is not set")
    api_key = os.getenv(API_KEY_ENV_VAR)
    if api_key is None:
        raise ValueError(f"Environment variable '{API_KEY_ENV_VAR}' is not set")
    encoded = base64.b64encode(
        f"{API_USER}:{api_key}|{DATABASE_MODE}".encode()
    ).decode()
    return encoded


def get_bq_data(table: str) -> pd.DataFrame:
    client = bigquery.Client()
    df = client.query_and_wait(f"SELECT * FROM {table}").to_dataframe()
    client.close()
    return df


def validate_dataframe(df: pd.DataFrame, required_columns: set) -> None:
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    logger.info(f"Validation passed — {len(df)} rows, columns: {list(df.columns)}")


def extract_van_error_detail(response: requests.Response) -> str:
    """Turn VAN's error JSON into a compact, readable string.

    Expects a shape like:
    {"errors": [{"code": "INVALID_PARAMETER", "text": "...", "properties": ["shift"]}]}
    Falls back to raw response text if the body isn't JSON or doesn't
    match that shape, so we never lose information even if VAN sends
    something unexpected.
    """
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


def create_signup(
    auth: str,
    vanid: int,
    event_id: int,
    event_shift_id: int,
    role_id: int,
    status_id: int,
    location_id: int | None = None,
    max_retries: int = MAX_RETRIES,
    initial_wait: float = INITIAL_WAIT,
) -> int:
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Basic {auth}",
    }
    payload = {
        "person": {"vanId": vanid},
        "event": {"eventId": event_id},
        "shift": {"eventShiftId": event_shift_id},
        "role": {"roleId": role_id},
        "status": {"statusId": status_id},
    }
    if location_id is not None:
        payload["location"] = {"locationId": location_id}

    # set to default message as a fallback
    last_error_detail = "No response received"

    for attempt in range(max_retries):
        try:
            response = requests.post(
                VAN_SIGNUPS_ENDPOINT, json=payload, headers=headers
            )
            if response.status_code == 201:
                return response.json()

            # pull VAN's error detail out of the response body
            last_error_detail = extract_van_error_detail(response)

            wait = initial_wait * 2**attempt
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} failed "
                f"(HTTP {response.status_code}): {last_error_detail}. "
                f"Retrying in {wait:.1f}s."
            )
            time.sleep(wait)
        except requests.RequestException as e:
            last_error_detail = str(e)
            wait = initial_wait * 2**attempt
            logger.warning(
                f"Request error on attempt {attempt + 1}/{max_retries}"
                f": {e}. Retrying in {wait:.1f}s."
            )
            time.sleep(wait)

    raise RuntimeError(
        f"All {max_retries} attempts to create signup failed. "
        f"Last error from VAN: {last_error_detail}"
    )


def get_currently_scheduled_signups(project_id: str, van_ids: list) -> pd.DataFrame:
    """
    CHANGED — reads from OUR OWN pw_signup_status instead of vansync.
    vansync is a periodically-synced mirror of VAN (confirmed lagging
    behind live VAN by hours), so it can be stale in both directions:
    missing signups we just created, and still showing signups we
    already cancelled as if they were live. Since pw_signup_status is
    written by this same script the moment a create/cancel call
    succeeds, it has no sync lag for anything our own pipeline touched
    -- it reflects the true current state immediately.
    """
    if not van_ids:
        return pd.DataFrame(
            columns=["myc_van_id", "event_id", "event_shift_id", "event_role_id"]
        )

    client = bigquery.Client(project=project_id)
    van_id_list = ", ".join(van_ids)
    query = f"""
        SELECT DISTINCT
            vanid AS myc_van_id,
            event_id,
            event_shift_id,
            role_id AS event_role_id
        FROM `{project_id}.{SIGNUP_STATUS_TABLE}`
        WHERE vanid IN ({van_id_list})
          AND status_id = 1
          AND applied = TRUE
    """
    return client.query(query).to_dataframe()

# ==== ADDED — category-scoped cancellation ====
def get_event_categories(project_id: str, event_ids: list) -> dict:
    """
    Maps each eventId to 'training' or 'pollwatching' based on VAN's
    event_type_id -- the same source new_foc.py already trusts for this
    distinction. Used to scope stale-signup cancellation to only the
    categories actually touched by this run's submissions, so a
    poll-watching-only resubmission never cancels an untouched training
    signup, and vice versa. Works identically regardless of which
    upstream script (new_foc.py or the future reschedule form script)
    produced the desired rows, since categorization happens here, not
    upstream.

    """
    if not event_ids:
        return {}
    client = bigquery.Client(project=project_id)
    event_id_list = ", ".join(f"'{e}'" for e in event_ids)
    query = f"""
        SELECT event_id, event_type_id
        FROM `your-gcp-project.vansync.events`
        WHERE event_id IN ({event_id_list})
    """
    result = client.query(query).to_dataframe()
    return {
        str(row["event_id"]): (
            "training" if str(row["event_type_id"]) == TRAINING_EVENT_TYPE_ID
            else "pollwatching"
        )
        for _, row in result.iterrows()
    }
# ================================================

def get_pending_stale(project_id: str, status_table: str) -> dict:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT vanid, event_id, event_shift_id, role_id, first_flagged_run_id
        FROM `{project_id}.{status_table}`
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{status_table} doesn't exist yet — treating as no pending stale signups")
        return {}
    return {
        (
            str(row["vanid"]), str(row["event_id"]),
            str(row["event_shift_id"]), str(row["role_id"]),
        ): row["first_flagged_run_id"]
        for _, row in result.iterrows()
    }


def write_pending_stale(rows: list, project_id: str, status_table: str) -> None:
    if not rows:
        logger.info("No new pending-stale rows to write this run")
        return

    status_df = pd.DataFrame(rows)
    staging_table = f"{status_table}_staging_{uuid.uuid4().hex[:8]}"
    client = bigquery.Client(project=project_id)

    table_schema = [
        {"name": "vanid", "type": "INTEGER"},
        {"name": "event_id", "type": "INTEGER"},
        {"name": "event_shift_id", "type": "INTEGER"},
        {"name": "role_id", "type": "INTEGER"},
        {"name": "first_flagged_run_id", "type": "STRING"},
        {"name": "first_flagged_at", "type": "TIMESTAMP"},
        {"name": "attempt_count", "type": "INTEGER"},
    ]

    table_ref = bigquery.TableReference.from_string(f"{project_id}.{status_table}")
    bq_schema = [bigquery.SchemaField(f["name"], f["type"]) for f in table_schema]
    client.create_table(bigquery.Table(table_ref, schema=bq_schema), exists_ok=True)

    pandas_gbq.to_gbq(
        status_df, destination_table=staging_table,
        project_id=project_id, if_exists="replace",
        table_schema=table_schema, progress_bar=False,
    )

    merge_query = f"""
        MERGE `{project_id}.{status_table}` T
        USING `{project_id}.{staging_table}` S
        ON T.vanid = S.vanid AND T.event_id = S.event_id
           AND T.event_shift_id = S.event_shift_id AND T.role_id = S.role_id
        WHEN NOT MATCHED THEN
          INSERT (vanid, event_id, event_shift_id, role_id, first_flagged_run_id, first_flagged_at, attempt_count)
          VALUES (vanid, event_id, event_shift_id, role_id, first_flagged_run_id, first_flagged_at, attempt_count)
    """
    client.query(merge_query).result()
    client.query(f"DROP TABLE `{project_id}.{staging_table}`").result()
    logger.info(f"Flagged {len(status_df)} signup(s) as newly-pending-stale in {status_table}")


def delete_pending_stale(keys: list, project_id: str, status_table: str) -> None:
    if not keys:
        return
    client = bigquery.Client(project=project_id)
    conditions = " OR ".join(
        f"(vanid = {v} AND event_id = {e} AND event_shift_id = {es} AND role_id = {r})"
        for v, e, es, r in keys
    )
    client.query(f"""
        DELETE FROM `{project_id}.{status_table}`
        WHERE {conditions}
    """).result()
    logger.info(f"Cleared {len(keys)} pending-stale flag(s) from {status_table}")

def cancel_stale_signups(
    df: pd.DataFrame, project_id: str, auth: str, run_id: str, run_datetime: str,
    current_status: dict,
) -> tuple[list, list]:
    van_ids = df["VANID"].astype(str).unique().tolist()
    currently_scheduled = get_currently_scheduled_signups(project_id, van_ids)

    # Build per-person desired sets and per-person desired categories,
    # instead of one global set shared across the whole batch.
    desired_by_vanid = {}
    desired_categories_by_vanid = {}

    all_event_ids = set(df["eventId"].astype(str).unique()) | set(
        currently_scheduled["event_id"].astype(str).unique()
    )
    event_categories = get_event_categories(project_id, list(all_event_ids))

    for vanid_str, group in df.groupby(df["VANID"].astype(str)):
        desired_by_vanid[vanid_str] = set(
            zip(
                group["VANID"].astype(str),
                group["eventId"].astype(str),
                group["eventShiftId"].astype(str),
            )
        )
        desired_categories_by_vanid[vanid_str] = {
            event_categories[eid]
            for eid in group["eventId"].astype(str).unique()
            if eid in event_categories
        }

    cancel_results = []
    cancel_status_rows = []

    for _, row in currently_scheduled.iterrows():
        vanid_str = str(row["myc_van_id"])
        event_id_str = str(row["event_id"])
        event_shift_id_str = str(row["event_shift_id"])
        role_id_str = str(row["event_role_id"])

        # Scope to THIS person's own submitted categories, not the batch's.
        person_desired_categories = desired_categories_by_vanid.get(vanid_str, set())
        person_desired = desired_by_vanid.get(vanid_str, set())

        category = event_categories.get(event_id_str)
        if category is None or category not in person_desired_categories:
            continue

        if (vanid_str, event_id_str, event_shift_id_str) in person_desired:
            continue

        already_cancelled_key = (vanid_str, event_id_str, event_shift_id_str, role_id_str)
        if current_status.get(already_cancelled_key) == str(CANCELLED_STATUS_ID):
            continue

        try:
            signup_id = create_signup(
                auth=auth,
                vanid=int(vanid_str),
                event_id=int(event_id_str),
                event_shift_id=int(event_shift_id_str),
                role_id=int(row["event_role_id"]),
                status_id=CANCELLED_STATUS_ID,
            )
            logger.info(
                f"Cancelled stale signup: vanId={vanid_str}, "
                f"eventId={event_id_str}, eventShiftId={event_shift_id_str}"
            )
            cancel_results.append(
                {
                    "run_id": run_id, "run_datetime": run_datetime, "state_code": state_code,
                    "status": "cancelled", "signup_id": str(signup_id), "error_message": "",
                    "VANID": int(vanid_str), "eventId": int(event_id_str),
                    "eventShiftId": int(event_shift_id_str), "roleId": int(row["event_role_id"]),
                    "statusId": CANCELLED_STATUS_ID,
                }
            )
            cancel_status_rows.append({
                "vanid": int(vanid_str), "event_id": int(event_id_str),
                "event_shift_id": int(event_shift_id_str), "role_id": int(row["event_role_id"]),
                "status_id": CANCELLED_STATUS_ID, "applied": True,
                "processed_at": run_datetime, "last_error": None,
                "attempt_count": 0,
            })
        except Exception as e:
            logger.error(
                f"Failed to cancel stale signup for VANID {vanid_str}, "
                f"eventId={event_id_str}: {e}"
            )
            cancel_results.append(
                {
                    "run_id": run_id, "run_datetime": run_datetime, "state_code": state_code,
                    "status": "cancel_failed", "signup_id": "", "error_message": str(e),
                    "VANID": int(vanid_str), "eventId": int(event_id_str),
                    "eventShiftId": int(event_shift_id_str), "roleId": int(row["event_role_id"]),
                    "statusId": CANCELLED_STATUS_ID,
                }
            )
            cancel_status_rows.append({
                "vanid": int(vanid_str), "event_id": int(event_id_str),
                "event_shift_id": int(event_shift_id_str), "role_id": int(row["event_role_id"]),
                "status_id": CANCELLED_STATUS_ID, "applied": False,
                "processed_at": run_datetime, "last_error": str(e),
                "attempt_count": 0,
            })

    logger.info(
        f"Stale-signup cancellation: {len(cancel_results)} attempted "
        f"({sum(1 for r in cancel_results if r['status'] == 'cancelled')} succeeded)"
    )
    return cancel_results, cancel_status_rows

def write_results_to_bq(
    results_df: pd.DataFrame, project_id: str, destination: str
) -> None:
    pandas_gbq.to_gbq(
        results_df,
        destination_table=destination,
        project_id=project_id,
        if_exists="append",
        progress_bar=True,
    )

def get_attempt_counts(project_id: str, status_table: str) -> dict:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT vanid, event_id, event_shift_id, role_id, attempt_count
        FROM `{project_id}.{status_table}`
        WHERE applied = FALSE
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        return {}
    return {
        (
            str(row["vanid"]), str(row["event_id"]),
            str(row["event_shift_id"]), str(row["role_id"]),
        ): int(row["attempt_count"]) if pd.notna(row["attempt_count"]) else 0
        for _, row in result.iterrows()
    }

def get_current_signup_status(project_id: str, status_table: str) -> dict:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT vanid, event_id, event_shift_id, role_id, status_id
        FROM `{project_id}.{status_table}`
        WHERE applied = TRUE
    """
    try:
        result = client.query(query).to_dataframe()
    except NotFound:
        logger.info(f"{status_table} doesn't exist yet — treating as first run")
        return {}
    return {
        (
            str(row["vanid"]), str(row["event_id"]),
            str(row["event_shift_id"]), str(row["role_id"]),
        ): str(row["status_id"])
        for _, row in result.iterrows()
    }

def write_signup_status(status_rows: list, project_id: str, status_table: str) -> None:
    if not status_rows:
        logger.info("No signup status updates to write this run")
        return

    status_df = pd.DataFrame(status_rows)

    # ==== ADDED — MERGE rejects more than one source row per target row,
    # so collapse duplicate keys (latest wins) before staging ====
    key_cols = ["vanid", "event_id", "event_shift_id", "role_id"]
    dupes = status_df.duplicated(key_cols, keep="last")
    if dupes.any():
        logger.warning(
            f"{dupes.sum()} duplicate signup status row(s) for the same "
            f"vanid/event_id/event_shift_id/role_id — keeping the latest for each"
        )
        status_df = status_df[~dupes]
    # ================================================================

    staging_table = f"{status_table}_staging_{uuid.uuid4().hex[:8]}"
    client = bigquery.Client(project=project_id)

    table_schema = [
        {"name": "vanid", "type": "INTEGER"},
        {"name": "event_id", "type": "INTEGER"},
        {"name": "event_shift_id", "type": "INTEGER"},
        {"name": "role_id", "type": "INTEGER"},
        {"name": "status_id", "type": "INTEGER"},
        {"name": "applied", "type": "BOOLEAN"},
        {"name": "processed_at", "type": "TIMESTAMP"},
        {"name": "last_error", "type": "STRING"},
        {"name": "attempt_count", "type": "INTEGER"},
    ]

    table_ref = bigquery.TableReference.from_string(f"{project_id}.{status_table}")
    bq_schema = [bigquery.SchemaField(f["name"], f["type"]) for f in table_schema]
    client.create_table(bigquery.Table(table_ref, schema=bq_schema), exists_ok=True)

    pandas_gbq.to_gbq(
        status_df, destination_table=staging_table,
        project_id=project_id, if_exists="replace",
        table_schema=table_schema, progress_bar=False,
    )

    merge_query = f"""
        MERGE `{project_id}.{status_table}` T
        USING `{project_id}.{staging_table}` S
        ON T.vanid = S.vanid AND T.event_id = S.event_id
           AND T.event_shift_id = S.event_shift_id AND T.role_id = S.role_id
        WHEN MATCHED THEN UPDATE SET
            status_id = S.status_id,
            applied = S.applied,
            processed_at = S.processed_at,
            last_error = S.last_error,
            attempt_count = S.attempt_count
        WHEN NOT MATCHED THEN INSERT (vanid, event_id, event_shift_id, role_id, status_id, applied, processed_at, last_error, attempt_count)
        VALUES (vanid, event_id, event_shift_id, role_id, status_id, applied, processed_at, last_error, attempt_count)
    """
    # ==== CHANGED — always drop the staging table, even if the MERGE fails ====
    try:
        client.query(merge_query).result()
    finally:
        client.query(f"DROP TABLE IF EXISTS `{project_id}.{staging_table}`").result()
    logger.info(f"MERGEd {len(status_df)} signup status row(s) into {status_table}")


def main(bq_table: str, project_id: str, destination_table: str) -> None:
    run_id = str(uuid.uuid4())
    run_datetime = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"Starting VAN signup run — run_id={run_id}, "
        f"source: {bq_table}, destination: {destination_table}"
    )

    df = get_bq_data(bq_table)
    logger.info(f"Loaded {len(df)} records from BigQuery")

    validate_dataframe(df, REQUIRED_COLUMNS)

    # ==== ADDED — duplicate source rows for the same signup would call VAN
    # twice and produce two status rows for one key (breaks the MERGE) ====
    signup_key = ["VANID", "eventId", "eventShiftId", "roleId"]
    dup_mask = df.duplicated(signup_key, keep="first")
    if dup_mask.any():
        logger.warning(
            f"{dup_mask.sum()} duplicate row(s) in {bq_table} for the same "
            f"VANID/eventId/eventShiftId/roleId — ignoring the extras"
        )
        df = df[~dup_mask]
    # ================================================================

    has_location = "locationId" in df.columns
    auth = get_auth()
    results = []

    # ==== CHANGED — fetch current_status BEFORE cancel_stale_signups so
    # the cancel path can check it for already-cancelled signups ====
    current_status = get_current_signup_status(project_id, SIGNUP_STATUS_TABLE)

    cancel_results, cancel_status_rows = cancel_stale_signups(
        df, project_id, auth, run_id, run_datetime, current_status
    )
    results.extend(cancel_results)
    # ================================================================

    def already_applied(row) -> bool:
        key = (str(row["VANID"]), str(row["eventId"]), str(row["eventShiftId"]), str(row["roleId"]))
        return current_status.get(key) == str(row["statusId"])

    attempt_counts = get_attempt_counts(project_id, SIGNUP_STATUS_TABLE)

    def exceeded_attempts(row) -> bool:
        key = (str(row["VANID"]), str(row["eventId"]), str(row["eventShiftId"]), str(row["roleId"]))
        return attempt_counts.get(key, 0) >= MAX_CREATE_ATTEMPTS

    skip_mask = df.apply(already_applied, axis=1)
    ceiling_mask = df.apply(exceeded_attempts, axis=1) & ~skip_mask
    skipped_df = df[skip_mask]
    ceiling_skipped_df = df[ceiling_mask]
    eligible_df = df[~skip_mask & ~ceiling_mask]

    logger.info(f"{skipped_df.shape[0]} row(s) already applied with the same status — skipping")
    logger.info(f"{ceiling_skipped_df.shape[0]} row(s) exceeded {MAX_CREATE_ATTEMPTS} failed attempts — skipping permanently until manually reset")
    logger.info(f"{eligible_df.shape[0]} row(s) eligible for a signup call this run")

    status_rows = list(cancel_status_rows)  # ==== CHANGED — seed with cancel rows ====

    for _, row in eligible_df.iterrows():
        vanid = row["VANID"]
        row_dict = row.to_dict()
        try:
            signup_id = create_signup(
                auth=auth,
                vanid=vanid,
                event_id=row["eventId"],
                event_shift_id=row["eventShiftId"],
                role_id=row["roleId"],
                status_id=row["statusId"],
                location_id=row["locationId"] if has_location else None,
            )
            results.append(
                {
                    "run_id": run_id,
                    "run_datetime": run_datetime,
                    "state_code": state_code,
                    "status": "success",
                    "signup_id": str(signup_id),
                    "error_message": "",
                    **row_dict,
                }
            )
            status_rows.append({
                "vanid": int(row["VANID"]), "event_id": int(row["eventId"]),
                "event_shift_id": int(row["eventShiftId"]), "role_id": int(row["roleId"]),
                "status_id": int(row["statusId"]), "applied": True,
                "processed_at": run_datetime, "last_error": None,
                "attempt_count": 0,
            })
        except Exception as e:
            logger.error(f"Failed to create signup for VANID {vanid}: {e}")
            results.append(
                {
                    "run_id": run_id,
                    "run_datetime": run_datetime,
                    "state_code": state_code,
                    "status": "failed",
                    "signup_id": "",
                    "error_message": str(e),
                    **row_dict,
                }
            )
            status_rows.append({
                "vanid": int(row["VANID"]), "event_id": int(row["eventId"]),
                "event_shift_id": int(row["eventShiftId"]), "role_id": int(row["roleId"]),
                "status_id": int(row["statusId"]), "applied": False,
                "processed_at": run_datetime, "last_error": str(e),
                "attempt_count": attempt_counts.get(
                    (str(row["VANID"]), str(row["eventId"]), str(row["eventShiftId"]), str(row["roleId"])), 0
                ) + 1,
            })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        success_count = failed_count = cancelled_count = cancel_failed_count = 0
    else:
        success_count = (results_df["status"] == "success").sum()
        failed_count = (results_df["status"] == "failed").sum()
        cancelled_count = (results_df["status"] == "cancelled").sum()
        cancel_failed_count = (results_df["status"] == "cancel_failed").sum()

    logger.info("=" * 60)
    logger.info("  VAN SIGNUP RUN COMPLETE")
    logger.info(f"  Run ID:         {run_id}")
    logger.info(f"  State:          {state_code}")
    logger.info(f"  Source table:   {bq_table}")
    logger.info("  ------------------------------------------")
    logger.info(f"  Total rows:     {len(results_df)}")
    logger.info(f"  Succeeded:      {success_count}")
    logger.info(f"  Failed:         {failed_count}")
    logger.info(f"  Cancelled:      {cancelled_count}")
    logger.info(f"  Cancel failed:  {cancel_failed_count}")
    logger.info("=" * 60)

    write_results_to_bq(
        results_df, project_id=project_id, destination=destination_table
    )
    logger.info("Results written to BigQuery")
    write_signup_status(status_rows, project_id, SIGNUP_STATUS_TABLE)


if __name__ == "__main__":
    main(
        bq_table=BQ_SOURCE_TABLE,
        project_id=BQ_PROJECT_ID,
        destination_table=BQ_RESULTS_TABLE,
    )