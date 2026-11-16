CREATE TABLE IF NOT EXISTS `your-gcp-project.commons.pw_scheduled_trainings_daily` (
    snapshot_date DATE,
    event_id INT64,
    event_type_id STRING,
    event_name STRING,
    event_date DATE,
    event_shift_id INT64,
    shift_start_datetime DATETIME,
    shift_end_datetime DATETIME,
    myc_van_id INT64,
    last_name STRING,
    first_name STRING,
    middle_name STRING,
    email STRING,
    event_role_id STRING,
    event_status_id STRING,
    event_status_name STRING
);

BEGIN TRANSACTION;

DELETE FROM `your-gcp-project.commons.pw_scheduled_trainings_daily`
WHERE snapshot_date = CURRENT_DATE('America/New_York');

INSERT INTO `your-gcp-project.commons.pw_scheduled_trainings_daily`
WITH events AS (
    SELECT
        event_id,
        event_type_id,
        event_name,
        DATE(datetime_offset_begin, "America/New_York") AS event_date,
        event_short_name,
        datetime_suppressed
    FROM `your-gcp-project.vansync.events`
    WHERE event_id IN (
        "248847", "249008", "249009", "249010", "249011", "249012", "249013", "249014",
        "249015", "249016", "249017", "249018", "249019", "249020", "249021", "249022",
        "249023", "249024", "249025", "249026", "249027", "249028"
    )
),

shifts AS (
    SELECT
        event_id,
        event_shift_id,
        event_shift_name,
        DATETIME(datetime_offset_begin, "America/New_York") AS datetime_offset_begin,
        DATETIME(datetime_offset_end, "America/New_York") AS datetime_offset_end
    FROM `your-gcp-project.vansync.event_shifts`
),

signups AS (
    SELECT myc_van_id, event_id, event_role_id, event_shift_id, event_signup_id
    FROM `your-gcp-project.vansync.event_signups`
),

statuses AS (
    SELECT event_signup_id, event_status_id, datetime_modified
    FROM `your-gcp-project.vansync.event_signups_statuses`
),

status_ref AS (
    SELECT event_status_id, event_status_name
    FROM `democrats.vansync_ref.event_statuses`
),

person AS (
    SELECT myc_van_id, last_name, first_name, middle_name
    FROM `your-gcp-project.vansync.person_records_myc`
),

-- Email comes from the volunteer's own form submission (latest of intake vs
-- reschedule, via the same view apply_events reads). Anyone with no training
-- row in that view falls back to their VAN email. The QUALIFY keeps exactly
-- one row per person, so multiple emails in VAN can't fan out this join.
email AS (
    SELECT myc_van_id, email
    FROM (
        SELECT CAST(VANID AS STRING) AS myc_van_id, email, 1 AS source_priority
        FROM `your-gcp-project.commons.pw_shifts_signup_ready_unified`
        WHERE event_date IS NULL AND email IS NOT NULL
        UNION ALL
        SELECT CAST(myc_van_id AS STRING) AS myc_van_id, email, 2 AS source_priority
        FROM `your-gcp-project.vansync.contacts_emails_myc`
        WHERE email IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY myc_van_id ORDER BY source_priority, email) = 1
)

SELECT
    CURRENT_DATE('America/New_York') AS snapshot_date,
    CAST(e.event_id AS INT64) AS event_id,
    e.event_type_id,
    e.event_name,
    e.event_date,
    CAST(s.event_shift_id AS INT64) AS event_shift_id,
    s.datetime_offset_begin AS shift_start_datetime,
    s.datetime_offset_end AS shift_end_datetime,
    CAST(su.myc_van_id AS INT64) AS myc_van_id,
    p.last_name,
    p.first_name,
    p.middle_name,
    em.email,
    su.event_role_id,
    st.event_status_id,
    sr.event_status_name
FROM events AS e
LEFT JOIN shifts AS s ON e.event_id = s.event_id
LEFT JOIN signups AS su ON s.event_shift_id = su.event_shift_id
LEFT JOIN statuses AS st ON su.event_signup_id = st.event_signup_id
LEFT JOIN status_ref AS sr ON st.event_status_id = sr.event_status_id
LEFT JOIN person AS p ON su.myc_van_id = p.myc_van_id
LEFT JOIN email AS em ON su.myc_van_id = em.myc_van_id
WHERE 1=1
    AND e.event_date >= CURRENT_DATE('America/New_York')
    AND e.event_type_id = "771155"
    AND su.event_role_id = "661061"
    AND e.datetime_suppressed IS NULL
    AND sr.event_status_name = "Scheduled"
    AND em.email IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY su.myc_van_id
    ORDER BY st.datetime_modified DESC
) = 1;

COMMIT TRANSACTION;