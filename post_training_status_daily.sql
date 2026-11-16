CREATE OR REPLACE TABLE `your-gcp-project.commons.pw_post_training_status_daily` AS
WITH events AS (
    SELECT
        event_id,
        event_type_id,
        event_name,
        DATE(datetime_offset_begin, "America/New_York") AS event_date,
        datetime_suppressed
    FROM `your-gcp-project.vansync.events`
    WHERE event_id IN (
        "249008", "248847", "249009", "249010", "249011", "249012", "249013", "249014",
        "249015", "249016", "249017", "249018", "249019", "249020", "249021", "249022",
        "249023", "249024", "249025", "249026", "249027", "249028", "249129"
    )
),

signups AS (
    SELECT myc_van_id, event_id, event_role_id, event_signup_id
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
),

-- SOC check unioned across intake + reschedule mirrors, most recent
-- submission per person wins if they appear in both
all_submissions AS (
    SELECT composite_key, county_of_residence, submission_timestamp
    FROM `your-gcp-project.commons.pw_intake_mirror`
    UNION ALL
    SELECT composite_key, county_of_residence, submission_timestamp
    FROM `your-gcp-project.commons.pw_reschedule_mirror`
),

latest_submission AS (
    SELECT composite_key, county_of_residence
    FROM all_submissions
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY composite_key
        ORDER BY submission_timestamp DESC
    ) = 1
),

soc AS (
    SELECT f.vanId AS myc_van_id
    FROM `your-gcp-project.commons.pw_foc_status` f
    JOIN latest_submission m ON f.composite_key = m.composite_key
    JOIN `your-gcp-project.commons.soc_counties` s ON m.county_of_residence = s.county
    WHERE f.foc_success = TRUE
),

resolved_signups AS (
    SELECT
        e.event_id,
        e.event_type_id,
        e.event_name,
        e.event_date,
        CAST(su.myc_van_id AS INT64) AS myc_van_id,
        p.last_name,
        p.first_name,
        p.middle_name,
        em.email,
        st.event_status_id,
        sr.event_status_name,
        soc.myc_van_id IS NOT NULL AS is_soc,
        ROW_NUMBER() OVER (
            PARTITION BY su.myc_van_id, e.event_id
            ORDER BY st.datetime_modified DESC
        ) AS rn
    FROM events AS e
    JOIN signups AS su ON e.event_id = su.event_id
    LEFT JOIN statuses AS st ON su.event_signup_id = st.event_signup_id
    LEFT JOIN status_ref AS sr ON st.event_status_id = sr.event_status_id
    LEFT JOIN person AS p ON su.myc_van_id = p.myc_van_id
    LEFT JOIN email AS em ON su.myc_van_id = em.myc_van_id
    LEFT JOIN soc ON CAST(su.myc_van_id AS INT64) = soc.myc_van_id
    WHERE 1=1
        AND e.event_date < CURRENT_DATE('America/New_York')
        AND e.event_type_id = "771155"
        AND su.event_role_id = "661061"
        AND e.datetime_suppressed IS NULL
        AND em.email IS NOT NULL
),

completed_non_soc AS (
    SELECT
        CURRENT_DATE('America/New_York') AS snapshot_date,
        CAST(event_id AS INT64) AS event_id,
        event_type_id, event_name, event_date, myc_van_id,
        last_name, first_name, middle_name, email,
        event_status_id, event_status_name, is_soc,
        "Post-training Email 1: Completed" AS template_id
    FROM resolved_signups
    WHERE rn = 1 AND event_status_name = "Completed" AND is_soc = FALSE
),

completed_soc AS (
    SELECT
        CURRENT_DATE('America/New_York') AS snapshot_date,
        CAST(event_id AS INT64) AS event_id,
        event_type_id, event_name, event_date, myc_van_id,
        last_name, first_name, middle_name, email,
        event_status_id, event_status_name, is_soc,
        "Post-training Email 2: SOCs" AS template_id
    FROM resolved_signups
    WHERE rn = 1 AND event_status_name = "Completed" AND is_soc = TRUE
),

no_show AS (
    SELECT
        CURRENT_DATE('America/New_York') AS snapshot_date,
        CAST(event_id AS INT64) AS event_id,
        event_type_id, event_name, event_date, myc_van_id,
        last_name, first_name, middle_name, email,
        event_status_id, event_status_name, is_soc,
        "Post-training Email 3: No Show" AS template_id
    FROM resolved_signups
    WHERE rn = 1 AND event_status_name = "No Show"
)

SELECT * FROM completed_non_soc
UNION ALL
SELECT * FROM completed_soc
UNION ALL
SELECT * FROM no_show;