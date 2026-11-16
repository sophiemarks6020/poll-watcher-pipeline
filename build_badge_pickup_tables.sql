-- build_badge_pickup_tables.sql
--
-- Builds the badge pickup tables Stage 5 (pw_send_credential_emails.py) reads.
-- Run the three steps IN ORDER in the BigQuery console (one at a time).
--
-- Pickup dates/hours below are the October versions supplied 2026-10-07 (DeKalb starts
-- Monday October 12th, Fulton starts Friday October 9th). The 5016 Redcliff Ct hours line
-- is kept EXACTLY as supplied ("Mondays through from 8:00 AM to 10:00 PM") -- it reads
-- incomplete; fix the wording in this file before running Step 1 if it should say more.
--
-- Sites: 5 DeKalb, 3 Fulton (1136 Reeder Cir serves both counties, one row each).
-- Coordinates: known ones are filled in; the three private homes with NULL are looked up
-- from the voter file (person_ga) by street + zip. CHECK step 1b -- any site still showing
-- a NULL latitude has to be fixed by hand (right-click the address in Google Maps).

-- =====================================================================================
-- STEP 1: the sites table (one row per site, with that site's own text)
-- =====================================================================================
CREATE OR REPLACE TABLE `your-gcp-project.commons.pw_badge_pickup_sites` AS
WITH s AS (
  SELECT * FROM UNNEST([
    STRUCT(
      'DeKalb' AS county,
      'Private home - 5016 Redcliff Ct' AS site_name,
      '5016 Redcliff Ct, Dunwoody, GA 30338' AS address,
      '5016 REDCLIFF' AS street_match,
      '30338' AS zip,
      CAST(NULL AS FLOAT64) AS latitude,
      CAST(NULL AS FLOAT64) AS longitude,
      """Your badge is available Beginning Monday October 12th at 3:00 PM, then:
Mondays through from 8:00 AM to 10:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.""" AS time_blurb,
      """Your badge pick up location is
a private home (pick up from bin on front porch, on the table to the right of the front door)
5016 Redcliff Ct, Dunwoody, GA 30338""" AS location_blurb,
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!' AS plan_blurb
    ),
    STRUCT(
      'DeKalb', 'Private home - 1136 Reeder Cir NE', '1136 Reeder Cir NE, Atlanta, GA 30306', '', '30306',
      33.79431194378, -84.349993545717,
      """Your badge is available Beginning Monday October 12th at 3:00 PM, then:
Mondays through Fridays from 8:00 AM - 8:00 PM
Saturdays and Sundays from 10:00 AM to 6:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.""",
      """Your badge pick up location is
a private home (pick up from bin on porch)
1136 Reeder Cir NE Atlanta, GA 30306""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    ),
    STRUCT(
      'DeKalb', 'Private home - 204 Wilton Dr', '204 Wilton Dr, Decatur, GA 30030', '', '30030',
      33.780263621229, -84.300397846694,
      """Your badge is available Beginning Monday October 12th at 11:00 AM, then:
Mondays through Fridays from 8:00 AM - 8:00 PM
Saturdays and Sundays from 10:00 AM to 6:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.""",
      """Your badge pick up location is
a private home (pick up from bin on porch)
204 Wilton Dr. Decatur, GA 30030""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    ),
    STRUCT(
      'DeKalb', 'Private home - 1638 Montcliff Court', '1638 Montcliff Court, Decatur, GA 30033', '1638 MONTCLIFF', '30033',
      CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
      """Your badge is available Beginning Monday October 12th at 3:00 PM, then:
Mondays through Fridays from 8:00 AM - 8:00 PM
Saturdays and Sundays from 10:00 AM to 6:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.""",
      """Your badge pick up location is
a private home (pick up from bin in carport)
1638 Montcliff Court, Decatur 30033""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    ),
    STRUCT(
      'DeKalb', 'Private home - 3846 Evans Rd', '3846 Evans Rd, Atlanta, GA 30340', '3846 EVANS', '30340',
      CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
      """Your badge is available Beginning Monday October 12th at 3:00 PM, then:
Mondays through Fridays from 8:00 AM - 8:00 PM
Saturdays and Sundays from 10:00 AM to 6:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.

IMPORTANT: Pull all the way into the driveway and use the driveway to turn around. Do not back out onto Evans Rd.""",
      """Your badge pick up location is
a private home (pick up from bin in front of garage)
3846 Evans Rd Atlanta, GA 30340""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    ),
    STRUCT(
      'Fulton', 'Private home - 1136 Reeder Cir NE', '1136 Reeder Cir NE, Atlanta, GA 30306', '', '30306',
      33.79431194378, -84.349993545717,
      """Your badge is available Beginning Friday October 9th at 3:00 PM, then:
Mondays through Fridays from 8:00 AM - 8:00 PM
Saturdays and Sundays from 10:00 AM to 6:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.""",
      """Your badge pick up location is
a private home (pick up from bin on porch)
1136 Reeder Cir NE Atlanta, GA 30306""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    ),
    STRUCT(
      'Fulton', 'Private home - 7490 Old Maine Trail', '7490 Old Maine Trail, Atlanta, GA 30328', '', '30328',
      33.959557835855, -84.371840638014,
      """Your badge is available Beginning Friday October 9th at 3:00 PM, then:
Mondays through Fridays from 8:00 AM - 8:00 PM
Saturdays and Sundays from 10:00 AM to 6:00 PM

NOTE: THIS IS A CONTACTLESS PICK UP. Please do not disturb the homeowner when you are picking up your badge.""",
      """Your badge pick up location is
a private home (pick up from bin on porch)
7490 Old Maine Trail, Atlanta, GA 30328""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    ),
    STRUCT(
      'Fulton', 'Fulton County Department of Registration & Elections', '5600 Campbellton Fairburn Rd, Fairburn, GA 30213', '', '30213',
      33.605662, -84.605639,
      'Your badge is available Beginning Friday October 9th at 3:00 PM, then Mondays through Fridays from 9:00 AM - 5:00 PM.',
      """Your badge pick up location is
Fulton County Department of Registration & Elections
5600 Campbellton Fairburn Rd.
Fairburn GA 30213""",
      'YOU MUST PICK UP YOUR BADGE IN ADVANCE AND BRING IT TO YOUR SHIFT!'
    )
  ])
),
looked_up AS (
  -- coordinates for the sites that don't have them yet, from the voter file
  SELECT s2.county, s2.site_name,
         AVG(p.voting_address_latitude) AS lat, AVG(p.voting_address_longitude) AS lon
  FROM s s2
  JOIN `your-gcp-project.dscc_assets.person_ga` p
    ON CAST(p.voting_zip AS STRING) = s2.zip
   AND STARTS_WITH(UPPER(p.voting_street_address), s2.street_match)
  WHERE s2.latitude IS NULL
    AND p.voting_address_latitude IS NOT NULL AND p.voting_address_longitude IS NOT NULL
  GROUP BY s2.county, s2.site_name
)
SELECT s.county, s.site_name, s.address,
       COALESCE(s.latitude, l.lat)  AS latitude,
       COALESCE(s.longitude, l.lon) AS longitude,
       s.time_blurb, s.location_blurb, s.plan_blurb
FROM s
LEFT JOIN looked_up l USING (county, site_name);


-- STEP 1b: CHECK. All 8 rows must have a latitude and longitude. If any is NULL, that
-- private home isn't in the voter file at that address: look up its lat/long in Google
-- Maps and run, e.g.:
--   UPDATE `your-gcp-project.commons.pw_badge_pickup_sites`
--   SET latitude = 33.9, longitude = -84.3
--   WHERE site_name = 'Private home - 5016 Redcliff Ct'
SELECT county, site_name, address, latitude, longitude FROM `your-gcp-project.commons.pw_badge_pickup_sites` ORDER BY county, site_name;


-- =====================================================================================
-- STEP 2: (A) every active Fulton / DeKalb volunteer with their latitude/longitude, then
--         (B) each volunteer's nearest pickup site in THEIR county.
-- Volunteer coordinates come from the voter file (person_ga). The LBJ VAN IDs do NOT match
-- myv_van_id (0 of 340 matched on 2026-10-07), so each volunteer is matched, in this order:
--   1 = myv_van_id   2 = national_myv_van_id   3 = first name + last name + ZIP (from the
--   zip at the end of the volunteer's address). match_source records which one was used.
-- Volunteers with no match are KEPT in table A with NULL coordinates (match_source NULL).
-- Re-run this whole step any time the sites table or the volunteer list changes.
-- =====================================================================================
CREATE OR REPLACE TABLE `your-gcp-project.commons.pw_volunteer_coordinates` AS
WITH st AS (
  SELECT van_id, county, first_name, last_name, address,
         UPPER(TRIM(first_name)) AS fn, UPPER(TRIM(last_name)) AS ln,
         REGEXP_EXTRACT(address, r'(\d{5})(?:-\d{4})?\s*$') AS zip
  FROM `your-gcp-project.commons.pw_credential_status`
  WHERE county IN ('Fulton', 'DeKalb') AND NOT is_removed_from_county
),
matches AS (
  SELECT st.van_id, st.county, p.voting_address_latitude AS lat, p.voting_address_longitude AS lon, 1 AS prio
  FROM st JOIN `your-gcp-project.dscc_assets.person_ga` p ON CAST(p.myv_van_id AS STRING) = st.van_id
  WHERE p.voting_address_latitude IS NOT NULL AND p.voting_address_longitude IS NOT NULL
  UNION ALL
  SELECT st.van_id, st.county, p.voting_address_latitude, p.voting_address_longitude, 2
  FROM st JOIN `your-gcp-project.dscc_assets.person_ga` p ON CAST(p.national_myv_van_id AS STRING) = st.van_id
  WHERE p.voting_address_latitude IS NOT NULL AND p.voting_address_longitude IS NOT NULL
  UNION ALL
  SELECT st.van_id, st.county, p.voting_address_latitude, p.voting_address_longitude, 3
  FROM st JOIN `your-gcp-project.dscc_assets.person_ga` p
    ON UPPER(p.first_name) = st.fn AND UPPER(p.last_name) = st.ln
   AND CAST(p.voting_zip AS STRING) = st.zip
  WHERE p.voting_address_latitude IS NOT NULL AND p.voting_address_longitude IS NOT NULL
),
best AS (
  SELECT van_id, county, lat, lon, prio
  FROM matches
  QUALIFY ROW_NUMBER() OVER (PARTITION BY van_id, county ORDER BY prio) = 1
)
SELECT st.van_id, st.first_name, st.last_name, st.county, st.address AS volunteer_address,
       b.lat AS latitude, b.lon AS longitude,
       CASE b.prio WHEN 1 THEN 'myv_van_id' WHEN 2 THEN 'national_myv_van_id' WHEN 3 THEN 'name_zip' END AS match_source
FROM st LEFT JOIN best b USING (van_id, county);

CREATE OR REPLACE TABLE `your-gcp-project.commons.pw_volunteer_nearest_pickup` AS
WITH vol AS (
  SELECT van_id, county, match_source, ST_GEOGPOINT(longitude, latitude) AS pt
  FROM `your-gcp-project.commons.pw_volunteer_coordinates`
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
),
sites AS (
  SELECT *, ST_GEOGPOINT(longitude, latitude) AS geog
  FROM `your-gcp-project.commons.pw_badge_pickup_sites`
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)
SELECT v.van_id, v.county,
       s.address AS nearest_pickup_location,
       ROUND(ST_DISTANCE(v.pt, s.geog) / 1609.34, 2) AS distance_miles,
       s.time_blurb, s.location_blurb, s.plan_blurb, v.match_source
FROM vol v
JOIN sites s ON s.county = v.county
QUALIFY ROW_NUMBER() OVER (PARTITION BY v.van_id, v.county ORDER BY ST_DISTANCE(v.pt, s.geog) ASC) = 1;


-- =====================================================================================
-- STEP 3: CHECK coverage. "no_row" are volunteers who can't be matched to the voter file
-- (they just get the plain county text in their email, nothing breaks).
-- =====================================================================================
SELECT st.county,
       COUNT(*) AS active_volunteers,
       COUNTIF(n.van_id IS NULL) AS no_row,
       ROUND(AVG(n.distance_miles), 1) AS avg_miles,
       ROUND(MAX(n.distance_miles), 1) AS max_miles
FROM `your-gcp-project.commons.pw_credential_status` st
LEFT JOIN `your-gcp-project.commons.pw_volunteer_nearest_pickup` n
  ON n.van_id = st.van_id AND n.county = st.county
WHERE st.county IN ('Fulton', 'DeKalb') AND NOT st.is_removed_from_county
GROUP BY st.county;

-- and how many volunteers landed at each site:
SELECT county, nearest_pickup_location, COUNT(*) AS volunteers
FROM `your-gcp-project.commons.pw_volunteer_nearest_pickup`
GROUP BY county, nearest_pickup_location ORDER BY county, volunteers DESC;
