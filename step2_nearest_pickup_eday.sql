-- ELECTION DAY COPY of step2_nearest_pickup.sql (2026-10-29): reads pw_credential_status_eday, writes the *_eday tables.
-- Leaves the early-voting tables alone. Sites table (pw_badge_pickup_sites) and manual coordinates are shared.
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
CREATE OR REPLACE TABLE `your-gcp-project.commons.pw_volunteer_coordinates_eday` AS
WITH st AS (
  SELECT van_id, county, first_name, last_name, address,
         UPPER(TRIM(first_name)) AS fn, UPPER(TRIM(last_name)) AS ln,
         REGEXP_EXTRACT(address, r'(\d{5})(?:-\d{4})?\s*$') AS zip
  FROM `your-gcp-project.commons.pw_credential_status_eday`
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
  UNION ALL
  -- 4 = coordinates geocoded by hand (table commons.pw_volunteer_manual_coordinates:
  --     van_id STRING, latitude FLOAT, longitude FLOAT). Only fills volunteers the voter file missed.
  SELECT st.van_id, st.county, m.latitude, m.longitude, 4
  FROM st JOIN `your-gcp-project.commons.pw_volunteer_manual_coordinates` m ON CAST(m.van_id AS STRING) = st.van_id
  WHERE m.latitude IS NOT NULL AND m.longitude IS NOT NULL
),
best AS (
  SELECT van_id, county, lat, lon, prio
  FROM matches
  QUALIFY ROW_NUMBER() OVER (PARTITION BY van_id, county ORDER BY prio) = 1
)
SELECT st.van_id, st.first_name, st.last_name, st.county, st.address AS volunteer_address,
       b.lat AS latitude, b.lon AS longitude,
       CASE b.prio WHEN 1 THEN 'myv_van_id' WHEN 2 THEN 'national_myv_van_id' WHEN 3 THEN 'name_zip' WHEN 4 THEN 'manual_geocode' END AS match_source
FROM st LEFT JOIN best b USING (van_id, county);

CREATE OR REPLACE TABLE `your-gcp-project.commons.pw_volunteer_nearest_pickup_eday` AS
WITH vol AS (
  SELECT van_id, county, match_source, ST_GEOGPOINT(longitude, latitude) AS pt
  FROM `your-gcp-project.commons.pw_volunteer_coordinates_eday`
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
-- EXPORT 1 (for a teammate): every active Fulton / DeKalb volunteer with lat/long, the site
-- they're assigned to and how far it is. Unmatched volunteers appear with blank coordinates.
-- =====================================================================================
SELECT c.van_id, c.first_name, c.last_name, c.county, c.volunteer_address,
       c.latitude, c.longitude, c.match_source,
       n.nearest_pickup_location AS assigned_pickup_site, n.distance_miles
FROM `your-gcp-project.commons.pw_volunteer_coordinates_eday` c
LEFT JOIN `your-gcp-project.commons.pw_volunteer_nearest_pickup_eday` n USING (van_id, county)
ORDER BY c.county, c.last_name, c.first_name;

-- =====================================================================================
-- EXPORT 2 (for a teammate): the badge pickup sites with lat/long
-- =====================================================================================
SELECT county, site_name, address, latitude, longitude
FROM `your-gcp-project.commons.pw_badge_pickup_sites`
ORDER BY county, site_name;
