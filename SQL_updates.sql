-- ============================================================
-- Replace unified_site_summary to use separated ECO tables:
--   eco_ped_traffic_data, eco_bike_traffic_data, eco_both_traffic_data
-- Keep Trail (hr_traffic_data) + Statewide as-is.
-- ============================================================

DROP VIEW IF EXISTS unified_site_summary;

CREATE OR REPLACE VIEW unified_site_summary AS
WITH
/* ============================================================
   1) ECO & TRAIL: derive duration as inclusive span of rows
      total_hours = (max(date) - min(date)) in hours + 1
   ============================================================ */
eco_ped_agg AS (
  SELECT
    e.location_name,
    SUM(e.count)::bigint AS total_counts,
    CASE WHEN COUNT(*) > 0
         THEN GREATEST(EXTRACT(EPOCH FROM (MAX(e.date) - MIN(e.date))) / 3600.0 + 1, 0)
         ELSE 0
    END AS total_hours
  FROM eco_ped_traffic_data e
  GROUP BY e.location_name
),
eco_bike_agg AS (
  SELECT
    e.location_name,
    SUM(e.count)::bigint AS total_counts,
    CASE WHEN COUNT(*) > 0
         THEN GREATEST(EXTRACT(EPOCH FROM (MAX(e.date) - MIN(e.date))) / 3600.0 + 1, 0)
         ELSE 0
    END AS total_hours
  FROM eco_bike_traffic_data e
  GROUP BY e.location_name
),
eco_both_agg AS (
  SELECT
    e.location_name,
    SUM(e.count)::bigint AS total_counts,
    CASE WHEN COUNT(*) > 0
         THEN GREATEST(EXTRACT(EPOCH FROM (MAX(e.date) - MIN(e.date))) / 3600.0 + 1, 0)
         ELSE 0
    END AS total_hours
  FROM eco_both_traffic_data e
  GROUP BY e.location_name
),
trail_agg AS (
  SELECT
    t.location_name,
    SUM(t.count)::bigint AS total_counts,
    CASE WHEN COUNT(*) > 0
         THEN GREATEST(EXTRACT(EPOCH FROM (MAX(t.date) - MIN(t.date))) / 3600.0 + 1, 0)
         ELSE 0
    END AS total_hours
  FROM hr_traffic_data t
  GROUP BY t.location_name
),

/* ============================================================
   2) STATEWIDE: parse `duration` text → numeric hours → bucket
      Handles "6 hr", "6 hrs", "6 hours", also minutes/days if present
   ============================================================ */
dur_map AS (
  SELECT * FROM (VALUES
    ('0-15hrs', 1),
    ('15-48hrs', 2),
    ('2days-14days', 3),
    ('14days-30days', 4),
    ('1month-3months', 5),
    ('3months-6months', 6),
    ('>6months', 7)
  ) AS dm(bucket, ord)
),

-- Pedestrian statewide
swp_row AS (
  SELECT
    COALESCE(p.location_name, '(Unknown)') AS location_name,
    p.estimated_annual                      AS est,
    p.longitude,
    p.latitude,
    CASE
      WHEN p.duration ~* '\d' THEN
        CASE
          WHEN p.duration ~* 'min' THEN NULLIF(regexp_replace(p.duration, '[^0-9\.]', '', 'g'), '')::double precision / 60.0
          WHEN p.duration ~* 'day' THEN NULLIF(regexp_replace(p.duration, '[^0-9\.]', '', 'g'), '')::double precision * 24.0
          ELSE NULLIF(regexp_replace(p.duration, '[^0-9\.]', '', 'g'), '')::double precision
        END
      ELSE NULL
    END AS dur_hours
  FROM statewide_pedestrian p
),
swp AS (
  SELECT
    r.location_name,
    SUM(r.est)::bigint AS total_counts,
    MIN(r.longitude)   AS longitude,
    MIN(r.latitude)    AS latitude,
    CASE
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 15 THEN '0-15hrs'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 48 THEN '15-48hrs'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 336 THEN '2days-14days'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 720 THEN '14days-30days'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 2160 THEN '1month-3months'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 4320 THEN '3months-6months'
      ELSE '>6months'
    END AS duration_bucket
  FROM swp_row r
  GROUP BY r.location_name
),

-- Bicyclist statewide
swb_row AS (
  SELECT
    COALESCE(b.location_name, '(Unknown)') AS location_name,
    b.estimated_annual                      AS est,
    b.longitude,
    b.latitude,
    CASE
      WHEN b.duration ~* '\d' THEN
        CASE
          WHEN b.duration ~* 'min' THEN NULLIF(regexp_replace(b.duration, '[^0-9\.]', '', 'g'), '')::double precision / 60.0
          WHEN b.duration ~* 'day' THEN NULLIF(regexp_replace(b.duration, '[^0-9\.]', '', 'g'), '')::double precision * 24.0
          ELSE NULLIF(regexp_replace(b.duration, '[^0-9\.]', '', 'g'), '')::double precision
        END
      ELSE NULL
    END AS dur_hours
  FROM statewide_bicyclist b
),
swb AS (
  SELECT
    r.location_name,
    SUM(r.est)::bigint AS total_counts,
    MIN(r.longitude)   AS longitude,
    MIN(r.latitude)    AS latitude,
    CASE
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 15 THEN '0-15hrs'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 48 THEN '15-48hrs'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 336 THEN '2days-14days'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 720 THEN '14days-30days'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 2160 THEN '1month-3months'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 4320 THEN '3months-6months'
      ELSE '>6months'
    END AS duration_bucket
  FROM swb_row r
  GROUP BY r.location_name
),

-- Trail user statewide
swt_row AS (
  SELECT
    COALESCE(t.location_name, '(Unknown)') AS location_name,
    t.estimated_annual                      AS est,
    t.longitude,
    t.latitude,
    CASE
      WHEN t.duration ~* '\d' THEN
        CASE
          WHEN t.duration ~* 'min' THEN NULLIF(regexp_replace(t.duration, '[^0-9\.]', '', 'g'), '')::double precision / 60.0
          WHEN t.duration ~* 'day' THEN NULLIF(regexp_replace(t.duration, '[^0-9\.]', '', 'g'), '')::double precision * 24.0
          ELSE NULLIF(regexp_replace(t.duration, '[^0-9\.]', '', 'g'), '')::double precision
        END
      ELSE NULL
    END AS dur_hours
  FROM statewide_trailuser t
),
swt AS (
  SELECT
    r.location_name,
    SUM(r.est)::bigint AS total_counts,
    MIN(r.longitude)   AS longitude,
    MIN(r.latitude)    AS latitude,
    CASE
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 15 THEN '0-15hrs'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 48 THEN '15-48hrs'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 336 THEN '2days-14days'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 720 THEN '14days-30days'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 2160 THEN '1month-3months'
      WHEN COALESCE(MAX(r.dur_hours), 999999) <= 4320 THEN '3months-6months'
      ELSE '>6months'
    END AS duration_bucket
  FROM swt_row r
  GROUP BY r.location_name
)

/* ============================================================
   3) FINAL UNION  (merge/rename facility types & merge trail source)
   ============================================================ */

-- ECO: PEDESTRIAN (actual)  → On-Street (sidewalk)
SELECT
  e.location_name AS "Location",
  CASE
    WHEN e.total_hours <= 15 THEN '0-15hrs'
    WHEN e.total_hours > 15  AND e.total_hours <= 48   THEN '15-48hrs'
    WHEN e.total_hours > 48  AND e.total_hours <= 336  THEN '2days-14days'
    WHEN e.total_hours > 336 AND e.total_hours <= 720  THEN '14days-30days'
    WHEN e.total_hours > 720 AND e.total_hours <= 2160 THEN '1month-3months'
    WHEN e.total_hours > 2160 AND e.total_hours <= 4320 THEN '3months-6months'
    WHEN e.total_hours > 4320 THEN '>6months'
    ELSE 'Unknown'
  END AS "Duration",
  e.total_counts AS "Total counts",
  'Actual' AS "Source type",
  NULL::double precision AS "Longitude",
  NULL::double precision AS "Latitude",
  'Wisconsin Pilot Counting Counts' AS "Source",
  'On-Street (sidewalk)' AS "Facility type",
  'On-Street' AS "Facility group",
  'Pedestrian' AS "Mode"
FROM eco_ped_agg e

UNION ALL
-- ECO: BICYCLIST (actual)  → On-Street (sidewalk/bike lane)
SELECT
  e.location_name AS "Location",
  CASE
    WHEN e.total_hours <= 15 THEN '0-15hrs'
    WHEN e.total_hours > 15  AND e.total_hours <= 48   THEN '15-48hrs'
    WHEN e.total_hours > 48  AND e.total_hours <= 336  THEN '2days-14days'
    WHEN e.total_hours > 336 AND e.total_hours <= 720  THEN '14days-30days'
    WHEN e.total_hours > 720 AND e.total_hours <= 2160 THEN '1month-3months'
    WHEN e.total_hours > 2160 AND e.total_hours <= 4320 THEN '3months-6months'
    WHEN e.total_hours > 4320 THEN '>6months'
    ELSE 'Unknown'
  END AS "Duration",
  e.total_counts AS "Total counts",
  'Actual' AS "Source type",
  NULL::double precision AS "Longitude",
  NULL::double precision AS "Latitude",
  'Wisconsin Pilot Counting Counts' AS "Source",
  'On-Street (sidewalk/bike lane)' AS "Facility type",
  'On-Street' AS "Facility group",
  'Bicyclist' AS "Mode"
FROM eco_bike_agg e

UNION ALL
-- ECO: BOTH (actual)  → rename Facility type On-Street → Intersection
SELECT
  e.location_name AS "Location",
  CASE
    WHEN e.total_hours <= 15 THEN '0-15hrs'
    WHEN e.total_hours > 15  AND e.total_hours <= 48   THEN '15-48hrs'
    WHEN e.total_hours > 48  AND e.total_hours <= 336  THEN '2days-14days'
    WHEN e.total_hours > 336 AND e.total_hours <= 720  THEN '14days-30days'
    WHEN e.total_hours > 720 AND e.total_hours <= 2160 THEN '1month-3months'
    WHEN e.total_hours > 2160 AND e.total_hours <= 4320 THEN '3months-6months'
    WHEN e.total_hours > 4320 THEN '>6months'
    ELSE 'Unknown'
  END AS "Duration",
  e.total_counts AS "Total counts",
  'Actual' AS "Source type",
  NULL::double precision AS "Longitude",
  NULL::double precision AS "Latitude",
  'Wisconsin Pilot Counting Counts' AS "Source",
  'Intersection' AS "Facility type",
  'On-Street'  AS "Facility group",
  'Both' AS "Mode"
FROM eco_both_agg e

UNION ALL
-- TRAIL (SEWRPC actual)  **merged source label, unchanged from your script**
SELECT
  t.location_name AS "Location",
  CASE
    WHEN t.total_hours <= 15 THEN '0-15hrs'
    WHEN t.total_hours > 15  AND t.total_hours <= 48   THEN '15-48hrs'
    WHEN t.total_hours > 48  AND t.total_hours <= 336  THEN '2days-14days'
    WHEN t.total_hours > 336 AND t.total_hours <= 720  THEN '14days-30days'
    WHEN t.total_hours > 720 AND t.total_hours <= 2160 THEN '1month-3months'
    WHEN t.total_hours > 2160 AND t.total_hours <= 4320 THEN '3months-6months'
    WHEN t.total_hours > 4320 THEN '>6months'
    ELSE 'Unknown'
  END AS "Duration",
  t.total_counts AS "Total counts",
  'Actual' AS "Source type",
  NULL::double precision AS "Longitude",
  NULL::double precision AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)' AS "Source",
  'Off-Street Trail' AS "Facility type",
  'Off-Street Trail' AS "Facility group",
  'Both' AS "Mode"
FROM trail_agg t

UNION ALL
-- STATEWIDE (modeled) – PEDESTRIAN  → On-Street (sidewalk)
SELECT
  swp.location_name AS "Location",
  swp.duration_bucket AS "Duration",
  swp.total_counts AS "Total counts",
  'Modeled' AS "Source type",
  swp.longitude AS "Longitude",
  swp.latitude AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)' AS "Source",
  'On-Street (sidewalk)' AS "Facility type",
  'On-Street' AS "Facility group",
  'Pedestrian' AS "Mode"
FROM swp

UNION ALL
-- STATEWIDE (modeled) – BICYCLIST  → On-Street (sidewalk/bike lane)
SELECT
  swb.location_name AS "Location",
  swb.duration_bucket AS "Duration",
  swb.total_counts AS "Total counts",
  'Modeled' AS "Source type",
  swb.longitude AS "Longitude",
  swb.latitude AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)' AS "Source",
  'On-Street (sidewalk/bike lane)' AS "Facility type",
  'On-Street' AS "Facility group",
  'Bicyclist' AS "Mode"
FROM swb

UNION ALL
-- STATEWIDE (modeled) – TRAIL USER
SELECT
  swt.location_name AS "Location",
  swt.duration_bucket AS "Duration",
  swt.total_counts AS "Total counts",
  'Modeled' AS "Source type",
  swt.longitude AS "Longitude",
  swt.latitude AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)' AS "Source",
  'Off-Street Trail' AS "Facility type",
  'Off-Street Trail' AS "Facility group",
  'Both' AS "Mode"
FROM swt;
