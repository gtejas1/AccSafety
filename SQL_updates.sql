DROP VIEW IF EXISTS unified_site_summary;

CREATE OR REPLACE VIEW unified_site_summary AS
WITH
/* Pre-aggregate statewide modeled tables so we can reuse them below */
swp AS (
  SELECT
    COALESCE(p.location_name, '(Unknown)') AS location_name,
    SUM(p.estimated_annual)::bigint        AS total_counts,
    p.longitude,
    p.latitude
  FROM statewide_pedestrian p
  GROUP BY COALESCE(p.location_name, '(Unknown)'), p.longitude, p.latitude
),
swb AS (
  SELECT
    COALESCE(b.location_name, '(Unknown)') AS location_name,
    SUM(b.estimated_annual)::bigint        AS total_counts,
    b.longitude,
    b.latitude
  FROM statewide_bicyclist b
  GROUP BY COALESCE(b.location_name, '(Unknown)'), b.longitude, b.latitude
)

/* ================= ECO (Pilot short-term, actual) ================= */
SELECT
  e.location_name                               AS "Location",
  'Short-term'                                  AS "Duration",
  SUM(e.count)::bigint                          AS "Total counts",
  'Actual'                                      AS "Source type",
  NULL::double precision                        AS "Longitude",
  NULL::double precision                        AS "Latitude",
  'Wisconsin Pilot Counting Counts'             AS "Source",
  'On-Street (sidewalk)'                        AS "Facility type",
  'On-Street'                                   AS "Facility group",
  'Pedestrian'                                  AS "Mode"
FROM eco_traffic_data e
GROUP BY e.location_name

UNION ALL
SELECT
  e.location_name                               AS "Location",
  'Short-term'                                  AS "Duration",
  SUM(e.count)::bigint                          AS "Total counts",
  'Actual'                                      AS "Source type",
  NULL::double precision                        AS "Longitude",
  NULL::double precision                        AS "Latitude",
  'Wisconsin Pilot Counting Counts'             AS "Source",
  'On-Street (sidewalk/bike lane)'              AS "Facility type",
  'On-Street'                                   AS "Facility group",
  'Bicyclist'                                   AS "Mode"
FROM eco_traffic_data e
GROUP BY e.location_name

UNION ALL
SELECT
  e.location_name                               AS "Location",
  'Short-term'                                  AS "Duration",
  SUM(e.count)::bigint                          AS "Total counts",
  'Actual'                                      AS "Source type",
  NULL::double precision                        AS "Longitude",
  NULL::double precision                        AS "Latitude",
  'Wisconsin Pilot Counting Counts'             AS "Source",
  'On-Street'                                   AS "Facility type",
  'On-Street'                                   AS "Facility group",
  'Both'                                        AS "Mode"
FROM eco_traffic_data e
GROUP BY e.location_name

/* ================= TRAIL (SEWRPC long-term, actual) ================= */
UNION ALL
SELECT
  t.location_name                               AS "Location",
  'Long-term'                                   AS "Duration",
  SUM(t.count)::bigint                          AS "Total counts",
  'Actual'                                      AS "Source type",
  NULL::double precision                        AS "Longitude",
  NULL::double precision                        AS "Latitude",
  'Off-Street Trail (SEWRPC Trail User Counts)' AS "Source",
  'Off-Street Trail'                            AS "Facility type",
  'Off-Street Trail'                            AS "Facility group",
  'Both'                                        AS "Mode"
FROM hr_traffic_data t
GROUP BY t.location_name

/* ================= STATEWIDE (modeled) — renamed + split ================= */
UNION ALL
SELECT
  swp.location_name                             AS "Location",
  'Long-term'                                   AS "Duration",
  swp.total_counts                              AS "Total counts",
  'Modeled'                                     AS "Source type",
  swp.longitude                                 AS "Longitude",
  swp.latitude                                  AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)'     AS "Source",
  CASE
    WHEN
      (
        swp.location_name ~* '\s&\s' OR
        swp.location_name ~* '\sand\s' OR
        swp.location_name ~* '\sat\s' OR
        swp.location_name ~* '\s@\s' OR
        swp.location_name ~* '.+/.+'
      )
      AND swp.location_name !~* '(trail|greenway|path|riverwalk|rail|boardwalk)'
    THEN 'On-Street (intersection)'
    ELSE 'On-Street (sidewalk)'
  END                                           AS "Facility type",
  'On-Street'                                   AS "Facility group",
  'Pedestrian'                                  AS "Mode"
FROM swp

UNION ALL
SELECT
  swb.location_name                             AS "Location",
  'Long-term'                                   AS "Duration",
  swb.total_counts                              AS "Total counts",
  'Modeled'                                     AS "Source type",
  swb.longitude                                 AS "Longitude",
  swb.latitude                                  AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)'     AS "Source",
  CASE
    WHEN
      (
        swb.location_name ~* '\s&\s' OR
        swb.location_name ~* '\sand\s' OR
        swb.location_name ~* '\sat\s' OR
        swb.location_name ~* '\s@\s' OR
        swb.location_name ~* '.+/.+'
      )
      AND swb.location_name !~* '(trail|greenway|path|riverwalk|rail|boardwalk)'
    THEN 'On-Street (intersection)'
    ELSE 'On-Street (sidewalk/bike lane)'
  END                                           AS "Facility type",
  'On-Street'                                   AS "Facility group",
  'Bicyclist'                                   AS "Mode"
FROM swb

/* ================= STATEWIDE TRAIL USER (modeled) ================= */
UNION ALL
SELECT
  COALESCE(tr.location_name, '(Unknown)')       AS "Location",
  'Long-term'                                   AS "Duration",
  SUM(tr.estimated_annual)::bigint              AS "Total counts",
  'Modeled'                                     AS "Source type",
  tr.longitude                                  AS "Longitude",
  tr.latitude                                   AS "Latitude",
  'Wisconsin Ped/Bike Database (Statewide)'     AS "Source",
  'Off-Street Trail'                            AS "Facility type",
  'Off-Street Trail'                            AS "Facility group",
  'Both'                                        AS "Mode"
FROM statewide_trailuser tr
GROUP BY COALESCE(tr.location_name, '(Unknown)'), tr.longitude, tr.latitude;
