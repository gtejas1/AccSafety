from __future__ import annotations

SUPPLEMENTAL_BOTH_SQL = """
SELECT
  both_agg.location_name AS "Location",
  CASE
    WHEN both_agg.total_hours <= 15 THEN '0-15hrs'
    WHEN both_agg.total_hours > 15  AND both_agg.total_hours <= 48 THEN '15-48hrs'
    WHEN both_agg.total_hours > 48  AND both_agg.total_hours <= 336 THEN '2days-14days'
    WHEN both_agg.total_hours > 336 AND both_agg.total_hours <= 720 THEN '14days-30days'
    WHEN both_agg.total_hours > 720 AND both_agg.total_hours <= 2160 THEN '1month-3months'
    WHEN both_agg.total_hours > 2160 AND both_agg.total_hours <= 4320 THEN '3months-6months'
    WHEN both_agg.total_hours > 4320 THEN '>6months'
    ELSE 'Unknown'
  END AS "Duration",
  both_agg.total_counts AS "Total counts",
  'Actual' AS "Source type",
  NULL::double precision AS "Longitude",
  NULL::double precision AS "Latitude",
  'Wisconsin Pilot Counting Program Counts' AS "Source",
  'Intersection' AS "Facility type",
  'Both' AS "Mode"
FROM (
  SELECT
    e.location_name,
    SUM(e.count)::bigint AS total_counts,
    CASE
      WHEN COUNT(*) > 0
      THEN GREATEST(EXTRACT(EPOCH FROM (MAX(e.date) - MIN(e.date))) / 3600.0 + 1, 0)
      ELSE 0
    END AS total_hours
  FROM eco_both_traffic_data e
  GROUP BY e.location_name
) both_agg
WHERE NOT EXISTS (
  SELECT 1
  FROM unified_site_summary existing
  WHERE existing."Location" = both_agg.location_name
    AND existing."Source" = 'Wisconsin Pilot Counting Program Counts'
    AND existing."Facility type" = 'Intersection'
    AND existing."Mode" = 'Both'
)
"""

UNIFIED_DATA_SQL = f"""
SELECT
  "Location",
  "Duration",
  "Total counts",
  "Source type",
  "Longitude",
  "Latitude",
  "Source",
  "Facility type",
  "Mode"
FROM unified_site_summary
UNION ALL
{SUPPLEMENTAL_BOTH_SQL}
"""

UNIFIED_SEARCH_SQL = f"""
SELECT
  "Location",
  "Longitude",
  "Latitude",
  "Total counts",
  "Source",
  "Facility type",
  "Mode"
FROM ({UNIFIED_DATA_SQL}) unified_data
WHERE "Location" ILIKE %(pattern)s
"""

UNIFIED_NEARBY_SQL = f"""
SELECT
  "Location",
  "Longitude",
  "Latitude",
  "Total counts",
  "Source",
  "Facility type",
  "Mode"
FROM ({UNIFIED_DATA_SQL}) unified_data
WHERE "Longitude" IS NOT NULL
  AND "Latitude" IS NOT NULL
"""
