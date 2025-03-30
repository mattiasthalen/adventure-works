MODEL (
  name dar.calendar,
  enabled TRUE,
  kind INCREMENTAL_BY_UNIQUE_KEY(
    unique_key _hook__epoch__date
  ),
  tags unified_star_schema,
  grain (_hook__epoch__date)
);

WITH cte__dates AS (
  SELECT
    string_split(_hook__epoch__date::TEXT, '|')[-1]::DATE AS date
  FROM dar._puppini_bridge__as_of
  WHERE 1 = 1
  AND _hook__epoch__date IS NOT NULL
  AND bridge__record_updated_at BETWEEN @start_dt AND @end_dt
  ORDER BY date
), cte__years AS (
  SELECT
    EXTRACT(YEAR FROM date) AS year
  FROM cte__dates
), cte__year_range AS (
  SELECT
    MIN(year) AS min_year,
    MAX(year) AS max_year
  FROM cte__years
), cte__date_range AS (
  SELECT
    MAKE_DATE(min_year, 1, 1) AS start_date,
    MAKE_DATE(max_year, 12, 31) AS end_date
  FROM cte__year_range
), cte__date_spine AS (
  SELECT
    UNNEST(
      GENERATE_SERIES(
        start_date,
        end_date,
        INTERVAL '1 day'
      )
    )::DATE AS date
  FROM cte__date_range
), cte__calendar AS (
  SELECT
    CONCAT('epoch__date|', date::TEXT) AS _hook__epoch__date,
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(ISOYEAR FROM date) AS iso_year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    STRFTIME('%b', date) AS month__name,
    EXTRACT(WEEK FROM date) AS week,
    EXTRACT(DOW FROM date) AS weekday,
    STRFTIME('%a', date) AS weekday__name,
    year||'-'||quarter AS year_quarter,
    STRFTIME('%Y-%m', date) AS year_month,
    STRFTIME('%Y-%b', date) AS year_month__name,
    STRFTIME('%G-W%V', date) AS year_week,
    STRFTIME('%G-W%V-%u', date) AS iso_week_date,
    STRFTIME('%Y-%j', date) AS ordinal_date,
    (EXTRACT(YEAR FROM date) % 4 = 0 AND (EXTRACT(YEAR FROM date) % 100 != 0 OR EXTRACT(YEAR FROM date) % 400 = 0)) AS is_leap_year
  FROM cte__date_spine
)

SELECT
  _hook__epoch__date::BLOB,
  date::TEXT,
  year::INTEGER,
  iso_year::INTEGER,
  quarter::INTEGER,
  month::INTEGER,
  month__name::TEXT,
  week::INTEGER,
  weekday::INTEGER,
  weekday__name::TEXT,
  year_quarter::TEXT,
  year_month::TEXT,
  year_month__name::TEXT,
  year_week::TEXT,
  iso_week_date::TEXT,
  ordinal_date::TEXT,
  is_leap_year::BOOLEAN
FROM cte__calendar