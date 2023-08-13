WITH
  source AS (
    SELECT
      *
    FROM
      {{ source('raw_zone', 'raw_nyc_yellow') }}
    WHERE
      VendorID = 2
      AND tpep_dropoff_datetime BETWEEN '2022-01-01'
      AND '2022-01-31'
  )

SELECT * FROM source
