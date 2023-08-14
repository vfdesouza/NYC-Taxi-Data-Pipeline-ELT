{{ config(
    materialized='incremental',
    cluster_by=['VendorID'],
    unique_key=['VendorID', 'tpep_pickup_datetime'],
    partition_by={
        "field": "tpep_pickup_datetime",
        "data_type": "datetime",
        "granularity": "day"
    }
) }}

WITH
  source AS (
    SELECT
      *
    FROM
      {{ ref('stg_model_test') }}
  )

SELECT * FROM source

{% if is_incremental() %}
  WHERE
  extract_at > (
    SELECT
      MAX(extract_at)
    FROM
      {{ this }}
  )
{% endif %}