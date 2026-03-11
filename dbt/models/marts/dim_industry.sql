{{ config(materialized='table', schema='analytics') }}

SELECT DISTINCT
    COALESCE(NULLIF(TRIM(industry), ''), 'Not available') AS industry,
    COALESCE(NULLIF(TRIM(sector), ''), 'Not available') AS sector
FROM {{ ref('stg_ipos_recent') }}