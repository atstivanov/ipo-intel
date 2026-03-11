{{ config(materialized='table', schema='analytics') }}

SELECT DISTINCT
    coalesce(nullif(trim(industry), ''), 'Not available') AS industry,
    coalesce(nullif(trim(sector), ''), 'Not available') AS sector
FROM {{ ref('stg_ipos_recent') }}