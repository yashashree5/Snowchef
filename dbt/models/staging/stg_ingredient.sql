{{ config(materialized='view') }}

SELECT DISTINCT
    INGREDIENT_ID AS INGREDIENT_PK,
    NAME AS INGREDIENT_NAME
FROM {{ source('raw', 'ingredient') }}