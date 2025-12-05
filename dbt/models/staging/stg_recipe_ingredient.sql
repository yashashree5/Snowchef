{{ config(materialized='view') }}

SELECT
    RECIPE_ID AS RECIPE_PK,
    INGREDIENT_ID AS INGREDIENT_PK,
    QUANTITY,
    UNIT,
    RAW_TEXT
FROM {{ source('raw', 'recipe_ingredient') }}