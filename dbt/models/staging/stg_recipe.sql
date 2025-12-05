{{ config(materialized='view') }}

SELECT DISTINCT
    RECIPE_ID AS RECIPE_PK,
    TITLE,
    INSTRUCTIONS,
    SERVINGS,
    SOURCE_URL,
    CUISINE,
    DIET_FLAGS,
    LOADED_AT
FROM {{ source('raw', 'recipe') }}