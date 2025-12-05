{{ config(materialized='view') }}

SELECT
    USER_ID AS USER_PK,
    INGREDIENT_ID AS INGREDIENT_PK,
    QUANTITY,
    UNIT,
    LOADED_AT
FROM {{ source('raw', 'pantry') }}