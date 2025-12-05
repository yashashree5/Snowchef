{{ config(materialized='table') }}

SELECT
    p.USER_PK,
    p.LOADED_AT,
    p.QUANTITY,
    p.UNIT,
    i.INGREDIENT_NAME
FROM {{ ref('stg_pantry') }} p
INNER JOIN {{ ref('stg_ingredient') }} i
    ON p.INGREDIENT_PK = i.INGREDIENT_PK