
  
    

        create or replace transient table USER_DB_PUMA.RAW_ANALYTICS.mart_recipe_availability
         as
        (

WITH recipe_needs AS (
    -- 1. Calculate total required ingredients for every recipe
    SELECT
        RECIPE_PK,
        COUNT(INGREDIENT_PK) AS TOTAL_REQUIRED_COUNT
    FROM USER_DB_PUMA.RAW.stg_recipe_ingredient
    GROUP BY 1
),

user_matches AS (
    -- 2. Count how many of those required ingredients the user actually owns
    SELECT
        ri.RECIPE_PK,
        p.USER_PK,
        COUNT(ri.INGREDIENT_PK) AS MATCHED_COUNT
    FROM USER_DB_PUMA.RAW.stg_recipe_ingredient ri
    INNER JOIN USER_DB_PUMA.RAW.stg_pantry p
        ON ri.INGREDIENT_PK = p.INGREDIENT_PK
    GROUP BY 1, 2
)

-- 3. Final calculation and aggregation for the application feed
SELECT
    r.RECIPE_PK,
    m.USER_PK,
    r.TITLE,
    r.CUISINE,
    n.TOTAL_REQUIRED_COUNT,
    COALESCE(m.MATCHED_COUNT, 0) AS MATCHED_COUNT,
    n.TOTAL_REQUIRED_COUNT - COALESCE(m.MATCHED_COUNT, 0) AS MISSING_COUNT,
    
    -- The core recommendation metric
    (COALESCE(m.MATCHED_COUNT, 0) * 100.0) / n.TOTAL_REQUIRED_COUNT AS MATCH_PERCENTAGE

FROM USER_DB_PUMA.RAW.stg_recipe r
INNER JOIN recipe_needs n ON r.RECIPE_PK = n.RECIPE_PK
-- LEFT JOIN ensures recipes with 0 matches are still included
LEFT JOIN user_matches m ON r.RECIPE_PK = m.RECIPE_PK
ORDER BY MATCH_PERCENTAGE DESC, MISSING_COUNT ASC
        );
      
  