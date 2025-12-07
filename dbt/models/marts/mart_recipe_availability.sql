{{ config(materialized='table') }}

WITH recipe_needs AS (
    -- 1. How many ingredients each recipe requires
    SELECT
        recipe_pk,
        COUNT(DISTINCT ingredient_pk) AS total_required_count
    FROM {{ ref('stg_recipe_ingredient') }}
    GROUP BY recipe_pk
),

users AS (
    -- 2. All distinct users from pantry
    SELECT DISTINCT
        user_pk
    FROM {{ ref('stg_pantry') }}
),

user_matches AS (
    -- 3. For each (user, recipe), how many ingredients the user has
    SELECT
        p.user_pk,
        ri.recipe_pk,
        COUNT(DISTINCT ri.ingredient_pk) AS matched_count
    FROM {{ ref('stg_pantry') }} p
    JOIN {{ ref('stg_recipe_ingredient') }} ri
      ON p.ingredient_pk = ri.ingredient_pk
    GROUP BY
        p.user_pk,
        ri.recipe_pk
),

user_recipe_grid AS (
    -- 4. All combinations user × recipe so we also see 0-match recipes
    SELECT
        u.user_pk,
        rn.recipe_pk,
        rn.total_required_count
    FROM users u
    CROSS JOIN recipe_needs rn
)

-- 5. Final mart for recommendations
SELECT
    g.user_pk,
    g.recipe_pk,
    r.title,
    r.instructions,
    r.cuisine,
    r.servings,
    r.diet_flags,
    r.source_url,
    g.total_required_count                           AS total_required_count,
    COALESCE(um.matched_count, 0)                    AS matched_count,
    g.total_required_count - COALESCE(um.matched_count, 0) AS missing_count,
    COALESCE(um.matched_count, 0) * 100.0
        / g.total_required_count                     AS match_percentage,
    CASE
        WHEN g.total_required_count - COALESCE(um.matched_count, 0) = 0
        THEN TRUE
        ELSE FALSE
    END AS can_cook_now,
    CASE
        WHEN g.total_required_count - COALESCE(um.matched_count, 0) BETWEEN 1 AND 2
        THEN TRUE
        ELSE FALSE
    END AS almost_can_cook
FROM user_recipe_grid g
JOIN {{ ref('stg_recipe') }} r
  ON g.recipe_pk = r.recipe_pk
LEFT JOIN user_matches um
  ON um.user_pk = g.user_pk
 AND um.recipe_pk = g.recipe_pk
