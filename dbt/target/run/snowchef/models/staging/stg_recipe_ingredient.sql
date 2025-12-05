
  create or replace   view USER_DB_PUMA.RAW.stg_recipe_ingredient
  
   as (
    

SELECT
    RECIPE_ID AS RECIPE_PK,
    INGREDIENT_ID AS INGREDIENT_PK,
    QUANTITY,
    UNIT,
    RAW_TEXT
FROM USER_DB_PUMA.RAW.recipe_ingredient
  );

