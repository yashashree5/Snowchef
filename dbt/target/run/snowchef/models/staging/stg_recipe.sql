
  create or replace   view USER_DB_PUMA.RAW.stg_recipe
  
   as (
    

SELECT DISTINCT
    RECIPE_ID AS RECIPE_PK,
    TITLE,
    INSTRUCTIONS,
    SERVINGS,
    SOURCE_URL,
    CUISINE,
    DIET_FLAGS,
    LOADED_AT
FROM USER_DB_PUMA.RAW.recipe
  );

