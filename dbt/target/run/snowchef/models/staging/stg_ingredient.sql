
  create or replace   view USER_DB_PUMA.RAW.stg_ingredient
  
   as (
    

SELECT DISTINCT
    INGREDIENT_ID AS INGREDIENT_PK,
    NAME AS INGREDIENT_NAME
FROM USER_DB_PUMA.RAW.ingredient
  );

