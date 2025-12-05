select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select RECIPE_PK
from USER_DB_PUMA.RAW.stg_recipe_ingredient
where RECIPE_PK is null



      
    ) dbt_internal_test