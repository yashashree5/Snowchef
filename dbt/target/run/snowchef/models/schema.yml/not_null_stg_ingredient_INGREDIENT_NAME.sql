select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select INGREDIENT_NAME
from USER_DB_PUMA.RAW.stg_ingredient
where INGREDIENT_NAME is null



      
    ) dbt_internal_test