select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select INGREDIENT_PK
from USER_DB_PUMA.RAW.stg_ingredient
where INGREDIENT_PK is null



      
    ) dbt_internal_test