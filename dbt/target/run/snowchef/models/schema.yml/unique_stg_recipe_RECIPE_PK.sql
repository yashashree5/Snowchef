select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    RECIPE_PK as unique_field,
    count(*) as n_records

from USER_DB_PUMA.RAW.stg_recipe
where RECIPE_PK is not null
group by RECIPE_PK
having count(*) > 1



      
    ) dbt_internal_test