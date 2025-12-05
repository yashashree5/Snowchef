
    
    

select
    INGREDIENT_PK as unique_field,
    count(*) as n_records

from USER_DB_PUMA.RAW.stg_ingredient
where INGREDIENT_PK is not null
group by INGREDIENT_PK
having count(*) > 1


