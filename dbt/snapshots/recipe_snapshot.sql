{% snapshot recipe_snapshot %}

{{
    config(
      target_database=target.database,
      target_schema='HISTORY', 
      unique_key='RECIPE_PK',
      strategy='check',
      check_cols=['TITLE', 'INSTRUCTIONS', 'SERVINGS', 'SOURCE_URL', 'CUISINE', 'DIET_FLAGS'],
      invalidate_hard_deletes=True,
    )
}}

SELECT *
FROM {{ ref('stg_recipe') }} 

{% endsnapshot %}