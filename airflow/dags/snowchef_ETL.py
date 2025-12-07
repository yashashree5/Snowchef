from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import random
import numpy as np
from datetime import datetime, timedelta
import requests
import json

# Configuration
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB = "USER_DB_PUMA"
SCHEMA = "RAW"
API_KEY = Variable.get("SPOONACULAR_API_KEY")

default_args = {
    'owner': 'snowchef',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_snowflake_cursor():
    """Get Snowflake connection and cursor"""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    return conn, conn.cursor()


with DAG(
    dag_id="snowchef_ETL_Dag",
    description="Snowchef ETL",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['snowchef', 'simple'],
) as dag:

    @task
    def create_tables():
        """Create all essential tables, including PANTRY."""
        conn, cur = get_snowflake_cursor()
        try:
            cur.execute(f"USE DATABASE {DB}")
            cur.execute(f"USE SCHEMA {SCHEMA}")
            
            # Recipe table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS RECIPE (
                    RECIPE_ID STRING PRIMARY KEY,
                    TITLE STRING,
                    INSTRUCTIONS STRING,
                    SERVINGS NUMBER,
                    SOURCE_URL STRING,
                    CUISINE STRING,
                    DIET_FLAGS ARRAY,
                    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Ingredient table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS INGREDIENT (
                    INGREDIENT_ID STRING PRIMARY KEY,
                    NAME STRING,
                    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Recipe-Ingredient link table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS RECIPE_INGREDIENT (
                    RECIPE_ID STRING,
                    INGREDIENT_ID STRING,
                    QUANTITY FLOAT,
                    UNIT STRING,
                    RAW_TEXT STRING,
                    PRIMARY KEY (RECIPE_ID, INGREDIENT_ID)
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS PANTRY (
                    USER_ID STRING,
                    INGREDIENT_ID STRING,
                    QUANTITY FLOAT,
                    UNIT STRING,
                    LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (USER_ID, INGREDIENT_ID)
                )
            """)
            
            conn.commit()
            print("Tables created successfully")
            
        finally:
            cur.close()
            conn.close()

    @task
    def extract_recipes():
        """Extract recipes from Spoonacular API - simplified (no pagination)"""
        url = "https://api.spoonacular.com/recipes/complexSearch"
        
        params = {
            "apiKey": API_KEY,
            "number": 50,
            "addRecipeInformation": True,
            "instructionsRequired": True,
            "fillIngredients": True,
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json().get("results", [])
        
        recipes = []
        for rec in data:
            # Parse instructions
            steps = []
            for block in rec.get("analyzedInstructions", []):
                for step in block.get("steps", []):
                    if step.get("step"):
                        steps.append(step["step"].strip())
            instructions = " ".join(steps) or rec.get("instructions", "")
            
            # Parse ingredients
            ingredients = []
            for ing in rec.get("extendedIngredients", []):
                ingredients.append({
                    "id": f"s_{ing.get('id', ing.get('name', 'unknown').replace(' ', '_'))}",
                    "name": ing.get("name") or ing.get("originalName") or "unknown",
                    "quantity": ing.get("amount"),
                    "unit": (ing.get("unit") or "").lower(),
                    "raw": ing.get("original"),
                })
            
            recipes.append({
                "id": f"s_{rec.get('id')}",
                "title": rec.get("title"),
                "instructions": instructions,
                "servings": rec.get("servings"),
                "url": rec.get("sourceUrl"),
                "cuisine": ((rec.get("cuisines") or [""])[0] or "").lower(),
                "diets": rec.get("diets", []),
                "ingredients": ingredients
            })
        
        print(f" Extracted {len(recipes)} recipes")
        return recipes

    @task
    def load_recipes(recipes: list):
        """Load recipes to Snowflake with MERGE (idempotent)"""
        conn, cur = get_snowflake_cursor()
        
        try:
            cur.execute(f"USE DATABASE {DB}")
            cur.execute(f"USE SCHEMA {SCHEMA}")
            
            recipes_loaded = 0
            ingredients_loaded = 0
            links_loaded = 0
            
            for recipe in recipes:
                # 1. MERGE Recipe
                cur.execute("""
                    MERGE INTO RECIPE t
                    USING (
                        SELECT %s as rid, %s as title, %s as inst, %s as serv, 
                               %s as url, %s as cuisine, TO_ARRAY(PARSE_JSON(%s)) as diets
                    ) s
                    ON t.RECIPE_ID = s.rid
                    WHEN MATCHED THEN UPDATE SET
                        TITLE = s.title,
                        INSTRUCTIONS = s.inst,
                        SERVINGS = s.serv,
                        SOURCE_URL = s.url,
                        CUISINE = s.cuisine,
                        DIET_FLAGS = s.diets,
                        LOADED_AT = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                        RECIPE_ID, TITLE, INSTRUCTIONS, SERVINGS, 
                        SOURCE_URL, CUISINE, DIET_FLAGS, LOADED_AT
                    ) VALUES (
                        s.rid, s.title, s.inst, s.serv, 
                        s.url, s.cuisine, s.diets, CURRENT_TIMESTAMP()
                    )
                """, (
                    recipe["id"], recipe["title"], recipe["instructions"],
                    recipe["servings"], recipe["url"], recipe["cuisine"],
                    json.dumps(recipe["diets"])
                ))
                recipes_loaded += 1
                
                # 2. MERGE Ingredients and Links
                for ing in recipe["ingredients"]:
                    # MERGE Ingredient
                    cur.execute("""
                        MERGE INTO INGREDIENT t
                        USING (SELECT %s as iid, %s as name) s
                        ON t.INGREDIENT_ID = s.iid
                        WHEN MATCHED THEN UPDATE SET
                            NAME = s.name,
                            LOADED_AT = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT (INGREDIENT_ID, NAME, LOADED_AT)
                        VALUES (s.iid, s.name, CURRENT_TIMESTAMP())
                    """, (ing["id"], ing["name"]))
                    ingredients_loaded += 1
                    
                    # MERGE Recipe-Ingredient Link
                    cur.execute("""
                        MERGE INTO RECIPE_INGREDIENT t
                        USING (
                            SELECT %s as rid, %s as iid, %s as qty, 
                                   %s as unit, %s as raw
                        ) s
                        ON t.RECIPE_ID = s.rid AND t.INGREDIENT_ID = s.iid
                        WHEN MATCHED THEN UPDATE SET
                            QUANTITY = s.qty,
                            UNIT = s.unit,
                            RAW_TEXT = s.raw
                        WHEN NOT MATCHED THEN INSERT (
                            RECIPE_ID, INGREDIENT_ID, QUANTITY, UNIT, RAW_TEXT
                        ) VALUES (s.rid, s.iid, s.qty, s.unit, s.raw)
                    """, (recipe["id"], ing["id"], ing["quantity"], ing["unit"], ing["raw"]))
                    links_loaded += 1
            
            conn.commit()
            print(f" Loaded: {recipes_loaded} recipes, {ingredients_loaded} ingredients, {links_loaded} links")
            
        finally:
            cur.close()
            conn.close()


    @task
    def load_synthetic_pantry_data():
       
        from collections import defaultdict

        # --- Config ---
        NUM_TEST_USERS = 10
        RECIPES_PER_USER = 3          # how many recipes each user can fully cook
        EXTRA_INGREDIENTS_PER_USER = 5  # random extra ingredients for realism

        TEST_USERS = [f"test_user_{i:02d}" for i in range(1, NUM_TEST_USERS + 1)]
        COMMON_UNITS = ["cup", "oz", "gram", "unit", "pound", "tsp", "tbsp", "clove", "can"]

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn, cur = get_snowflake_cursor()

        try:
            # 1) Load recipe → ingredient mappings
            cur.execute(f"""
                SELECT RECIPE_ID, INGREDIENT_ID
                FROM {DB}.{SCHEMA}.RECIPE_INGREDIENT
            """)
            rows = cur.fetchall()

            if not rows:
                print("No recipe_ingredient data found. Run recipe load first.")
                return

            recipe_to_ingredients = defaultdict(set)
            all_ingredient_ids = set()

            for recipe_id, ingredient_id in rows:
                if recipe_id and ingredient_id:
                    recipe_to_ingredients[recipe_id].add(ingredient_id)
                    all_ingredient_ids.add(ingredient_id)

            recipes_with_ingredients = [r for r, ings in recipe_to_ingredients.items() if ings]

            if not recipes_with_ingredients:
                print("No recipes with ingredients found. Skipping pantry generation.")
                return

            print(f"Found {len(recipes_with_ingredients)} recipes with ingredients.")
            print(f"Found {len(all_ingredient_ids)} unique ingredients.")

        finally:
            cur.close()
            conn.close()

        pantry_records = []

        # 2) Build pantry per synthetic user
        for user_id in TEST_USERS:
            # Make sure we don't ask for more recipes than exist
            recipes_for_user = random.sample(
                recipes_with_ingredients,
                k=min(RECIPES_PER_USER, len(recipes_with_ingredients))
            )

            user_ingredient_set = set()

            # a) Add all ingredients for the chosen recipes (guarantees 100% matches)
            for recipe_id in recipes_for_user:
                for ing_id in recipe_to_ingredients[recipe_id]:
                    if ing_id in user_ingredient_set:
                        continue
                    user_ingredient_set.add(ing_id)
                    pantry_records.append({
                        "USER_ID": user_id,
                        "INGREDIENT_ID": ing_id,
                        "QUANTITY": round(random.uniform(0.5, 10.0), 2),
                        "UNIT": random.choice(COMMON_UNITS),
                    })

            # b) Add some extra random ingredients (noise, more partial matches)
            remaining_ingredients = list(all_ingredient_ids - user_ingredient_set)
            if remaining_ingredients:
                extra_k = min(EXTRA_INGREDIENTS_PER_USER, len(remaining_ingredients))
                extra_ings = random.sample(remaining_ingredients, k=extra_k)

                for ing_id in extra_ings:
                    user_ingredient_set.add(ing_id)
                    pantry_records.append({
                        "USER_ID": user_id,
                        "INGREDIENT_ID": ing_id,
                        "QUANTITY": round(random.uniform(0.5, 10.0), 2),
                        "UNIT": random.choice(COMMON_UNITS),
                    })

            print(f"User {user_id} has {len(user_ingredient_set)} unique pantry items.")

        # 3) Bulk load into PANTRY
        if not pantry_records:
            print("No pantry records generated.")
            return

        pantry_df = pd.DataFrame(pantry_records)
        pantry_df["LOADED_AT"] = datetime.utcnow()
        pantry_df.columns = pantry_df.columns.str.upper()

        print(f"Generated {len(pantry_records)} synthetic pantry entries for {len(TEST_USERS)} users.")

        truncate_sql = f"TRUNCATE TABLE {DB}.{SCHEMA}.PANTRY"
        print(f"Executing: {truncate_sql}")
        hook.run(truncate_sql)

        hook.insert_rows(
            table="PANTRY",
            rows=pantry_df.values.tolist(),
            target_fields=pantry_df.columns.tolist(),
            commit_every=pantry_df.shape[0],
            database=DB,
            schema=SCHEMA,
        )

        print("Synthetic pantry data loaded successfully with strong recipe alignment.")


    # DAG Flow
    tables = create_tables()
    recipes = extract_recipes()
    load = load_recipes(recipes)
    
   
    load_pantry = load_synthetic_pantry_data()
    
    tables >> recipes >> load >> load_pantry