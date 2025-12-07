flowchart TB

%% =======================
%%  DATA INPUT SOURCES
%% =======================
subgraph Sources["DATA SOURCES"]
    API["📦 Spoonacular API\n(recipes, ingredients, instructions)"]
    SYNTH["🧪 Synthetic User Generator\n(generates pantry items)"]
end

%% =======================
%%  ORCHESTRATION (AIRFLOW)
%% =======================
subgraph Airflow["🛠️ Airflow Orchestration"]
    ETL["snowchef_ETL_Dag\n(Python ETL)\n- Fetch recipes\n- Load raw tables\n- Generate synthetic pantry"]
    ELT["snowchef_ELT_Dag\n(dbt runs/tests/snapshots)"]
end

%% =======================
%%  SNOWFLAKE WAREHOUSE
%% =======================
subgraph Snowflake["❄️ Snowflake Data Warehouse"]

    subgraph RAW["RAW Schema\n(USER_DB_PUMA.RAW)"]
        RAW_REC["RECIPE"]
        RAW_ING["INGREDIENT"]
        RAW_RI["RECIPE_INGREDIENT"]
        RAW_PAN["PANTRY\n(synthetic users)"]
    end

    subgraph DBT["dbt Transformations"]
        STG_REC["stg_recipe"]
        STG_ING["stg_ingredient"]
        STG_RI["stg_recipe_ingredient"]
        STG_PAN["stg_pantry"]

        MART_MATCH["mart_recipe_availability\n(user × recipe match scores)"]
        MART_INV["mart_user_inventory\n(user pantry overview)"]
    end
end

%% =======================
%%  APPLICATION LAYERS
%% =======================
subgraph Apps["📱 Application Layer"]
    STREAMLIT["Streamlit App\n“What can I cook now?”\n+ visual insights"]
    TABLEAU["Tableau Dashboards\n(optional BI layer)"]
end

%% =======================
%%  DATA FLOW LINKS
%% =======================

API --> ETL
SYNTH --> ETL

ETL --> RAW_REC
ETL --> RAW_ING
ETL --> RAW_RI
ETL --> RAW_PAN

ELT --> STG_REC
ELT --> STG_ING
ELT --> STG_RI
ELT --> STG_PAN

STG_REC --> MART_MATCH
STG_RI --> MART_MATCH
STG_PAN --> MART_MATCH

STG_PAN --> MART_INV
STG_ING --> MART_INV

MART_MATCH --> STREAMLIT
MART_INV --> STREAMLIT

MART_MATCH --> TABLEAU
