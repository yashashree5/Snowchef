# Snowchef

Snowchef is a smart, data-driven kitchen assistant that predicts what you can cook based on the ingredients in your pantry.  
It uses a modern analytics stack — **Airflow**, **Snowflake**, **dbt**, **Snowpark**, and **Tableau** — to build an end-to-end pipeline from raw recipe data all the way to recipe recommendations and dashboards.

---

## ✨ What Snowchef Does

- 🔎 **Understands your pantry**  
  Ingests ingredient and pantry data into Snowflake and keeps it fresh with Airflow.

- 🍽️ **Recommends recipes**  
  Uses Snowpark and Python to score recipes based on:
  - Ingredients you already have  
  - Missing ingredients and their cost/availability  
  - Dietary tags (e.g. vegetarian, gluten-free) — if available in your data  

- 🔁 **Transforms data with dbt**  
  Cleans and models raw recipe & pantry tables into analytics-ready marts.

- 📊 **Visualizes insights in Tableau**  
  Dashboards for:
  - Top recommended recipes
  - Ingredient substitutions
  - Pantry usage and food-waste trends
  - Popular cuisines / categories over time

---

## 🧱 Architecture Overview

**High-level flow:**

1. **Airflow** orchestrates ingestion jobs and dbt runs.
2. **Raw data** (recipes, ingredients, pantry) lands in **Snowflake**.
3. **dbt** models transform raw data into clean, documented models (staging → core → marts).
4. **Snowpark** (via Python) powers the recommendation logic.
5. **Tableau** connects to Snowflake to build dashboards on top of the marts.
6. (Optional) **`app.py`** can be used as a simple entry point / script for Snowpark jobs or a minimal API.

---

## 📁 Repository Structure

> Update this section if your structure changes.

```text
Snowchef/
├─ airflow/               # Airflow DAGs, configs and plugins
├─ dbt/                   # dbt project (models, seeds, snapshots, macros)
├─ app.py                 # Example Snowpark app / script entrypoint
├─ docker-compose.yaml    # Docker services (e.g. Airflow scheduler/webserver, etc.)
├─ requirements.txt       # Python dependencies
└─ README.md              # Project documentation

