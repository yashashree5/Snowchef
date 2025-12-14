# Snowchef

Snowchef is a data-driven recipe recommendation system that helps users determine what they can cook based on the ingredients already available in their pantry.  
The project uses **Airflow**, **Snowflake**, **dbt**, and **Streamlit** to create an end-to-end data pipeline and an interactive web application.

---

## ✨ Key Features

- 🔎 **Pantry-aware recipe recommendations**  
  Recipes are ranked based on ingredient availability and missing ingredients.

- 📥 **Automated data ingestion with Airflow**  
  Recipe and pantry data is ingested and refreshed on a schedule.

- 🧮 **Data transformation using dbt**  
  Raw data is cleaned, standardized, and modeled into analytics-ready tables.

- 🖥️ **Interactive Streamlit UI (`app.py`)**  
  Users explore their pantry, view recommendations, and inspect recipe details.

---

## 🧱 Architecture Overview

Snowchef includes four main components:

1. **Airflow**  
   - Orchestrates ingestion and transformation workflows.  
   - Ensures pipeline automation and reproducibility.

2. **Snowflake**  
   - Stores raw recipe data, pantry data, and transformed dbt models.

3. **dbt**  
   - Builds staging, core, and mart models for recipe logic and UI queries.

4. **Streamlit (`app.py`)**  
   - Provides a lightweight and interactive front-end for users to explore recipe recommendations.

---

## 📁 Repository Structure

```text
Snowchef/
├─ airflow/               # Airflow DAGs and configurations
├─ dbt/                   # dbt models, seeds, and transformations
├─ app.py                 # Streamlit dashboard application
├─ docker-compose.yaml    # Docker setup for Airflow components
├─ requirements.txt       # Python dependencies
└─ README.md              # Documentation
