import streamlit as st
import pandas as pd
import snowflake.connector
from typing import Optional
import altair as alt
import json

# ---------- Basic Page Config ----------
st.set_page_config(
    page_title="Snowchef – Smart Pantry & Recipe Recommender",
    page_icon="🍳",
    layout="wide",
)

# ---------- A little custom CSS for nicer UI ----------
st.markdown(
    """
    <style>
    body {
        background-color: #0f172a;
    }
    .main {
        background: radial-gradient(circle at top, #0f172a 0, #020617 60%);
        color: #e5e7eb;
    }
    .snow-card {
        border-radius: 18px;
        padding: 1.2rem 1.5rem;
        background: rgba(15,23,42,0.85);
        border: 1px solid rgba(148,163,184,0.4);
        box-shadow: 0 18px 40px rgba(15,23,42,0.9);
        margin-bottom: 1rem;
    }
    .snow-metric {
        font-size: 1.8rem;
        font-weight: 700;
        color: #fbbf24;
    }
    .snow-metric-label {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #9ca3af;
    }
    .snow-pill {
        display: inline-block;
        padding: 0.15rem 0.45rem;
        border-radius: 999px;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-right: 0.25rem;
    }
    .pill-can-now {
        background: rgba(22,163,74,0.15);
        color: #4ade80;
        border: 1px solid rgba(34,197,94,0.5);
    }
    .pill-almost {
        background: rgba(234,179,8,0.1);
        color: #facc15;
        border: 1px solid rgba(250,204,21,0.6);
    }
    .pill-low {
        background: rgba(148,163,184,0.25);
        color: #e5e7eb;
        border: 1px solid rgba(148,163,184,0.6);
    }
    .recipe-title {
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 0.15rem;
    }
    .recipe-subtitle {
        font-size: 0.8rem;
        color: #9ca3af;
        margin-bottom: 0.5rem;
    }
    .recipe-instructions {
        font-size: 0.9rem;
        line-height: 1.5;
        color: #e5e7eb;
        white-space: pre-wrap;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

RAW_SCHEMA = "RAW"  # where RECIPE_INGREDIENT, PANTRY, INGREDIENT live

# ---------- Snowflake Helpers ----------

@st.cache_data(show_spinner=False)
def get_connection_params():
    """Read Snowflake connection params from Streamlit secrets."""
    cfg = st.secrets["snowflake"]
    return {
        "user": cfg["user"],
        "password": cfg["password"],
        "account": cfg["account"],
        "warehouse": cfg["warehouse"],
        "database": cfg["database"],
        "schema": cfg["schema"],  # mart schema, e.g. RAW_ANALYTICS
        "role": cfg.get("role"),
    }


def get_snowflake_connection():
    params = get_connection_params()
    conn = snowflake.connector.connect(
        user=params["user"],
        password=params["password"],
        account=params["account"],
        warehouse=params["warehouse"],
        database=params["database"],
        schema=params["schema"],
        role=params["role"],
    )
    return conn


@st.cache_data(show_spinner=False)
def load_users() -> pd.DataFrame:
    """Load distinct users from mart table."""
    conn = get_snowflake_connection()
    try:
        query = """
            SELECT DISTINCT user_pk
            FROM MART_RECIPE_AVAILABILITY
            ORDER BY user_pk
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df


@st.cache_data(show_spinner=True)
def load_user_recipes(
    user_pk: str,
    max_missing: int,
    title_search: Optional[str] = None,
) -> pd.DataFrame:
    """Load recipes for a given user with filters from the mart."""
    conn = get_snowflake_connection()
    try:
        base_query = """
            SELECT
                user_pk,
                recipe_pk,
                title,
                instructions,
                cuisine,
                servings,
                source_url,
                diet_flags,
                total_required_count,
                matched_count,
                missing_count,
                match_percentage,
                can_cook_now,
                almost_can_cook
            FROM MART_RECIPE_AVAILABILITY
            WHERE user_pk = %s
              AND missing_count <= %s
        """

        params = [user_pk, max_missing]

        if title_search:
            base_query += " AND LOWER(title) LIKE %s\n"
            params.append(f"%{title_search.lower()}%")

        base_query += """
            ORDER BY
                can_cook_now DESC,
                match_percentage DESC,
                missing_count ASC,
                title ASC
        """

        cur = conn.cursor()
        cur.execute(base_query, params)
        cols = [c[0].lower() for c in cur.description]
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=cols)
    finally:
        conn.close()

    return df


@st.cache_data(show_spinner=False)
def get_missing_ingredients(user_pk: str, recipe_pk: str) -> list[str]:
    """
    For a given user and recipe, return the list of ingredient names
    that are required by the recipe but not present in the user's pantry.
    """
    params = get_connection_params()
    db = params["database"]

    conn = get_snowflake_connection()
    try:
        query = f"""
            SELECT i.name AS ingredient_name
            FROM {db}.{RAW_SCHEMA}.RECIPE_INGREDIENT ri
            JOIN {db}.{RAW_SCHEMA}.INGREDIENT i
              ON ri.INGREDIENT_ID = i.INGREDIENT_ID
            LEFT JOIN {db}.{RAW_SCHEMA}.PANTRY p
              ON p.INGREDIENT_ID = ri.INGREDIENT_ID
             AND p.USER_ID = %s
            WHERE ri.RECIPE_ID = %s
              AND p.INGREDIENT_ID IS NULL
            ORDER BY ingredient_name
        """
        df = pd.read_sql(query, conn, params=(user_pk, recipe_pk))
    finally:
        conn.close()

    if df.empty:
        return []
    return df["INGREDIENT_NAME"].tolist()


# ---------- Layout: Header ----------

st.markdown(
    """
    <div class="snow-card" style="margin-bottom: 1.5rem;">
      <h1 style="margin:0; font-size:2.1rem; font-weight:700;">
        🍳 Snowchef
      </h1>
      <p style="margin-top:0.3rem; color:#9ca3af; font-size:0.95rem;">
        Smart pantry &amp; recipe recommendation warehouse powered by Snowflake, Airflow, dbt &amp; Streamlit.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------- Sidebar Filters ----------

with st.sidebar:
    st.markdown("### 👤 Pick a test user")
    users_df = load_users()
    if users_df.empty:
        st.error("No users found in MART_RECIPE_AVAILABILITY. Run your ETL/ELT first.")
        st.stop()

    selected_user = st.selectbox(
        "Synthetic user",
        users_df["USER_PK"],
        index=0,
        format_func=lambda x: f"{x}",
    )

    st.markdown("---")
    st.markdown("### 🎯 Recommendation settings")

    view_mode = st.radio(
        "Show recipes that…",
        [
            "I can cook now only",
            "I can cook now + almost there",
            "All recipes within missing limit",
        ],
        index=1,
    )

    max_missing = st.slider(
        "Maximum missing ingredients",
        min_value=0,
        max_value=5,
        value=2,
        step=1,
        help="Recipes can miss at most this many ingredients.",
    )

    title_search = st.text_input(
        "Search by recipe title (optional)",
        placeholder="e.g. pasta, curry, salad...",
    )

    max_recipes = st.slider(
        "Maximum recipes to show",
        min_value=3,
        max_value=30,
        value=12,
        step=3,
    )

    st.markdown("---")
    st.markdown(
        "💡 These users are synthetic. In a real app, this would reflect the logged-in user’s live pantry."
    )

# ---------- Load Data ----------

recipes_df = load_user_recipes(
    user_pk=selected_user,
    max_missing=max_missing,
    title_search=title_search or None,
)

# Apply view-mode filters
if view_mode == "I can cook now only":
    recipes_df = recipes_df[recipes_df["can_cook_now"] == True].copy()
elif view_mode == "I can cook now + almost there":
    recipes_df = recipes_df[
        (recipes_df["can_cook_now"] == True) | (recipes_df["almost_can_cook"] == True)
    ].copy()
# else: keep all recipes within missing limit

# Limit total recipes for a shorter page
if not recipes_df.empty and len(recipes_df) > max_recipes:
    recipes_df = recipes_df.head(max_recipes)

# ---------- Metrics card ----------

with st.container():
    st.markdown('<div class="snow-card">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)

    total_recipes = len(recipes_df)
    can_now_total = int(recipes_df["can_cook_now"].sum()) if not recipes_df.empty else 0
    almost_total = int(recipes_df["almost_can_cook"].sum()) if not recipes_df.empty else 0

    with col1:
        st.markdown('<div class="snow-metric-label">RECIPES RETURNED</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="snow-metric">{total_recipes}</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="snow-metric-label">CAN COOK NOW</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="snow-metric">{can_now_total}</div>', unsafe_allow_html=True)

    with col3:
        st.markdown('<div class="snow-metric-label">ALMOST CAN COOK</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="snow-metric">{almost_total}</div>', unsafe_allow_html=True)

    st.markdown(
        f"<p style='margin-top:0.75rem; font-size:0.9rem; color:#9ca3af;'>"
        f"Showing recipe matches for <b>{selected_user}</b> based on pantry contents in Snowflake."
        f"</p>",
        unsafe_allow_html=True,
    )
    st.markdown('</div>', unsafe_allow_html=True)

# ---------- Visualizations: Status + Dietary preferences ----------

if not recipes_df.empty:
    viz_col1, viz_col2 = st.columns(2)

    # Status pie/donut chart
    status_df = recipes_df.copy()

    def label_status(row):
        if row["can_cook_now"]:
            return "Can cook now"
        elif row["almost_can_cook"]:
            return "Almost there"
        else:
            return "Other match"

    status_df["status"] = status_df.apply(label_status, axis=1)
    status_counts = (
        status_df.groupby("status")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    with viz_col1:
        st.markdown("#### 🍕 Match breakdown")
        if not status_counts.empty:
            chart = (
                alt.Chart(status_counts)
                .mark_arc(innerRadius=50)
                .encode(
                    theta="count",
                    color=alt.Color("status", legend=None),
                    tooltip=["status", "count"],
                )
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.write("No data for status breakdown.")

    # Dietary preferences bar chart from DIET_FLAGS
    with viz_col2:
        st.markdown("#### 🥗 Dietary styles you can cook")
        diet_rows = []

        for _, row in recipes_df.iterrows():
            diets = row.get("diet_flags")
            if diets is None:
                continue

            # Snowflake may return ARRAY or JSON string
            diets_parsed = None
            if isinstance(diets, str):
                try:
                    diets_parsed = json.loads(diets)
                except Exception:
                    diets_parsed = [diets]
            else:
                diets_parsed = diets

            if not diets_parsed:
                continue

            for d in diets_parsed:
                label = (str(d) or "").strip()
                if label:
                    diet_rows.append({"diet": label})

        if diet_rows:
            diet_df = (
                pd.DataFrame(diet_rows)
                .groupby("diet")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
                .head(10)
            )

            bar = (
                alt.Chart(diet_df)
                .mark_bar()
                .encode(
                    x=alt.X("count:Q", title="Number of recipes"),
                    y=alt.Y("diet:N", sort="-x", title="Diet / preference"),
                    tooltip=["diet", "count"],
                )
            )
            st.altair_chart(bar, use_container_width=True)
        else:
            st.write("No dietary preference data available.")

st.markdown("")

if recipes_df.empty:
    st.warning("No recipes found for the current settings. Try increasing the missing limit.")
    st.stop()

# ---------- Split into top 3 full matches and the rest ----------

can_now_df = recipes_df[recipes_df["can_cook_now"] == True].copy()
can_now_df = can_now_df.sort_values(
    by=["match_percentage", "missing_count"],
    ascending=[False, True],
)
top3_can_df = can_now_df.head(3)
remaining_can_df = can_now_df.iloc[3:]

almost_df = recipes_df[recipes_df["can_cook_now"] == False].copy()

# ---------- Helper: render a single card (Recipe ID removed) ----------

def render_recipe_card(row, show_instructions_inline: bool, show_missing: bool):
    match_pct = round(row["match_percentage"], 1)
    missing = int(row["missing_count"])
    matched = int(row["matched_count"])
    total_req = int(row["total_required_count"])

    if row["can_cook_now"]:
        pill_class = "pill-can-now"
        pill_label = "Can cook now"
    elif row["almost_can_cook"]:
        pill_class = "pill-almost"
        pill_label = "Almost there"
    else:
        pill_class = "pill-low"
        pill_label = "Low match"

    st.markdown('<div class="snow-card">', unsafe_allow_html=True)

    # Title + subtitle
    st.markdown(
        f"""
        <div class="recipe-title">{row['title']}</div>
        <div class="recipe-subtitle">
            {row['cuisine'] or 'Unknown cuisine'} · Serves {row['servings'] or '?'}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Pills (status + % match)
    tag_col1, tag_col2 = st.columns([0.6, 0.4])
    with tag_col2:
        st.markdown(
            f"""
            <div style="text-align:right;">
                <span class="snow-pill {pill_class}">{pill_label}</span>
                <span class="snow-pill pill-low">{match_pct:.0f}% match</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Stats row – now ONLY 2 columns (no "Match percentage" stat)
    stat1, stat2 = st.columns(2)
    with stat1:
        st.caption("Matched ingredients")
        st.write(f"**{matched} / {total_req}**")
    with stat2:
        st.caption("Missing ingredients")
        st.write(f"**{missing}**")

    # Instructions / expander
    if show_instructions_inline:
        st.caption("Instructions")
        instructions = row["instructions"] or "No instructions available."
        st.markdown(
            f"<div class='recipe-instructions'>{instructions}</div>",
            unsafe_allow_html=True,
        )
        if row["source_url"]:
            st.markdown(
                f"[Open original recipe ↗]({row['source_url']})",
                unsafe_allow_html=False,
            )
    else:
        with st.expander("View instructions & missing ingredients"):
            instructions = row["instructions"] or "No instructions available."
            st.markdown(
                f"<div class='recipe-instructions'>{instructions}</div>",
                unsafe_allow_html=True,
            )
            if row["source_url"]:
                st.markdown(
                    f"[Open original recipe ↗]({row['source_url']})",
                    unsafe_allow_html=False,
                )

            if show_missing:
                missing_names = get_missing_ingredients(
                    row["user_pk"], row["recipe_pk"]
                )
                if missing_names:
                    st.markdown("**Missing ingredients:**")
                    st.write(", ".join(missing_names))
                else:
                    st.markdown("**No missing ingredients.**")

    st.markdown("</div>", unsafe_allow_html=True)


# ---------- Section 1: Top 3 recipes you can cook now (grid) ----------

if not top3_can_df.empty:
    st.markdown(f"### ✅ Top recipes you can cook right now for `{selected_user}`")

    num_cards = len(top3_can_df)
    cols = st.columns(num_cards)

    for col, (_, row) in zip(cols, top3_can_df.iterrows()):
        with col:
            render_recipe_card(row, show_instructions_inline=True, show_missing=False)

    st.markdown("")

# ---------- Section 2: Other good matches (grid) ----------

combined_df = pd.concat([remaining_can_df, almost_df], ignore_index=True)

if not combined_df.empty:
    st.markdown("### 🍽️ More recipes you’re close to")

    cards = combined_df.to_dict("records")
    num_cols = 3

    for i in range(0, len(cards), num_cols):
        row_cards = cards[i : i + num_cols]
        cols = st.columns(len(row_cards))
        for col, row_dict in zip(cols, row_cards):
            with col:
                row_series = pd.Series(row_dict)
                render_recipe_card(
                    row_series,
                    show_instructions_inline=False,
                    show_missing=True,
                )
