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
        background-color: #0a1929;
    }
    .main {
        background: radial-gradient(circle at top, #1a2332 0%, #0c1621 50%, #050d15 100%);
        color: #e3f2fd;
    }
    .snow-card {
        border-radius: 18px;
        padding: 1.2rem 1.5rem;
        background: linear-gradient(135deg, rgba(10,25,41,0.95) 0%, rgba(13,30,48,0.95) 100%);
        border: 1px solid rgba(41,182,246,0.3);
        box-shadow: 0 8px 32px rgba(41,182,246,0.15), 0 0 60px rgba(41,182,246,0.05);
        margin-bottom: 1rem;
    }
    .snow-metric {
        font-size: 1.8rem;
        font-weight: 700;
        color: #29b6f6;
        text-shadow: 0 0 20px rgba(41,182,246,0.5);
    }
    .snow-metric-label {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #4fc3f7;
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
        background: rgba(102,187,106,0.2);
        color: #81c784;
        border: 1px solid rgba(102,187,106,0.5);
        box-shadow: 0 0 10px rgba(102,187,106,0.3);
    }
    .pill-almost {
        background: rgba(255,167,38,0.15);
        color: #ffb74d;
        border: 1px solid rgba(255,167,38,0.5);
        box-shadow: 0 0 10px rgba(255,167,38,0.2);
    }
    .pill-low {
        background: rgba(41,182,246,0.2);
        color: #4fc3f7;
        border: 1px solid rgba(41,182,246,0.5);
        box-shadow: 0 0 10px rgba(41,182,246,0.3);
    }
    .recipe-title {
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 0.15rem;
        color: #e3f2fd;
    }
    .recipe-subtitle {
        font-size: 0.8rem;
        color: #81d4fa;
        margin-bottom: 0.5rem;
    }
    .recipe-instructions {
        font-size: 0.9rem;
        line-height: 1.5;
        color: #b3e5fc;
        white-space: pre-wrap;
    }
    .header-with-bg {
        position: relative;
        border-radius: 18px;
        padding: 1.2rem 1.5rem;
        background: linear-gradient(135deg, rgba(10,25,41,0.95) 0%, rgba(13,30,48,0.95) 100%);
        border: 1px solid rgba(41,182,246,0.4);
        box-shadow: 0 8px 32px rgba(41,182,246,0.2), 0 0 80px rgba(41,182,246,0.1);
        margin-bottom: 1.5rem;
        overflow: hidden;
    }
    .header-with-bg::before {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-image: url('https://images.unsplash.com/photo-1556910103-1c02745aae4d?w=1200&q=80');
        background-size: cover;
        background-position:75% 75%;
        opacity: 0.5;
        z-index: 0;
    }
    .header-content {
        position: relative;
        z-index: 1;
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


@st.cache_data(show_spinner=False)
def get_pantry_ingredients(user_pk: str) -> list[str]:
    """
    For a given user, return the list of ingredient names in their pantry.
    """
    params = get_connection_params()
    db = params["database"]

    conn = get_snowflake_connection()
    try:
        query = f"""
            SELECT i.name AS ingredient_name
            FROM {db}.{RAW_SCHEMA}.PANTRY p
            JOIN {db}.{RAW_SCHEMA}.INGREDIENT i
              ON p.INGREDIENT_ID = i.INGREDIENT_ID
            WHERE p.USER_ID = %s
            ORDER BY ingredient_name
        """
        df = pd.read_sql(query, conn, params=(user_pk,))
    finally:
        conn.close()

    if df.empty:
        return []
    return df["INGREDIENT_NAME"].tolist()


# ---------- Layout: Header ----------

st.markdown(
    """
    <div class="header-with-bg">
      <div class="header-content">
        <h1 style="margin:0; font-size:2.1rem; font-weight:700;">
        Snowchef
        </h1>
        <p style="margin-top:0.3rem; color:#81d4fa; font-size:0.95rem;">
          Smart pantry &amp; recipe recommendation warehouse powered by Snowflake, Airflow, dbt &amp; Streamlit.
        </p>
      </div>
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
        f"<p style='margin-top:0.75rem; font-size:0.9rem; color:#81d4fa;'>"
        f"Showing recipe matches for <b>{selected_user}</b> based on items pantry contents."
        f"</p>",
        unsafe_allow_html=True,
    )
    st.markdown('</div>', unsafe_allow_html=True)

# ---------- Visualizations: Shopping list + Pantry contents ----------

if not recipes_df.empty:
    viz_col1, viz_col2 = st.columns(2)

    # Shopping list - pie chart by category
    with viz_col1:
        st.markdown("#### 🛒 Shopping list by category")
        shopping_list = []
        
        for _, row in recipes_df.iterrows():
            missing_ingredients = get_missing_ingredients(row["user_pk"], row["recipe_pk"])
            shopping_list.extend(missing_ingredients)
        
        if shopping_list:
            # Define ingredient categories
            def categorize_ingredient(ingredient):
                ingredient_lower = ingredient.lower()
                
                if any(word in ingredient_lower for word in ['lettuce', 'tomato', 'onion', 'pepper', 'carrot', 'celery', 
                                                               'cucumber', 'spinach', 'kale', 'broccoli', 'cauliflower',
                                                               'asparagus', 'zucchini', 'mushroom', 'avocado', 'potato']):
                    return 'Produce'
                elif any(word in ingredient_lower for word in ['chicken', 'beef', 'pork', 'fish', 'salmon', 'tuna',
                                                                 'shrimp', 'turkey', 'lamb', 'tofu', 'tempeh', 'egg']):
                    return 'Proteins'
                elif any(word in ingredient_lower for word in ['milk', 'cheese', 'butter', 'cream', 'yogurt', 
                                                                 'sour cream', 'parmesan', 'mozzarella', 'cheddar']):
                    return 'Dairy'
                elif any(word in ingredient_lower for word in ['salt', 'pepper', 'garlic', 'ginger', 'cumin', 'paprika',
                                                                 'oregano', 'basil', 'thyme', 'rosemary', 'cinnamon',
                                                                 'chili', 'spice', 'herb', 'bay', 'parsley', 'cilantro']):
                    return 'Spices & Herbs'
                elif any(word in ingredient_lower for word in ['oil', 'vinegar', 'sauce', 'stock', 'broth', 'flour',
                                                                 'sugar', 'rice', 'pasta', 'noodle', 'beans', 'lentil',
                                                                 'quinoa', 'honey', 'syrup', 'soy sauce', 'tomato paste']):
                    return 'Pantry Staples'
                else:
                    return 'Other'
            
            # Categorize and count
            shopping_with_category = [
                {"ingredient": ing, "category": categorize_ingredient(ing)} 
                for ing in shopping_list
            ]
            
            category_counts = (
                pd.DataFrame(shopping_with_category)
                .groupby("category")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            
            # Create pie chart
            pie = (
                alt.Chart(category_counts)
                .mark_arc()
                .encode(
                    theta=alt.Theta("count:Q"),
                    color=alt.Color("category:N", 
                                    scale=alt.Scale(scheme='category20'),
                                    legend=alt.Legend(title="Category")),
                    tooltip=["category", "count"],
                )
                .properties(height=300)
            )
            st.altair_chart(pie, use_container_width=True)
            
            # Show detailed list by category
            with st.expander("View detailed shopping list"):
                for category in category_counts["category"]:
                    items = [item["ingredient"] for item in shopping_with_category if item["category"] == category]
                    unique_items = sorted(set(items))
                    st.markdown(f"**{category}:** {', '.join(unique_items)}")
        else:
            st.write("No missing ingredients - you can cook everything!")

    # Pantry contents - individual ingredients
    with viz_col2:
        st.markdown("#### 🥘 What's in your pantry")
        pantry_items = get_pantry_ingredients(selected_user)
        
        if pantry_items:
            # Create dataframe with individual ingredients
            pantry_df = pd.DataFrame({"ingredient": pantry_items})
            
            # Count occurrences and sort
            ingredient_counts = (
                pantry_df.groupby("ingredient")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
                .head(15)  # Show top 15 ingredients
            )
            
            bar = (
                alt.Chart(ingredient_counts)
                .mark_bar()
                .encode(
                    x=alt.X("count:Q", title="Quantity"),
                    y=alt.Y("ingredient:N", sort="-x", title="Ingredient"),
                    color=alt.value("#66bb6a"),
                    tooltip=["ingredient", "count"],
                )
                .properties(height=400)
            )
            st.altair_chart(bar, use_container_width=True)
        else:
            st.write("No pantry data available.")

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

# ---------- Helper: render a single card (fixed missing count) ----------

def render_recipe_card(row, show_instructions_inline: bool, show_missing: bool, show_matched: bool = True):
    match_pct = round(row["match_percentage"], 1)
    matched = int(row["matched_count"])
    total_req = int(row["total_required_count"])
    
    # Only fetch missing ingredients when we need to display the list
    if show_missing:
        missing_names = get_missing_ingredients(row["user_pk"], row["recipe_pk"])
        missing = len(missing_names)
    else:
        missing_names = []
        missing = int(row["missing_count"])

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

    # Stats row – conditional display
    if show_matched:
        stat1, stat2 = st.columns(2)
        with stat1:
            st.caption("Matched ingredients")
            st.write(f"**{matched} / {total_req}**")
        with stat2:
            st.caption("Missing ingredients")
            st.write(f"**{missing}**")
    else:
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
            render_recipe_card(row, show_instructions_inline=True, show_missing=False, show_matched=True)

    st.markdown("")

# ---------- Section 2: Other good matches (grid) ----------

combined_df = pd.concat([remaining_can_df, almost_df], ignore_index=True)

if not combined_df.empty:
    st.markdown("### 🍽️ More recipes you're close to")

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
                    show_matched=False,
                )