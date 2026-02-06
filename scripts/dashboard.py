"""
FDA Drug Shortage Analysis Dashboard
Interactive dashboard displaying shortage metrics and insights
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
import getpass

# ============================================
# Page Configuration
# ============================================

st.set_page_config(
    page_title="FDA Drug Shortage Analysis",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# Database Connection
# ============================================

@st.cache_resource
def get_database_connection():
    """Create cached database connection"""

    DB_USER = 'root'
    DB_HOST = 'localhost'
    DB_PORT = '3306'
    DB_NAME = 'fda_shortage_db'

    DB_PASSWORD = getpass.getpass("Enter MySQL password for user 'root': ")

    connection_string = (
        f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    try:
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Database connection error: {e}")
        st.stop()

# ============================================
# Data Loading Functions
# ============================================

@st.cache_data(ttl=600)
def load_manufacturer_risk(_engine):
    query = """
    SELECT 
        company_name,
        current_affected_packages,
        current_affected_products
    FROM current_manufacturer_risk
    LIMIT 15
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=600)
def load_shortage_overview(_engine):
    query = """
    SELECT 
        COUNT(*) as total_shortages,
        COUNT(DISTINCT company_name) as affected_manufacturers,
        COUNT(DISTINCT product_ndc) as affected_products,
        SUM(CASE WHEN status = 'Current' THEN 1 ELSE 0 END) as current_shortages
    FROM shortages_with_ndc
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=600)
def load_brand_vs_generic(_engine):
    query = """
    SELECT 
        CASE 
            WHEN brand_name IS NOT NULL AND brand_name != '' THEN 'Branded Drug'
            ELSE 'Generic/Unbranded'
        END AS drug_type,
        COUNT(*) AS shortage_count
    FROM shortages_with_ndc
    WHERE status = 'Current'
    GROUP BY drug_type
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=600)
def load_route_analysis(_engine):
    query = """
    SELECT 
        CASE 
            WHEN route LIKE '%ORAL%' THEN 'Oral'
            WHEN route LIKE '%INTRAVENOUS%' OR route LIKE '%IV%' THEN 'Intravenous'
            WHEN route LIKE '%INJECTION%' THEN 'Injection'
            WHEN route LIKE '%TOPICAL%' THEN 'Topical'
            WHEN route LIKE '%INHALATION%' THEN 'Inhalation'
            ELSE 'Other'
        END AS administration_route,
        COUNT(*) AS shortage_count
    FROM shortages_with_ndc
    WHERE status = 'Current' AND route IS NOT NULL
    GROUP BY administration_route
    ORDER BY shortage_count DESC
    LIMIT 10
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=600)
def load_product_type_analysis(_engine):
    query = """
    SELECT 
        product_type,
        COUNT(*) AS shortage_count,
        COUNT(DISTINCT company_name) AS manufacturers
    FROM shortages_with_ndc
    WHERE status = 'Current' AND product_type IS NOT NULL
    GROUP BY product_type
    ORDER BY shortage_count DESC
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=600)
def load_detailed_shortages(_engine, limit=50):
    query = f"""
    SELECT 
        company_name AS manufacturer,
        shortage_generic_name AS drug_name,
        brand_name,
        shortage_dosage_form AS dosage_form,
        package_description,
        product_type,
        DATEDIFF(CURDATE(), STR_TO_DATE(initial_posting_date, '%Y%m%d')) AS days_active
    FROM shortages_with_ndc
    WHERE status = 'Current' AND product_ndc IS NOT NULL
    ORDER BY days_active DESC
    LIMIT {limit}
    """
    return pd.read_sql(query, _engine)

# ============================================
# Dashboard Layout
# ============================================

def main():

    st.title("💊 FDA Drug Shortage Analysis Dashboard")
    st.markdown("""
    This dashboard analyzes drug shortages by combining FDA's National Drug Code (NDC) database 
    with drug shortage data to reveal insights not possible from either dataset alone.
    """)

    engine = get_database_connection()

    st.sidebar.header("Dashboard Controls")
    st.sidebar.markdown("---")

    refresh_button = st.sidebar.button("🔄 Refresh Data", use_container_width=True)
    if refresh_button:
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Data Sources:**
    - FDA National Drug Code Database
    - FDA Drug Shortages Database

    **Last Updated:** Real-time
    """)

    st.header("📊 Key Metrics")

    overview = load_shortage_overview(engine)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Shortages", f"{overview['total_shortages'].iloc[0]:,}")
    col2.metric("Current Shortages", f"{overview['current_shortages'].iloc[0]:,}")
    col3.metric("Affected Manufacturers", f"{overview['affected_manufacturers'].iloc[0]:,}")
    col4.metric("Affected Products", f"{overview['affected_products'].iloc[0]:,}")

    st.markdown("---")

    st.header("🏭 Top Manufacturers by Shortage Risk")
    manufacturer_data = load_manufacturer_risk(engine)

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(
            manufacturer_data,
            x='company_name',
            y='current_affected_packages',
            title='Current Affected Packages by Manufacturer',
            labels={'company_name': 'Manufacturer', 'current_affected_packages': 'Affected Packages'},
            color='current_affected_packages',
            color_continuous_scale='Reds'
        )
        fig.update_layout(xaxis_tickangle=-45, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(manufacturer_data.head(10), hide_index=True, use_container_width=True)

    st.markdown("---")

    st.header("💊 Brand Name vs Generic Drug Shortages")
    brand_data = load_brand_vs_generic(engine)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.pie(
            brand_data,
            values='shortage_count',
            names='drug_type',
            title='Current Shortages: Branded vs Generic',
            color_discrete_sequence=['#FF6B6B', '#4ECDC4']
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        total = brand_data['shortage_count'].sum()
        branded = brand_data.loc[brand_data['drug_type'] == 'Branded Drug', 'shortage_count'].sum()
        generic = brand_data.loc[brand_data['drug_type'] == 'Generic/Unbranded', 'shortage_count'].sum()

        if total > 0:
            st.metric("Branded Drug Shortages", f"{(branded/total)*100:.1f}%")
            st.metric("Generic Drug Shortages", f"{(generic/total)*100:.1f}%")

    st.markdown("---")

    st.header("💉 Shortages by Route of Administration")
    route_data = load_route_analysis(engine)

    fig = px.bar(
        route_data,
        x='administration_route',
        y='shortage_count',
        title='Current Shortages by Administration Route',
        labels={'administration_route': 'Route', 'shortage_count': 'Number of Shortages'},
        color='shortage_count',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.header("📋 Prescription vs OTC Drug Shortages")
    product_type_data = load_product_type_analysis(engine)

    col1, col2 = st.columns([2, 1])

    with col1:
        fig = px.bar(
            product_type_data,
            x='product_type',
            y='shortage_count',
            title='Current Shortages by Product Type',
            labels={'product_type': 'Product Type', 'shortage_count': 'Shortages'},
            color='shortage_count',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.dataframe(product_type_data, hide_index=True, use_container_width=True)

    st.markdown("---")

    st.header("📑 Detailed Current Shortage List")

    num_records = st.slider("Number of records to display:", 10, 100, 50, 10)
    detailed_data = load_detailed_shortages(engine, num_records)

    st.dataframe(detailed_data, hide_index=True, use_container_width=True, height=400)

    csv = detailed_data.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name="fda_drug_shortages.csv",
        mime="text/csv"
    )

    st.markdown("""
    ---
    **ADS-507 Final Project** | University of San Diego | Mark Villanueva, Nancy Walker, Sheshma
    """)

# ============================================
# Run the Dashboard
# ============================================

if __name__ == "__main__":
    main()
