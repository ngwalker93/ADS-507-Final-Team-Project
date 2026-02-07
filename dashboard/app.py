"""
STREAMLIT DASHBOARD APPLICATION
This module initializes and runs the Streamlit dashboard application.
"""


#Our main focus is to answer
#Which manufacturers have the highest shortage risk?
#Do branded drugs have longer shortage durations than generics?
#Which package types are most vulnerable to shortages?


# Import necessary libraries
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="FDA Shortage Dashboard", layout="wide")
st.title("FDA Drug Shortage Monitoring Dashboard")

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "fda_shortage_db")

engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,
)

def query_df(sql: str) -> pd.DataFrame:
    return pd.read_sql(text(sql), engine)

only_current = st.sidebar.checkbox("Only Current shortages", value=True)
where_status = "WHERE status = 'Current'" if only_current else "WHERE 1=1"

# Manufacturer risk
st.subheader("1) Manufacturers with highest shortage risk")
df_manu = query_df("""
SELECT company_name,
       current_affected_packages,
       current_affected_products
FROM current_manufacturer_risk
ORDER BY current_affected_packages DESC
LIMIT 15;
""")
st.dataframe(df_manu, use_container_width=True)
st.bar_chart(df_manu.set_index("company_name")[["current_affected_packages"]])

st.divider()

# Branded vs generic duration proxy
st.subheader("2) Branded vs Generic: average days active (proxy)")

df_brand = query_df(f"""
SELECT
  CASE
    WHEN brand_name IS NOT NULL AND brand_name <> '' THEN 'Branded'
    ELSE 'Generic/Unbranded'
  END AS drug_type,
  COUNT(*) AS shortage_count,
  ROUND(AVG(
    DATEDIFF(
      CURDATE(),
      COALESCE(
        STR_TO_DATE(initial_posting_date, '%Y-%m-%d'),
        STR_TO_DATE(initial_posting_date, '%Y%m%d')
      )
    )
  ), 1) AS avg_days_active
FROM shortages_with_ndc
{where_status}
  AND initial_posting_date IS NOT NULL
GROUP BY drug_type;
""")
st.dataframe(df_brand, use_container_width=True)
st.bar_chart(df_brand.set_index("drug_type")[["avg_days_active"]])

st.divider()

# Package type vulnerability
st.subheader("3) Package types most vulnerable to shortages")

df_pkg = query_df(f"""
SELECT
  CASE
    WHEN LOWER(package_description) LIKE '%bottle%' THEN 'Bottle'
    WHEN LOWER(package_description) LIKE '%vial%' THEN 'Vial'
    WHEN LOWER(package_description) LIKE '%blister%' THEN 'Blister Pack'
    WHEN LOWER(package_description) LIKE '%carton%' THEN 'Carton'
    WHEN LOWER(package_description) LIKE '%kit%' THEN 'Kit'
    ELSE 'Other/Unknown'
  END AS package_type,
  COUNT(*) AS shortage_count
FROM shortages_with_ndc
{where_status}
  AND package_description IS NOT NULL
GROUP BY package_type
ORDER BY shortage_count DESC;
""")
st.dataframe(df_pkg, use_container_width=True)
st.bar_chart(df_pkg.set_index("package_type")[["shortage_count"]])

engine.dispose()
