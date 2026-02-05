"""
FDA Data Processing Script
Cleans and normalizes the downloaded FDA datasets into structured CSV tables
"""

import pandas as pd
import json
import os

print("Starting data processing...")

# ============================================
# Process NDC Dataset
# ============================================
print("\n1. Processing NDC dataset...")

# Standardize raw FDA field names into ISO-8601 formate to facilitate downstream processing. 
# Ensures dates fields are loaded as native DATE in MySQL 
def yyyymmdd_to_iso(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    # normalize missing/placeholder values to NaT
    s = s.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA,
                   "None": pd.NA, "null": pd.NA, "NULL": pd.NA,
                   "0": pd.NA, "00000000": pd.NA})
    # parse YYYYMMDD to date objects
    dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    # Return ISO string for MySQL/CSV: "YYYY-MM-DD" (or <NA> for missing)
    return dt.dt.strftime("%Y-%m-%d").astype("string")

print("Starting data processing...")
print("\n1. Processing NDC dataset...")

try:
    # Load the NDC JSON file
    with open('data/drug-ndc-0001-of-0001.json', 'r',encoding="utf-8") as f:
        ndc_data = json.load(f)
    
    # Extract results into DataFrame
    df_ndc = pd.DataFrame(ndc_data['results'])
    print(f"   Loaded {len(df_ndc)} NDC records")

    # ---- Clean product_ndc to prevent primary key duplicate insertion ----
    if "product_ndc" in df_ndc.columns:
        df_ndc["product_ndc"] = df_ndc["product_ndc"].astype("string").str.strip()

    # ---- normalize product-level dates (only if present) ----
    date_cols = ["listing_expiration_date", "marketing_start_date", "marketing_end_date"]
    for col in date_cols:
        if col in df_ndc.columns:
            df_ndc[col] = yyyymmdd_to_iso(df_ndc[col])

     # ---- packaging table (1-to-many) ----

    packaging_rows = []
    if "packaging" in df_ndc.columns and "product_ndc" in df_ndc.columns:
        for _, row in df_ndc[["product_ndc", "packaging"]].dropna(subset=["product_ndc"]).iterrows():
            pkgs = row["packaging"]
            if not isinstance(pkgs, list):
                continue
        for pkg in pkgs:
            packaging_rows.append({
                "product_ndc": row["product_ndc"],
                "package_ndc": (str(pkg.get("package_ndc")).strip() if pkg.get("package_ndc") is not None else None),
                "description": pkg.get("description"),
                "marketing_start_date": pkg.get("marketing_start_date"),
                "marketing_end_date": pkg.get("marketing_end_date"),
                "sample": pkg.get("sample"),
            })

    # Build packaging DataFrame
    ndc_packaging = pd.DataFrame(packaging_rows)

    # normalize packaging-level dates to YYYY-MM-DD strings for MySQL DATE
    for col in ["marketing_start_date", "marketing_end_date"]:
        if col in ndc_packaging.columns:
         ndc_packaging[col] = pd.to_datetime(
                ndc_packaging[col].astype("string").str.strip().str.replace(r"\.0$", "", regex=True),
                format="%Y%m%d",
                errors="coerce"
            ).dt.strftime("%Y-%m-%d").astype("string")
    # ensure boolean "sample' column is boolean (and MySQL-friendly later)
    if "sample" in ndc_packaging.columns:
        ndc_packaging["sample"] = (
            ndc_packaging["sample"]
            .astype("string")
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False})
    )
    ndc_packaging["sample"] = ndc_packaging["sample"].fillna(False).astype(bool)
         
    # clean + dedupe packaging
    if not ndc_packaging.empty and "package_ndc" in ndc_packaging.columns:
        ndc_packaging["package_ndc"] = ndc_packaging["package_ndc"].astype("string").str.strip()
        ndc_packaging = ndc_packaging.dropna(subset=["package_ndc"])
        ndc_packaging = ndc_packaging[ndc_packaging["package_ndc"].ne("")]
        ndc_packaging = ndc_packaging.drop_duplicates(subset=["package_ndc"], keep="first")

    ndc_packaging.to_csv("data/ndc_packaging.csv", index=False)
    print(f"   ✓ Created ndc_packaging.csv ({len(ndc_packaging)} rows)")

    # ---- active ingredients table (1-to-many) ----
    active_rows = []

    if "active_ingredients" in df_ndc.columns and "product_ndc" in df_ndc.columns:
        for _, row in df_ndc[["product_ndc", "active_ingredients"]].dropna(subset=["product_ndc"]).iterrows():
            ings = row["active_ingredients"]
            if not isinstance(ings, list):
                continue
            for ing in ings:
                active_rows.append({
                    "product_ndc": row["product_ndc"],
                    "name": ing.get("name"),
                    "strength": ing.get("strength"),
                })
    # Build active ingredients DataFrame
    ndc_active_ingredients = pd.DataFrame(active_rows).drop_duplicates()

    ndc_active_ingredients.to_csv("data/ndc_active_ingredients.csv", index=False)
    print(f"   ✓ Created ndc_active_ingredients.csv ({len(ndc_active_ingredients)} rows)")

    # ---- core NDC table ----
    # Create core NDC products table
    ndc_core_columns = [
        'product_ndc', 'generic_name', 'labeler_name', 'brand_name',
        'finished', 'marketing_category', 'dosage_form', 'route',
        'product_type', 'marketing_start_date', 'application_number'
    ]

    # Only keep columns that exist
    available_columns = [col for col in ndc_core_columns if col in df_ndc.columns]
    ndc_core = df_ndc[available_columns].copy()

    #prevent primary key duplicate insertion
    if "product_ndc" in ndc_core.columns:
        ndc_core = ndc_core.dropna(subset=["product_ndc"])
        ndc_core["product_ndc"] = ndc_core["product_ndc"].astype(str).str.strip()
        ndc_core = ndc_core[ndc_core["product_ndc"].ne("")]
        ndc_core = ndc_core[ndc_core["product_ndc"].str.lower().ne("nan")]
        ndc_core = ndc_core.drop_duplicates(subset=["product_ndc"], keep="first")

    # Save core NDC table
    ndc_core.to_csv("data/ndc_core.csv", index=False)
    print(f"   ✓ Created ndc_core.csv ({len(ndc_core)} rows)")

except Exception as e:
    print(f"   ✗ Error processing NDC dataset: {e}")

# ============================================
# Process Drug Shortages Dataset
# ============================================
print("\n2. Processing Drug Shortages dataset...")

try:
    # Load the drug shortage JSON file
    with open('data/drug-shortages-0001-of-0001.json', 'r',encoding="utf-8") as f:
        shortage_data = json.load(f)
    
    # Extract results into DataFrame
    df_shortages = pd.DataFrame(shortage_data['results'])
    print(f"   Loaded {len(df_shortages)} shortage records")

    # Convert date columns to ISO-8601 format for MySQL DATE compatibility
    date_cols = ['discontinued_date', 'initial_posting_date', 'update_date']

    for col in date_cols:
        if col in df_shortages.columns:
            dt = pd.to_datetime(df_shortages[col], format="%m/%d/%Y", errors="coerce")
            df_shortages[col] = dt.dt.strftime("%Y-%m-%d").astype("string")
    
    # Create core shortage table with fields that actually exist
    shortage_core = pd.DataFrame({
        'package_ndc': df_shortages.get('package_ndc'),
        'generic_name': df_shortages.get('generic_name'),
        'company_name': df_shortages.get('company_name'),
        'status': df_shortages.get('status'),
        'therapeutic_category': df_shortages.get('therapeutic_category'),
        'initial_posting_date': df_shortages.get('initial_posting_date'),
        'update_date': df_shortages.get('update_date'),
        'dosage_form': df_shortages.get('presentation'),  # Use presentation field
        'reason': None  # Not available in FDA data
    })
    if "package_ndc" in shortage_core.columns:
        shortage_core = shortage_core.dropna(subset=["package_ndc"])
        shortage_core["package_ndc"] = shortage_core["package_ndc"].astype(str).str.strip()
        shortage_core = shortage_core[shortage_core["package_ndc"].ne("")]
        shortage_core = shortage_core[shortage_core["package_ndc"].str.lower().ne("nan")]
    # Save core shortage table
    shortage_core.to_csv('data/drug_shortages_core.csv', index=False)
    
    print(f"   ✓ Created drug_shortages_core.csv ({len(shortage_core)} shortages)")
    
    # Extract contact information
    contact_records = []
    for _, row in df_shortages.iterrows():
        package_ndc = row.get('package_ndc')
        if package_ndc is not None:
            package_ndc = str(package_ndc).strip()

        contact_info = row.get('contact_info')
        if contact_info:
            contact_records.append({
                'package_ndc': package_ndc,
                'contact_info': str(contact_info)
            })
    
    shortage_contacts = pd.DataFrame(contact_records, columns=["package_ndc", "contact_info"])
    shortage_contacts.to_csv("data/shortage_contacts.csv", index=False)
    if not shortage_contacts.empty:
        shortage_contacts = shortage_contacts.dropna(subset=["package_ndc"])
        shortage_contacts["package_ndc"] = shortage_contacts["package_ndc"].astype(str).str.strip()
        shortage_contacts = shortage_contacts[shortage_contacts["package_ndc"].ne("")]
        shortage_contacts = shortage_contacts[shortage_contacts["package_ndc"].str.lower().ne("nan")]
        shortage_contacts = shortage_contacts.drop_duplicates(subset=["package_ndc", "contact_info"], keep="first")
    print(f"  Created shortage_contacts.csv ({len(shortage_contacts)} contacts)")

except Exception as e:
    print(f"   ✗ Error processing Drug Shortages dataset: {e}")

print("\n✓ Data processing complete!")
print("\nGenerated files in data/ directory:")
print("  - ndc_core.csv")
print("  - ndc_packaging.csv")
print("  - drug_shortages_core.csv")
print("  - shortage_contacts.csv")
print("\nNext step: Load these CSV files into MySQL")
