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

try:
    # Load the NDC JSON file
    with open('data/drug-ndc-0001-of-0001.json', 'r') as f:
        ndc_data = json.load(f)
    
    # Extract results into DataFrame
    df_ndc = pd.DataFrame(ndc_data['results'])
    print(f"   Loaded {len(df_ndc)} NDC records")
    
    # Create core NDC products table
    ndc_core_columns = [
        'product_ndc', 'generic_name', 'labeler_name', 'brand_name',
        'finished', 'marketing_category', 'dosage_form', 'route',
        'product_type', 'marketing_start_date', 'application_number'
    ]
    
    # Only keep columns that exist
    available_columns = [col for col in ndc_core_columns if col in df_ndc.columns]
    ndc_core = df_ndc[available_columns].copy()
    
    # Save core NDC table
    ndc_core.to_csv('data/ndc_core.csv', index=False)
    print(f"   ✓ Created ndc_core.csv ({len(ndc_core)} products)")
    
    # Extract packaging information (one-to-many relationship)
    packaging_records = []
    for idx, row in df_ndc.iterrows():
        product_ndc = row.get('product_ndc')
        packaging_list = row.get('packaging', [])
        
        if isinstance(packaging_list, list):
            for pkg in packaging_list:
                packaging_records.append({
                    'product_ndc': product_ndc,
                    'package_ndc': pkg.get('package_ndc'),
                    'description': pkg.get('description'),
                    'marketing_start_date': pkg.get('marketing_start_date')
                })
    
    ndc_packaging = pd.DataFrame(packaging_records)
    ndc_packaging.to_csv('data/ndc_packaging.csv', index=False)
    print(f"   ✓ Created ndc_packaging.csv ({len(ndc_packaging)} packages)")
    
except Exception as e:
    print(f"   ✗ Error processing NDC dataset: {e}")

# ============================================
# Process Drug Shortages Dataset
# ============================================
print("\n2. Processing Drug Shortages dataset...")

try:
    # Load the drug shortage JSON file
    with open('data/drug-shortages-0001-of-0001.json', 'r') as f:
        shortage_data = json.load(f)
    
    # Extract results into DataFrame
    df_shortages = pd.DataFrame(shortage_data['results'])
    print(f"   Loaded {len(df_shortages)} shortage records")
    
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
    
    # Save core shortage table
    shortage_core.to_csv('data/drug_shortages_core.csv', index=False)
    print(f"   ✓ Created drug_shortages_core.csv ({len(shortage_core)} shortages)")
    
    # Extract contact information
    contact_records = []
    for idx, row in df_shortages.iterrows():
        package_ndc = row.get('package_ndc')
        contact_info = row.get('contact_info')
        
        if contact_info:
            contact_records.append({
                'package_ndc': package_ndc,
                'contact_info': str(contact_info)
            })
    
    if len(contact_records) > 0:
        shortage_contacts = pd.DataFrame(contact_records)
        shortage_contacts.to_csv('data/shortage_contacts.csv', index=False)
        print(f"   ✓ Created shortage_contacts.csv ({len(shortage_contacts)} contacts)")
    else:
        # Create empty template if no contacts
        shortage_contacts = pd.DataFrame(columns=['package_ndc', 'contact_info'])
        shortage_contacts.to_csv('data/shortage_contacts.csv', index=False)
        print(f"   ✓ Created shortage_contacts.csv (empty - no contacts in data)")
    
except Exception as e:
    print(f"   ✗ Error processing Drug Shortages dataset: {e}")

print("\n✓ Data processing complete!")
print("\nGenerated files in data/ directory:")
print("  - ndc_core.csv")
print("  - ndc_packaging.csv")
print("  - drug_shortages_core.csv")
print("  - shortage_contacts.csv")
print("\nNext step: Load these CSV files into MySQL")
