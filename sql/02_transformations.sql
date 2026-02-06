-- ============================================
-- SQL Transformations
-- This file contains the required SQL transformation:
-- Joins drug shortages with NDC data to create enriched dataset
-- ============================================

USE fda_shortage_db;

-- ============================================
-- MAIN TRANSFORMATION: Join Shortages with NDC Data
-- This LEFT JOIN enriches shortage data with product details
-- Satisfies project requirement for SQL transformation
-- ============================================

DROP TABLE IF EXISTS shortages_with_ndc;

CREATE TABLE shortages_with_ndc AS
SELECT 
    -- Generate row ID
    ROW_NUMBER() OVER (ORDER BY s.package_ndc) as shortage_id,
    
    -- Shortage information
    s.package_ndc,
    s.generic_name AS shortage_generic_name,
    s.company_name,
    s.status,
    s.therapeutic_category,
    s.initial_posting_date,
    s.update_date,
    s.dosage_form AS shortage_dosage_form,
    s.reason,
    
    -- Packaging information
    p.product_ndc,
    p.description AS package_description,
    p.marketing_start_date AS package_marketing_start_date,
    
    -- Product information from NDC
    n.generic_name AS ndc_generic_name,
    n.labeler_name AS manufacturer,
    n.brand_name,
    n.finished,
    n.marketing_category,
    n.dosage_form AS ndc_dosage_form,
    n.route,
    n.product_type,
    n.application_number
    
FROM raw_drug_shortages s
LEFT JOIN raw_ndc_packaging p 
    ON s.package_ndc = p.package_ndc
LEFT JOIN raw_ndc n 
    ON p.product_ndc = n.product_ndc;

-- Add indexes for better query performance
CREATE INDEX idx_status ON shortages_with_ndc(status(50));
CREATE INDEX idx_company ON shortages_with_ndc(company_name(100));
CREATE INDEX idx_product_ndc ON shortages_with_ndc(product_ndc);

-- ============================================
-- ANALYSIS VIEW 1: Current Package Shortages
-- Focus on currently active shortages at package level
-- ============================================

CREATE OR REPLACE VIEW current_package_shortages AS
SELECT DISTINCT
    shortage_generic_name AS generic_name,
    company_name,
    status,
    product_ndc,
    package_ndc,
    package_description,
    therapeutic_category,
    initial_posting_date,
    update_date
FROM shortages_with_ndc
WHERE status = 'Current'
ORDER BY company_name, generic_name;

-- ============================================
-- ANALYSIS VIEW 2: Multi-Package Shortage Products
-- Identifies products with shortages affecting multiple packages
-- ============================================

CREATE OR REPLACE VIEW multi_package_shortages AS
SELECT 
    product_ndc,
    shortage_generic_name AS generic_name,
    company_name AS manufacturer,
    COUNT(DISTINCT package_ndc) AS affected_packages
FROM shortages_with_ndc
WHERE product_ndc IS NOT NULL
GROUP BY product_ndc, shortage_generic_name, company_name
HAVING COUNT(DISTINCT package_ndc) > 1
ORDER BY affected_packages DESC, company_name;

-- ============================================
-- ANALYSIS VIEW 3: Manufacturer Risk Assessment
-- Counts affected packages and products per manufacturer
-- ============================================

CREATE OR REPLACE VIEW manufacturer_risk_analysis AS
SELECT 
    company_name,
    COUNT(DISTINCT package_ndc) AS affected_packages,
    COUNT(DISTINCT product_ndc) AS affected_products,
    COUNT(DISTINCT CASE WHEN status = 'Current' THEN package_ndc END) AS current_shortage_packages
FROM shortages_with_ndc
WHERE company_name IS NOT NULL
GROUP BY company_name
ORDER BY affected_packages DESC;

-- ============================================
-- ANALYSIS VIEW 4: Current Manufacturer Risk
-- Focus only on currently active shortages by manufacturer
-- ============================================

CREATE OR REPLACE VIEW current_manufacturer_risk AS
SELECT 
    company_name,
    COUNT(DISTINCT package_ndc) AS current_affected_packages,
    COUNT(DISTINCT product_ndc) AS current_affected_products
FROM shortages_with_ndc
WHERE status = 'Current' 
    AND company_name IS NOT NULL
GROUP BY company_name
ORDER BY current_affected_packages DESC;

-- ============================================
-- Verification Queries
-- ============================================

-- Check the main enriched table
SELECT COUNT(*) AS total_shortage_records FROM shortages_with_ndc;

-- Check how many shortages successfully joined with NDC data
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN product_ndc IS NOT NULL THEN 1 ELSE 0 END) AS matched_records,
    SUM(CASE WHEN product_ndc IS NULL THEN 1 ELSE 0 END) AS unmatched_records,
    ROUND(SUM(CASE WHEN product_ndc IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS match_percentage
FROM shortages_with_ndc;

-- Show sample of enriched data
SELECT * FROM shortages_with_ndc LIMIT 5;

-- Show all created views
SHOW FULL TABLES WHERE table_type = 'VIEW';

-- Success message
SELECT 'SQL transformations completed successfully!' AS status;
