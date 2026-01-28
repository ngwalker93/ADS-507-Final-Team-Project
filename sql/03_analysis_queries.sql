-- ============================================
-- Analysis Queries
-- Demonstrates the value of the enriched shortage + NDC dataset
-- These queries answer practical business questions
-- ============================================

USE fda_shortage_db;

-- ============================================
-- QUERY 1: Top Manufacturers by Current Shortage Risk
-- Identifies manufacturers most affected by active shortages
-- ============================================

SELECT 
    company_name,
    current_affected_packages,
    current_affected_products
FROM current_manufacturer_risk
LIMIT 10;

-- ============================================
-- QUERY 2: Shortage Status Breakdown
-- Overall summary of shortage statuses
-- ============================================

SELECT 
    status,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS affected_manufacturers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM shortages_with_ndc
WHERE status IS NOT NULL
GROUP BY status
ORDER BY shortage_count DESC;

-- ============================================
-- QUERY 3: Products with Multiple Package Shortages
-- Shows which products have the most widespread packaging issues
-- ============================================

SELECT 
    generic_name,
    manufacturer,
    affected_packages,
    product_ndc
FROM multi_package_shortages
LIMIT 15;

-- ============================================
-- QUERY 4: Shortage Trends by Therapeutic Category
-- Identifies which drug categories are most affected
-- ============================================

SELECT 
    therapeutic_category,
    COUNT(*) AS total_shortages,
    COUNT(DISTINCT company_name) AS manufacturers_affected,
    COUNT(DISTINCT product_ndc) AS products_affected
FROM shortages_with_ndc
WHERE therapeutic_category IS NOT NULL
GROUP BY therapeutic_category
ORDER BY total_shortages DESC
LIMIT 10;

-- ============================================
-- QUERY 5: Current Shortages by Dosage Form
-- Shows which drug formulations are most commonly in shortage
-- ============================================

SELECT 
    shortage_dosage_form AS dosage_form,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers
FROM shortages_with_ndc
WHERE status = 'Current' 
    AND shortage_dosage_form IS NOT NULL
GROUP BY shortage_dosage_form
ORDER BY shortage_count DESC
LIMIT 10;

-- ============================================
-- QUERY 6: Manufacturers with Longest Active Shortages
-- Identifies which manufacturers have oldest unresolved shortages
-- Uses initial_posting_date to calculate duration
-- ============================================

SELECT 
    company_name,
    shortage_generic_name AS generic_name,
    status,
    initial_posting_date,
    update_date,
    DATEDIFF(CURDATE(), STR_TO_DATE(initial_posting_date, '%Y%m%d')) AS days_since_initial_posting
FROM shortages_with_ndc
WHERE status = 'Current'
    AND initial_posting_date IS NOT NULL
    AND company_name IS NOT NULL
ORDER BY days_since_initial_posting DESC
LIMIT 20;

-- ============================================
-- QUERY 7: Match Rate Analysis
-- Shows how well shortage data joins with NDC database
-- ============================================

SELECT 
    'Total Shortage Records' AS metric,
    COUNT(*) AS count
FROM shortages_with_ndc

UNION ALL

SELECT 
    'Matched with NDC Data' AS metric,
    COUNT(*) AS count
FROM shortages_with_ndc
WHERE product_ndc IS NOT NULL

UNION ALL

SELECT 
    'Unmatched (No NDC Found)' AS metric,
    COUNT(*) AS count
FROM shortages_with_ndc
WHERE product_ndc IS NULL;

-- ============================================
-- QUERY 8: Current Shortages by Product Type
-- Classifies current shortages by prescription vs OTC
-- ============================================

SELECT 
    product_type,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers,
    COUNT(DISTINCT product_ndc) AS products
FROM shortages_with_ndc
WHERE status = 'Current'
    AND product_type IS NOT NULL
GROUP BY product_type
ORDER BY shortage_count DESC;

-- ============================================
-- QUERY 9: Detailed Current Shortage List
-- Comprehensive view of all active shortages with key details
-- ============================================

SELECT 
    company_name AS manufacturer,
    shortage_generic_name AS drug_name,
    brand_name,
    shortage_dosage_form AS dosage_form,
    package_description,
    therapeutic_category,
    initial_posting_date AS posted_date,
    reason AS shortage_reason
FROM shortages_with_ndc
WHERE status = 'Current'
ORDER BY company_name, shortage_generic_name
LIMIT 50;

-- ============================================
-- QUERY 10: Shortage Reasons Analysis
-- Identifies most common reasons for drug shortages
-- ============================================

SELECT 
    CASE 
        WHEN reason LIKE '%manufacturing%' THEN 'Manufacturing Issues'
        WHEN reason LIKE '%demand%' THEN 'Demand Increase'
        WHEN reason LIKE '%delay%' THEN 'Delay'
        WHEN reason LIKE '%discontinu%' THEN 'Discontinuation'
        WHEN reason IS NULL THEN 'Not Specified'
        ELSE 'Other'
    END AS reason_category,
    COUNT(*) AS shortage_count
FROM shortages_with_ndc
WHERE status = 'Current'
GROUP BY reason_category
ORDER BY shortage_count DESC;

-- ============================================
-- Success Message
-- ============================================

SELECT 'Analysis queries ready to run!' AS status;
