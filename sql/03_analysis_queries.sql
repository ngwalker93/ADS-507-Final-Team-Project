-- ============================================
-- Analysis Queries - Showcasing Join Value
-- These queries demonstrate insights ONLY possible by joining 
-- shortage data with NDC product information
-- ============================================

USE fda_shortage_db;

-- ============================================
-- QUERY 1: Top Manufacturers by Current Shortage Risk
-- Uses: product_ndc count (requires join to packaging and NDC)
-- Value: Shows manufacturers with most widespread shortage impact
-- ============================================

SELECT 
    company_name,
    current_affected_packages,
    current_affected_products
FROM current_manufacturer_risk
LIMIT 10;

-- ============================================
-- QUERY 2: Brand Name vs Generic Drug Shortages
-- Uses: brand_name from NDC (NOT in shortage data alone)
-- Value: Reveals if branded or generic drugs face more shortages
-- ============================================

SELECT 
    CASE 
        WHEN brand_name IS NOT NULL AND brand_name != '' THEN 'Branded Drug'
        ELSE 'Generic/Unbranded'
    END AS drug_type,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers_affected,
    ROUND(AVG(DATEDIFF(CURDATE(), STR_TO_DATE(initial_posting_date, '%Y%m%d'))), 0) AS avg_days_in_shortage
FROM shortages_with_ndc
WHERE status = 'Current'
    AND initial_posting_date IS NOT NULL
GROUP BY drug_type
ORDER BY shortage_count DESC;

-- ============================================
-- QUERY 3: Products with Multiple Package Shortages
-- Uses: product_ndc + package_ndc relationship (requires join)
-- Value: Identifies products with widespread packaging supply issues
-- ============================================

SELECT 
    generic_name,
    manufacturer,
    affected_packages,
    product_ndc
FROM multi_package_shortages
LIMIT 15;

-- ============================================
-- QUERY 4: Prescription vs OTC Drug Shortage Duration
-- Uses: product_type from NDC (NOT in shortage data)
-- Value: Shows if prescription drugs have longer shortage durations than OTC
-- ============================================

SELECT 
    product_type,
    COUNT(*) AS current_shortages,
    COUNT(DISTINCT company_name) AS manufacturers,
    ROUND(AVG(DATEDIFF(CURDATE(), STR_TO_DATE(initial_posting_date, '%Y%m%d'))), 0) AS avg_days_active,
    MAX(DATEDIFF(CURDATE(), STR_TO_DATE(initial_posting_date, '%Y%m%d'))) AS longest_active_days
FROM shortages_with_ndc
WHERE status = 'Current'
    AND product_type IS NOT NULL
    AND initial_posting_date IS NOT NULL
GROUP BY product_type
ORDER BY current_shortages DESC;

-- ============================================
-- QUERY 5: Package Size Distribution in Shortages
-- Uses: package_description from packaging table (NOT in shortage data)
-- Value: Reveals which package sizes are most vulnerable to shortages
-- ============================================

SELECT 
    CASE 
        WHEN package_description LIKE '%bottle%' THEN 'Bottle'
        WHEN package_description LIKE '%vial%' THEN 'Vial'
        WHEN package_description LIKE '%blister%' THEN 'Blister Pack'
        WHEN package_description LIKE '%carton%' THEN 'Carton'
        WHEN package_description LIKE '%kit%' THEN 'Kit'
        ELSE 'Other/Unknown'
    END AS package_type,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers
FROM shortages_with_ndc
WHERE status = 'Current'
    AND package_description IS NOT NULL
GROUP BY package_type
ORDER BY shortage_count DESC;

-- ============================================
-- QUERY 6: Route of Administration Shortage Analysis
-- Uses: route from NDC (NOT in shortage data)
-- Value: Shows which administration routes are most affected by shortages
-- ============================================

SELECT 
    CASE 
        WHEN route LIKE '%ORAL%' THEN 'Oral'
        WHEN route LIKE '%INTRAVENOUS%' OR route LIKE '%IV%' THEN 'Intravenous'
        WHEN route LIKE '%INJECTION%' THEN 'Injection'
        WHEN route LIKE '%TOPICAL%' THEN 'Topical'
        WHEN route LIKE '%INHALATION%' THEN 'Inhalation'
        ELSE 'Other'
    END AS administration_route,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT product_ndc) AS products_affected
FROM shortages_with_ndc
WHERE status = 'Current'
    AND route IS NOT NULL
GROUP BY administration_route
ORDER BY shortage_count DESC;

-- ============================================
-- QUERY 7: Match Rate Analysis
-- Uses: Join success metrics
-- Value: Shows data quality and join effectiveness
-- ============================================

SELECT 
    'Total Shortage Records' AS metric,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shortages_with_ndc), 2) AS percentage
FROM shortages_with_ndc

UNION ALL

SELECT 
    'Matched with NDC Data' AS metric,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shortages_with_ndc), 2) AS percentage
FROM shortages_with_ndc
WHERE product_ndc IS NOT NULL

UNION ALL

SELECT 
    'Unmatched (No NDC Found)' AS metric,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shortages_with_ndc), 2) AS percentage
FROM shortages_with_ndc
WHERE product_ndc IS NULL;

-- ============================================
-- QUERY 8: Marketing Category Impact on Shortages
-- Uses: marketing_category from NDC (NOT in shortage data)
-- Value: Shows if certain approval types (NDA, ANDA, OTC) face more shortages
-- ============================================

SELECT 
    marketing_category,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers,
    COUNT(DISTINCT product_ndc) AS products
FROM shortages_with_ndc
WHERE status = 'Current'
    AND marketing_category IS NOT NULL
GROUP BY marketing_category
ORDER BY shortage_count DESC;

-- ============================================
-- QUERY 9: Detailed Current Shortage List with Enriched Data
-- Uses: brand_name, labeler_name, package_description from join
-- Value: Comprehensive shortage report impossible without join
-- ============================================

SELECT 
    company_name AS manufacturer,
    shortage_generic_name AS generic_name,
    brand_name,
    manufacturer AS ndc_labeler,
    shortage_dosage_form AS dosage_form,
    route AS administration_route,
    package_description,
    product_type,
    initial_posting_date AS posted_date,
    DATEDIFF(CURDATE(), STR_TO_DATE(initial_posting_date, '%Y%m%d')) AS days_active
FROM shortages_with_ndc
WHERE status = 'Current'
    AND product_ndc IS NOT NULL
ORDER BY days_active DESC
LIMIT 50;

-- ============================================
-- QUERY 10: Manufacturer Portfolio Size vs Shortage Risk
-- Uses: product_ndc to count total NDC portfolio vs shortages
-- Value: Tests if larger manufacturers have proportionally more shortages
-- ============================================

SELECT 
    s.company_name,
    COUNT(DISTINCT s.product_ndc) AS products_with_shortages,
    COUNT(DISTINCT n.product_ndc) AS total_ndc_portfolio,
    ROUND(COUNT(DISTINCT s.product_ndc) * 100.0 / COUNT(DISTINCT n.product_ndc), 2) AS shortage_rate_percent
FROM shortages_with_ndc s
LEFT JOIN raw_ndc n ON s.manufacturer = n.labeler_name
WHERE s.status = 'Current'
    AND s.company_name IS NOT NULL
GROUP BY s.company_name
HAVING COUNT(DISTINCT n.product_ndc) >= 5  -- Only manufacturers with 5+ products
ORDER BY products_with_shortages DESC
LIMIT 20;

-- ============================================
-- Success Message
-- ============================================

SELECT 'Analysis queries ready - all showcase join value' AS status;
