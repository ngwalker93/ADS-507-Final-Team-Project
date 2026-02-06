-- ============================================
-- FDA Drug Shortage Analysis Database
-- Creates database structure for NDC and drug shortage data
-- ============================================

-- Create database
CREATE DATABASE IF NOT EXISTS fda_shortage_db;
USE fda_shortage_db;

-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS shortage_contacts;
DROP TABLE IF EXISTS shortages_with_ndc;
DROP TABLE IF EXISTS raw_drug_shortages;
DROP TABLE IF EXISTS raw_ndc_packaging;
DROP TABLE IF EXISTS raw_ndc;

-- ============================================
-- Table 1: Raw NDC Product Data
-- Core drug product information from FDA NDC database
-- ============================================
CREATE TABLE raw_ndc (
    product_ndc VARCHAR(20) PRIMARY KEY,
    generic_name TEXT,
    labeler_name TEXT,
    brand_name TEXT,
    finished BOOLEAN,
    marketing_category VARCHAR(100),
    dosage_form VARCHAR(100),
    route TEXT,
    product_type VARCHAR(100),
    marketing_start_date VARCHAR(20),
    application_number VARCHAR(50),
    INDEX idx_labeler (labeler_name(255)),
    INDEX idx_brand (brand_name(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- Table 2: NDC Packaging Information
-- One-to-many relationship: one product can have multiple packages
-- ============================================
CREATE TABLE raw_ndc_packaging (
    package_ndc VARCHAR(30) PRIMARY KEY,
    product_ndc VARCHAR(20),
    description TEXT,
    marketing_start_date VARCHAR(20),
    FOREIGN KEY (product_ndc) REFERENCES raw_ndc(product_ndc)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    INDEX idx_product_ndc (product_ndc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- Table 3: Drug Shortages Data
-- FDA drug shortage information
-- AUTO_INCREMENT primary key for shortage_id
-- ============================================
CREATE TABLE raw_drug_shortages (
    shortage_id INT AUTO_INCREMENT PRIMARY KEY,
    package_ndc VARCHAR(30),
    generic_name TEXT,
    company_name VARCHAR(255),
    status VARCHAR(50),
    therapeutic_category TEXT,
    initial_posting_date VARCHAR(20),
    update_date VARCHAR(20),
    dosage_form TEXT,
    reason TEXT,
    INDEX idx_package_ndc (package_ndc),
    INDEX idx_status (status(50)),
    INDEX idx_company (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- Table 4: Shortage Contact Information
-- Normalized contact info separated from main shortage data
-- ============================================
CREATE TABLE shortage_contacts (
    contact_id INT AUTO_INCREMENT PRIMARY KEY,
    package_ndc VARCHAR(30),
    contact_info TEXT,
    INDEX idx_package_ndc (package_ndc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- Verification Queries
-- ============================================

-- Show all tables
SHOW TABLES;

-- Show structure of each table
DESCRIBE raw_ndc;
DESCRIBE raw_ndc_packaging;
DESCRIBE raw_drug_shortages;
DESCRIBE shortage_contacts;

-- Success message
SELECT 'Database schema created successfully!' AS status;
