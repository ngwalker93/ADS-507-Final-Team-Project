-- ============================================
-- FDA Drug Shortage Analysis Database
-- Creates database structure for NDC and drug shortage data
-- ============================================



-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS shortage_contacts;
DROP TABLE IF EXISTS shortages_with_ndc;
DROP TABLE IF EXISTS raw_drug_shortages;
DROP TABLE IF EXISTS raw_ndc_packaging;
DROP TABLE IF EXISTS raw_ndc;
DROP TABLE IF EXISTS raw_ndc_active_ingredients;

-- ============================================
-- Table 1: Raw NDC Product Data
-- Core drug product information from FDA NDC database
-- ============================================
CREATE TABLE raw_ndc (
    product_ndc VARCHAR(20) PRIMARY KEY,
    generic_name TEXT,
    labeler_name TEXT,
    brand_name TEXT,
    finished TINYINT(1),
    marketing_category VARCHAR(100),
    dosage_form TEXT,
    route TEXT,
    product_type VARCHAR(150),
    marketing_start_date VARCHAR(20),
    marketing_end_date VARCHAR(20),
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
    marketing_end_date VARCHAR(20),
    sample TINYINT(1),
    FOREIGN KEY (product_ndc) REFERENCES raw_ndc(product_ndc)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    INDEX idx_product_ndc (product_ndc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- Table 3: Drug Shortages Data
-- FDA drug shortage information
-- Composite primary key: package_ndc + initial_posting_date
-- ============================================
CREATE TABLE raw_drug_shortages (
    shortage_id INT AUTO_INCREMENT PRIMARY KEY,
    package_ndc VARCHAR(30),
    generic_name TEXT,
    company_name TEXT,
    status VARCHAR(50),
    therapeutic_category TEXT,
    initial_posting_date VARCHAR(20),
    update_date VARCHAR(20),
    dosage_form TEXT,
    reason TEXT,
    INDEX idx_package_ndc (package_ndc),
    INDEX idx_status (status),
    INDEX idx_company (company_name(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================
-- Table 4: Shortage Contact Information
-- Normalized contact info separated from main shortage data
-- ============================================
CREATE TABLE shortage_contacts (
    contact_id INT AUTO_INCREMENT PRIMARY KEY,
    shortage_id INT NOT NULL,
    contact_info TEXT,
    FOREIGN KEY (shortage_id) REFERENCES raw_drug_shortages(shortage_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    INDEX idx_shortage_id (shortage_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- Table 5: Active Ingredients (Optional Enhancement)
-- Normalized active ingredient info separated from main shortage data
-- ============================================

CREATE TABLE raw_ndc_active_ingredients (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_ndc VARCHAR(20),
    name TEXT,
    strength VARCHAR(255),
    FOREIGN KEY (product_ndc) REFERENCES raw_ndc(product_ndc)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    INDEX idx_product_ndc (product_ndc)
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
