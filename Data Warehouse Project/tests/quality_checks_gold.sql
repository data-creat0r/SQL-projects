/*
===============================================================================
Quality Checks
===============================================================================
Description:
    This script runs a set of data quality checks to verify the integrity,
    consistency, and correctness of the Gold layer. The checks are designed
    to ensure:
      - Surrogate keys in dimension tables are unique.
      - Referential integrity holds between fact and dimension tables.
      - Relationships in the data model behave as expected for analytics.

Usage Notes:
    - Any rows returned by these checks indicate potential data issues that
      should be investigated and resolved.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Verify that the customer key in gold.dim_customers is unique
-- Expected: query should return no rows

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for uniqueness of product key in gold.dim_products
-- Expectation: no results

SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
