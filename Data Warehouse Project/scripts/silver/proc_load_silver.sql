/*
===============================================================================
Stored Procedure: silver.load_silver (Bronze -> Silver)
===============================================================================
Description:
    This stored procedure runs the ETL logic that moves data from the 'bronze'
    schema into the 'silver' schema. For each target table it:
        - Truncates the corresponding silver table.
        - Inserts cleaned and transformed data sourced from bronze tables.

Parameters:
    None.
    The procedure does not take any input parameters and does not return a value.

How to run:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver ()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count  bigint;
    v_start_time timestamptz;
    v_end_time   timestamptz;
    v_duration   interval;
BEGIN

    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: silver.crm_cust_info';

    BEGIN
        TRUNCATE TABLE silver.crm_cust_info;

        RAISE NOTICE 'INSERTING DATA INTO: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id, 
            cst_key, 
            TRIM(cst_firstname) AS cst_firstname, 
            TRIM(cst_lastname)  AS cst_lastname,  
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id 
                       ORDER BY cst_create_date DESC
                   ) AS flag_last 
            FROM bronze.crm_cust_info 
            WHERE cst_id IS NOT NULL
        ) t
        WHERE t.flag_last = 1;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO silver.crm_cust_info IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading silver.crm_cust_info after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: silver.crm_prd_info';

    BEGIN
        TRUNCATE TABLE silver.crm_prd_info;

        RAISE NOTICE 'INSERTING DATA INTO: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key))      AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0)                       AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END                                         AS prd_line,
            CAST (prd_start_dt AS DATE)                 AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key 
                    ORDER BY prd_start_dt
                ) - INTERVAL '1 day'
                AS DATE
            )                                           AS prd_end_dt
        FROM bronze.crm_prd_info;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO silver.crm_prd_info IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading silver.crm_prd_info after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: silver.crm_sales_details';

    BEGIN
        TRUNCATE TABLE silver.crm_sales_details;

        RAISE NOTICE 'INSERTING DATA INTO: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL 
                  OR sls_sales <= 0 
                  OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO silver.crm_sales_details IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading silver.crm_sales_details after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: silver.erp_cust_az12';

    BEGIN
        TRUNCATE TABLE silver.erp_cust_az12;

        RAISE NOTICE 'INSERTING DATA INTO: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO silver.erp_cust_az12 IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading silver.erp_cust_az12 after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;
	

    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: silver.erp_loc_a101';

    BEGIN
        TRUNCATE TABLE silver.erp_loc_a101;

        RAISE NOTICE 'INSERTING DATA INTO: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN TRIM(cntry) = 'DE'                THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA')      THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO silver.erp_loc_a101 IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading silver.erp_loc_a101 after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: silver.erp_px_cat_g1v2';

    BEGIN
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        RAISE NOTICE 'INSERTING DATA INTO: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO silver.erp_px_cat_g1v2 IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading silver.erp_px_cat_g1v2 after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;
END;
$$;
