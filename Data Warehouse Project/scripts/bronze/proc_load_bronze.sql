/*
===============================================================================
Stored Procedure: bronze.load_bronze (Source CSV -> Bronze Layer)
===============================================================================
Description:
    This stored procedure loads data into tables in the 'bronze' schema from
    external CSV files. For each source table it:
      - Truncates the target bronze table.
      - Reloads the data from the corresponding CSV file using the PostgreSQL
        COPY command.

File paths:
    The COPY statements use a placeholder base path like:
        'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\...'
    Before running this procedure, replace `YOUR_LOCAL_PATH` with the actual
    location of the project on your machine.

Parameters:
    None.
    The procedure does not accept input arguments and does not return a value.

How to run:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze ()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time timestamptz;
    v_end_time   timestamptz;
    v_duration   interval;
    v_row_count  bigint;
BEGIN
    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: bronze.crm_cust_info';

    BEGIN
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE 'LOADING DATA INTO: bronze.crm_cust_info';
        COPY bronze.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        FROM 'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'   
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER true
        );

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO bronze.crm_cust_info IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading bronze.crm_cust_info after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: bronze.crm_prd_info';

    BEGIN
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE 'LOADING DATA INTO: bronze.crm_prd_info';
        COPY bronze.crm_prd_info (
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        FROM 'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'   
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER true
        );

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO bronze.crm_prd_info IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading bronze.crm_prd_info after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: bronze.crm_sales_details';

    BEGIN
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE 'LOADING DATA INTO: bronze.crm_sales_details';
        COPY bronze.crm_sales_details (
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
        FROM 'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'   
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER true
        );

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO bronze.crm_sales_details IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading bronze.crm_sales_details after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: bronze.erp_cust_az12';

    BEGIN
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE 'LOADING DATA INTO: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12 (
            CID,
            BDATE,
            GEN
        )
        FROM 'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'   
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER true
        );

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO bronze.erp_cust_az12 IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading bronze.erp_cust_az12 after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: bronze.erp_loc_a101';

    BEGIN
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE 'LOADING DATA INTO: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101 (
            CID,
            CNTRY
        )
        FROM 'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'   
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER true
        );

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO bronze.erp_loc_a101 IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading bronze.erp_loc_a101 after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;


    RAISE NOTICE '==================================================';
    v_start_time := clock_timestamp();
    RAISE NOTICE 'TRUNCATING TABLE: bronze.erp_px_cat_g1v2';

    BEGIN
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE 'LOADING DATA INTO: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2 (
            ID,
            CAT,
            SUBCAT,
            MAINTENANCE
        )
        FROM 'C:\YOUR_LOCAL_PATH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'   
        WITH (
            FORMAT csv,
            DELIMITER ',',
            HEADER true
        );

        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;

        RAISE NOTICE 'INSERTED % ROWS INTO bronze.erp_px_cat_g1v2 IN %', v_row_count, v_duration;
    EXCEPTION
        WHEN others THEN
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;

            RAISE WARNING 'Error loading bronze.erp_px_cat_g1v2 after %: % (SQLSTATE: %)',
                v_duration, SQLERRM, SQLSTATE;
    END;
END;
$$;
