-----------------CUST INFO--------------CUST INFO ---------------------CUST INFO


-------CHECKING NULL VALUES

SELECT cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)> 1 OR cst_id IS NULL


-------------Checking DUPLICATE Values


SELECT * FROM(
SELECT 
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) WHERE flag_last =1 AND cst_id = 29446

--------------CHECK FOR UNWANTED SPACES

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


---Data Standarization and consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info


---------PRD INFO--------------------PRD INFO---------------PRD INFO---------PRD INFO-----------PRD INFO-----------------

-----Check For unwanted spaces------------
SELECT 
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-----------Check for NULL or 0 values 

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

----------Checking for dates ship and due dt

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info


SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
	FROM bronze.crm_prd_info
	WHERE prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509')



-------SALES DETAILS-----------------SALES DETAILS---------------------SALES DETAILS


----------Chek for invaalid dates


SELECT 
NULLIF (sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0 
OR LENGTH(sls_order_dt::Text) !=8 
OR sls_order_dt >20500101
OR sls_order_dt< 19000101


-----------Check for NULL, invalid price,quantity and cost--------------

SELECT DISTINCT
	sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0  OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
		CASE WHEN sls_price IS NULL OR sls_price <=0
			THEN sls_sales/NULLIF (sls_quantity,0)
			ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales!=sls_quantity * sls_price

OR sls_sales IS NULL or sls_price IS NULL or sls_quantity IS NULL
OR sls_sales <=0 or sls_price <=0  or sls_quantity <=0
ORDER BY sls_sales,sls_quantity, sls_price



-------ERP CUST AZ12--------------ERP CUST AZ12-------------ERP CUST AZ12--------------------ERP CUST AZ12


-----Checking the primary key

SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	ELSE  cid
	END  AS cid,
	CASE WHEN bdate>NOW() THEN NULL
	ELSE bdate
END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN('F','FEMALE') THEN 'Female'
	      WHEN UPPER(TRIM(gen)) IN('M','MALE') THEN 'Male'
	ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12


-----------Checking for genders 


SELeCT  DISTINCT 
gen FROM 
bronze.erp_cust_az12
