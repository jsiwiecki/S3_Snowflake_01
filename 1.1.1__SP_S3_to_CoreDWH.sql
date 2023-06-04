//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////   STORED PROCEDURES TO MOVE DATA FROM S3 TO CORE_DWH TABLES
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////
/////   S3 -> CORE_DWH.CUSTOMER


CREATE OR REPLACE PROCEDURE load_data_to_customer()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) " +
                          "SELECT $1, REPLACE($2, 'Customer#', '') as C_NAME, $3, $4, $5, TO_NUMBER(REPLACE($6, ',', '.')), $7," +
                          "    TRIM(" +
                          "        REGEXP_REPLACE($8, '( --| \\\\.)$', '')" +
                          "    ) as C_COMMENT " +
                          "FROM @snowflake_lab_aws/h_customer.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion)
    return result.next() ? 'Data ingested successfully to CUSTOMER table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
/////   S3 -> CORE_DWH.LINEITEM


CREATE OR REPLACE PROCEDURE load_data_to_lineitem()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) " +
                          "SELECT $1, $2, $3, $4, $5, TO_NUMBER(REPLACE($6, ',', '.')), TO_NUMBER(REPLACE($7, ',', '.')), TO_NUMBER(REPLACE($8, ',', '.')), $9, $10, $11, $12, $13, $14, $15," +
                          "    TRIM(" +
                          "        REGEXP_REPLACE($16, '( --| \\\\.)$', '')" +
                          "    ) as L_COMMENT " +
                          "FROM @snowflake_lab_aws/h_lineitem.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to LINEITEM table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
/////   S3 -> CORE_DWH.NATION


CREATE OR REPLACE PROCEDURE load_data_to_nation()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.NATION (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) " +
                          "SELECT $1, $2, $3, " +
                          "    TRIM(" +
                          "        REGEXP_REPLACE($4, ' \\\\.$', '')" +
                          "    ) as N_COMMENT " +
                          "FROM @snowflake_lab_aws/h_nation.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to NATION table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    S3 -> CORE_DWH.ORDERS


CREATE OR REPLACE PROCEDURE load_data_to_orders()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) " +
                          "SELECT $1, $2, $3, TO_NUMBER(REPLACE($4, ',', '.')), $5, $6, REPLACE($7, 'Clerk#', '') as O_CLERK, $8, " +
                          "    TRIM(" +
                          "        REGEXP_REPLACE($9, ' \\\\.$', '')" +
                          "    ) as O_COMMENT " +
                          "FROM @snowflake_lab_aws/h_order.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to ORDERS table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    S3 -> CORE_DWH.PART


CREATE OR REPLACE PROCEDURE load_data_to_part()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) " +
                          "SELECT $1, $2, REPLACE($3, 'Manufacturer#', '') AS P_MFGR, REPLACE($4, 'Brand#', '') as P_BRAND, $5, $6, $7, $8, $9 " +
                          "FROM @snowflake_lab_aws/h_part.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to PART table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    S3 -> CORE_DWH.PARTSUPP


CREATE OR REPLACE PROCEDURE load_data_to_partsupp()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.PARTSUPP (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) " +
                          "SELECT $1, $2, $3, TO_NUMBER(REPLACE($4, ',', '.')), " +
                          "TRIM(" +
                          "    REGEXP_REPLACE($5, ' \\\\.$', '')" +
                          ") " +
                          "FROM @snowflake_lab_aws/h_partsupp.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to PARTSUPP table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    S3 -> CORE_DWH.REGION


CREATE OR REPLACE PROCEDURE load_data_to_region()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.REGION (R_REGIONKEY, R_NAME, R_COMMENT) " +
                          "SELECT $1, $2, " +
                          "    TRIM(" +
                          "        REGEXP_REPLACE($3, ' \\\\.$', '')" +
                          "    ) as R_COMMENT " +
                          "FROM @snowflake_lab_aws/h_region.csv " +
                          "(file_format => csv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to REGION table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
/// S3 -> CORE_DWH.SUPPLIER


CREATE OR REPLACE PROCEDURE load_data_to_supplier()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO CORE_DWH.SUPPLIER (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) " +
                          "SELECT $1, REPLACE($2, 'Supplier#', '') AS S_NAME, $3, $4, $5, TO_NUMBER(REPLACE($6, ',', '.')), " +
                          "    TRIM(" +
                          "        REGEXP_REPLACE($7, ' \\\\.$', '')" +
                          "    ) as S_COMMENT " +
                          "FROM @snowflake_lab_aws/h_supplier.dsv " +
                          "(file_format => dsv_format)";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to SUPPLIER table.' : 'Ingestion failed.';
$$;