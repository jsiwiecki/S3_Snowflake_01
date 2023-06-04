//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////   STORED PROCEDURES TO MOVE FROM CORE_DWH TO DATA_MART WITH STAR SCHEMA TABLES
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////


//////////////////////////////////////
/////    CORE_DWH.CUSTOMER -> DATA_MART.DIM_CUSTOMER

CREATE OR REPLACE PROCEDURE load_data_to_DIM_CUSTOMER()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.DIM_CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) " +
                          "SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT " +
                          "FROM CORE_DWH.CUSTOMER ";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to DIM_CUSTOMER table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
/////  CORE_DWH.LINEITEM -> DATA_MART.FACT_LINEITEM

CREATE OR REPLACE PROCEDURE load_data_to_FACT_LINEITEM()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.FACT_LINEITEM (L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) " +
                          "SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT " +
                          "FROM CORE_DWH.LINEITEM ";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to FACT_LINEITEM table.' : 'Ingestion failed.';
$$;



//////////////////////////////////////
/////   CORE_DWH.NATION -> DATA_MART.DIM_NATION


CREATE OR REPLACE PROCEDURE load_data_to_DIM_NATION()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.DIM_NATION (N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT) " +
                          "SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT " +
                          "FROM CORE_DWH.NATION";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to DIM_NATION table.' : 'Ingestion failed.';
$$;



//////////////////////////////////////
////    CORE_DWH.ORDERS & CORE_DWH.CUSTOMER & CORE_DWH.LINEITEM -> DATA_MART.FACT_ORDERS

CREATE OR REPLACE PROCEDURE load_data_to_FACT_ORDERS()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the copy data query with join operation
    var copy_data_query = "INSERT INTO DATA_MART.FACT_ORDERS (O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, C_NATIONKEY, L_DISCOUNT) " +
                          "SELECT ORDERS.O_ORDERKEY, ORDERS.O_CUSTKEY, ORDERS.O_ORDERSTATUS, ORDERS.O_TOTALPRICE, ORDERS.O_ORDERDATE, ORDERS.O_ORDERPRIORITY, ORDERS.O_CLERK, ORDERS.O_SHIPPRIORITY, ORDERS.O_COMMENT, CUSTOMER.C_NATIONKEY, LINEITEM.L_DISCOUNT " + 
                          "FROM CORE_DWH.ORDERS " +
                          "JOIN CORE_DWH.CUSTOMER ON ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY " +
                          "JOIN CORE_DWH.LINEITEM ON ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY;";
                          
    var stmt = snowflake.createStatement({sqlText: copy_data_query});
    var result = stmt.execute();

    // Return the status of the copy operation
    return result.next() ? 'Data copied successfully from ORDERS to FACT_ORDERS table.' : 'Copy failed.';
$$;


//////////////////////////////////////
////    CORE_DWH.PART -> DATA_MART.DIM_PART

CREATE OR REPLACE PROCEDURE load_data_to_DIM_PART()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.DIM_PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT) " +
                          "SELECT P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT "+
                          "FROM CORE_DWH.PART";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to DIM_PART table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    CORE_DWH.PARTSUPP -> DATA_MART.DIM_PARTSUPP


CREATE OR REPLACE PROCEDURE load_data_to_DIM_PARTSUPP()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.DIM_PARTSUPP (PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) " +
                          "SELECT  PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT " +
                          "FROM CORE_DWH.PARTSUPP ";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to DIM_PARTSUPP table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    DATA_MART.DIM_REGION -> CORE_DWH.REGION

CREATE OR REPLACE PROCEDURE load_data_to_DIM_REGION()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.DIM_REGION (R_REGIONKEY, R_NAME, R_COMMENT) " +
                          "SELECT R_REGIONKEY, R_NAME, R_COMMENT " +
                          "FROM CORE_DWH.REGION";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to DIM_REGION table.' : 'Ingestion failed.';
$$;


//////////////////////////////////////
////    CORE_DWH.SUPPLIER -> DATA_MART.DIM_SUPPLIER


CREATE OR REPLACE PROCEDURE load_data_to_DIM_SUPPLIER()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    AS
$$
    // Execute the ingestion query
    var ingestion_query = "INSERT INTO DATA_MART.DIM_SUPPLIER (S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT) " +
                          "SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT " +
                          "FROM CORE_DWH.SUPPLIER";
                          
    var stmt = snowflake.createStatement({sqlText: ingestion_query});
    var result = stmt.execute();

    // Return the status of the ingestion
    return result.next() ? 'Data ingested successfully to DIM_SUPPLIER table.' : 'Ingestion failed.';
$$;
