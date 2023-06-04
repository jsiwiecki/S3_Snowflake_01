// CREATION OF DIMENSIONAL TABLES

-- DIM_CUSTOMER
CREATE OR REPLACE TABLE DATA_MART.DIM_CUSTOMER LIKE CORE_DWH.CUSTOMER;

-- DIM_NATION
CREATE TABLE DATA_MART.DIM_NATION LIKE CORE_DWH.NATION;

-- DIM_PART
CREATE TABLE DATA_MART.DIM_PART LIKE CORE_DWH.PART;

-- DIM_PARTSUPP
CREATE TABLE DATA_MART.DIM_PARTSUPP LIKE CORE_DWH.PARTSUPP;

-- DIM_REGION
CREATE TABLE DATA_MART.DIM_REGION LIKE CORE_DWH.REGION;

-- DIM_SUPPLIER
CREATE TABLE DATA_MART.DIM_SUPPLIER LIKE CORE_DWH.SUPPLIER;


// CREATION OF FACT TABLES


CREATE OR REPLACE TABLE DATA_MART.FACT_ORDERS (
    O_ORDERKEY       NUMBER,
    O_CUSTKEY        NUMBER,
    C_NATIONKEY      NUMBER,
    O_ORDERSTATUS    STRING,
    O_TOTALPRICE     FLOAT,
    O_ORDERDATE      DATE,
    O_ORDERPRIORITY  STRING,
    O_CLERK          STRING,
    O_SHIPPRIORITY   NUMBER,
    O_COMMENT        STRING,
    L_DISCOUNT       FLOAT
    );

create table DATA_MART.FACT_LINEITEM
(
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice FLOAT8 not null,
  l_discount      FLOAT8 not null,
  l_tax           FLOAT8 not null,
  l_returnflag    CHAR(1),
  l_linestatus    CHAR(1),
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_comment       VARCHAR(44)
);