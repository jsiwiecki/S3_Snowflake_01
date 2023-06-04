# General descrition of this project
This repository consist of a results of ETL project using S3 and Snowflake.

## Goal of this repository:
- Practice ETL process using S3 & Snowflake
- Practice integration between S3 and Snowflake
- Explore Snowflake's features like: Tasks, Stored Procedures etc. 



### Setup AWS
1.  Creation of S3 to put there files
![Image](images/image01.png)

2.  Creation of policies to integrate S3 with Snowflake
![Image](images/image02.png)

3.  Creation of role for Snowflake
![Image](images/image03.png)

4.  Assigning created policy to the role
![Image](images/image04.png)

### Setup Snowflake & AWS integration
1.  Creating Snowflake & AWS Storage integration
![Image](images/image05.png)

2.  Getting Snowflake credentials for integration
![Image](images/image06.png)

3.  AWS trust policy adjusted to Snowflake credentials
![Image](images/image07.png)

### Placing source files to S3
1.  Move files to S3
![Image](images/image08.png)

2.  Creation of CORE_DWH schema and tables within
1.1.0__Tables_creation.sql


3.  Create stage for S3
![Image](images/image09.png)

4.  Check content of the stage
![Image](images/image10.png)

5.  Creation of Snowflake file format
![Image](images/image11.png)

6.  Peaking the data from stage
![Image](images/image12.png)


### Preparation for ETL process

1.  Prepare query to ingest data
![Image](images/image13.png)

2.  Prepare query that cleans data
![Image](images/image13.png)

2.  Prepare Stage Procedures to ingest data from Stage to CORE_DWH schema from each file to according table
`1.1.1__SP_S3_to_CoreDWH.sql`
![Image](images/image15.png)

