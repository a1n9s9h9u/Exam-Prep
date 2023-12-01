Which of the following commands can a data engineer use to compact small data files of a Delta table into larger ones ? 

OPTIMIZE 

A data engineer is trying to use Delta time travel to rollback a table to a previous version, but the data engineer received an error that the data files are no longer present. 

Which of the following commands was run on the table that caused deleting the data files? 
 
VACUUM 

In Delta Lake tables, which of the following is the primary format for the data files? 

Parquet 

Which of the following locations hosts the Databricks web application ? 

control plane 

In Databricks Repos, which of the following operations a data engineer can use to update the local version of a repo from its remote Git repository ? 

Pull 

According to the Databricks Lakehouse architecture, which of the following is located in the customer's cloud account? 

cluster virtual machines 

Which of the following best describes Databricks Lakehouse? 

Single, flexible, high-performance system that supports data, analytics, ….... 

If the default notebook language is SQL, which of the following options a data engineer can use to run a Python code in this SQL Notebook ? 

Add %python at the start of a cell 

Which of the following tasks is not supported by Databricks Repos, and must be performed in your Git provider ? 

The following tasks are not supported by Databricks Repos, and must be performed in your Git provider: 

Create a pull request 

Delete branches 

Which of the following statements is Not true about Delta Lake ? 

It is not true that Delta Lake builds upon XML format. It builds upon Parquet and JSON formats 

How long is the default retention period of the VACUUM command ? 

7 days 

The data engineering team has a Delta table called employees that contains the employees personal information including their gross salaries. 

Which of the following code blocks will keep in the table only the employees having a salary greater than 3000 ? 

In order to keep only the employees having a salary greater than 3000, we must delete the employees having salary less than or equal 3000. To do so, use the DELETE statement: 

DELETE FROM table_name WHERE condition; 

A data engineer wants to create a relational object by pulling data from two tables. The relational object must be used by other data engineers in other sessions on the same cluster only. In order to save on storage costs, the date engineer wants to avoid copying and storing physical data. 

Which of the following relational objects should the data engineer create? 

Global Temporary view  

A data engineer has developed a code block to completely reprocess data based on the following if-condition in Python: 

if process_mode = "init" and not is_table_exist: 

  print("Start processing ...") 

This if-condition is returning an invalid syntax error. 

Which of the following changes should be made to the code block to fix this error ? 

If process_mode == “init” and not is_table_exist: (Double equal to) 

Fill in the below blank to successfully create a table in Databricks using data from an existing PostgreSQL database: 

CREATE TABLE employees 

 USING ____________ 

 OPTIONS ( 

   url "jdbc:postgresql:dbserver", 

   dbtable "employees" 

 ) 

Org.apache.spark.sql.jdbc 

Using the JDBC library, Spark SQL can extract data from any existing relational database that supports JDBC. Examples include mysql, postgres, SQLite, and more. 

Which of the following commands can a data engineer use to create a new table along with a comment ? 

Syntax: 

CREATE TABLE table_name 

COMMENT "here is a comment" 

AS query 

A junior data engineer usually uses INSERT INTO command to write data into a Delta table. A senior data engineer suggested using another command that avoids writing of duplicate records. 

Which of the following commands is the one suggested by the senior data engineer ? 

MERGE INTO allows to merge a set of updates, insertions, and deletions based on a source table into a target Delta table. With MERGE INTO, you can avoid inserting the duplicate records when writing into Delta tables. 

A data engineer is designing a Delta Live Tables pipeline. The source system generates files containing changes captured in the source data. Each change event has metadata indicating whether the specified record was inserted, updated, or deleted. In addition to a timestamp column indicating the order in which the changes happened. The data engineer needs to update a target table based on these change events. 

Which of the following commands can the data engineer use to best solve this problem? 

APPLY CHANGES INTO  

In PySpark, which of the following commands can you use to query the Delta table employees created in Spark SQL? 

spark.table() function returns the specified Spark SQL table as a PySpark DataFrame 

Which of the following code blocks can a data engineer use to create a user defined function (UDF) ? 

The correct syntax to create a UDF is: 

CREATE [OR REPLACE] FUNCTION function_name ( [ parameter_name data_type [, ...] ] ) 

RETURNS data_type 

RETURN { expression | query } 

When dropping a Delta table, which of the following explains why only the table's metadata will be deleted, while the data files will be kept in the storage ? 

The table is external 

External (unmanaged) tables are tables whose data is stored in an external storage path by using a LOCATION clause. 

When you run DROP TABLE on an external table, only the table's metadata is deleted, while the underlying data files are kept. 

Given the two tables students_course_1 and students_course_2. Which of the following commands can a data engineer use to get all the students from the above two tables without duplicate records ? 

With UNION, you can return the result of subquery1 plus the rows of subquery2 

Syntax: 

subquery1 

UNION [ ALL | DISTINCT ] 

subquery2 

If ALL is specified duplicate rows are preserved. 

If DISTINCT is specified the result does not contain any duplicate rows. This is the default. 

Given the following command: 

CREATE DATABASE IF NOT EXISTS hr_db ; 

In which of the following locations will the hr_db database be located? 

Since we are creating the database here without specifying a LOCATION clause, the database will be created in the default warehouse directory under dbfs:/user/hive/warehouse 

 

Fill in the below blank to get the students enrolled in less than 3 courses from array column students 

 
 

SELECT 

 faculty_id, 

 students, 

 ___________ AS few_courses_students 

FROM faculties 

 

FILTER(students, I -> I.total_courses <3) 

 

Given the following Structured Streaming query: 

 
 

(spark.table("orders") 

       .withColumn("total_after_tax", col("total")+col("tax")) 

   .writeStream 

       .option("checkpointLocation", checkpointPath) 

       .outputMode("append") 

        .______________  

       .table("new_orders") 

) 

 
 

Fill in the blank to make the query executes a micro-batch to process data every 2 minutes 

 

Trigger(processingTime=“2 minutes”) 

 

Which of the following is used by Auto Loader to load data incrementally? 

 

Spark Structured Streaming 

 

Which of the following statements best describes Auto Loader ? 

 

Autoloader monitors a source location, in which files accumulate, to identify and ingest …........... 

 

Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage. 

 

A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline: 
 

CONSTRAINT valid_id EXPECT (id IS NOT NULL) _____________ 

 

Fill in the above blank so records violating this constraint will be added to the target table, and reported in metrics 

 

There is no need to add ON VIOLATION clause. 

 

By default, records that violate the expectation are added to the target dataset along with valid records, but violations will be reported in the event log 

 

The data engineer team has a DLT pipeline that updates all the tables once and then stops. The compute resources of the pipeline continue running to allow for quick testing. 

 

Which of the following best describes the execution modes of this DLT pipeline ? 

 

The DLT pipeline executes in Triggered Pipeline mode under Development mode. 

 

Which of the following will utilize Gold tables as their source? 

 

Dashboards 

 

Which of the following code blocks can a data engineer use to query the existing streaming table events ? 

 

Delta Lake is deeply integrated with Spark Structured Streaming. You can load tables as a stream using: 

spark.readStream.table(table_name) 

 

In multi-hop architecture, which of the following statements best describes the Bronze layer ? 

 

It maintains raw data ingested from various sources. 

 

Given the following Structured Streaming query 

 
 

(spark.readStream 

       .format("cloudFiles") 

       .option("cloudFiles.format", "json") 

       .load(ordersLocation) 

    .writeStream 

       .option("checkpointLocation", checkpointPath) 

       .table("uncleanedOrders") 

) 

 
 

Which of the following best describe the purpose of this query in a multi-hop architecture? 

 

The query here is using Autoloader (cloudFiles) to load raw json data from ordersLocation into the Bronze table uncleanedOrders 

 

A data engineer has the following query in a Delta Live Tables pipeline: 

 
 

CREATE LIVE TABLE aggregated_sales 

AS 

 SELECT store_id, sum(total) 

 FROM cleaned_sales 

 GROUP BY store_id 

 
 

The pipeline is failing to start due to an error in this query 

 

Which of the following changes should be made to this query to successfully start the DLT pipeline ? 

 

CREATE LIVE TABLE aggregated_sales 

AS 

SELECT store_id, sum(total) 

FROM LIVE.cleaned_sales 

GROUP BY store_id 

 

A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline: 
 

CONSTRAINT valid_id EXPECT (id IS NOT NULL) _____________ 

 

Fill in the above blank so records violating this constraint will be dropped, and reported in metrics 

 

ON VIOLATION DROP ROW 

 

Which of the following compute resources is available in Databricks SQL ? 

 

SQL warehouse 

 

Which of the following is the benefit of using the Auto Stop feature of Databricks SQL warehouses ? 

 

Minimizes the total running time of the warehouse. 

 

Which of the following alert destinations is Not supported in Databricks SQL ? 

 

SMS 

 

A data engineering team has a long-running multi-tasks Job. The team members need to be notified when the run of this job completes. 
 

Which of the following approaches can be used to send emails to the team members when the job completes ? 

 

They can configure email notification settings in the job page. 

 

A data engineer wants to increase the cluster size of an existing Databricks SQL warehouse. 
 

Which of the following is the benefit of increasing the cluster size of Databricks SQL warehouses ? 

 

Improves the latency of the queries execution. 

 

Which of the following describes Cron syntax in Databricks Jobs ? 

 

It is an expression to represent complex job schedule that can be defined programmatically. 

The data engineer team has a DLT pipeline that updates all the tables at defined intervals until manually stopped. The compute resources terminate when the pipeline is stopped. 
 

Which of the following best describes the execution modes of this DLT pipeline ? 

 

The DLT pipeline executes in Continuous Pipeline mode under Production mode. 

 

Which part of the Databricks Platform can a data engineer use to grant permissions on tables to users ? 

 

Data Explorer 

 

Which of the following commands can a data engineer use to grant full permissions to the HR team on the table employees ? 

 

GRANT ALL PRIVILEGES ON TABLE employees TO hr_team 

 

A data engineer uses the following SQL query: 

 

GRANT MODIFY ON TABLE employees TO hr_team 

 

Which of the following describes the ability given by the MODIFY privilege ? 

 

All the above abilities are given by the MODIFY privilrge. 

 

The MODIFY privilege gives the ability to add, delete, and modify data to or from an object. 

 

One of the foundational technologies provided by the Databricks Lakehouse Platform is an open-source, file-based storage format that brings reliability to data lakes. 

 

Which of the following technologies is being described in the above statement? 

 

Delta Lake 

 

Which of the following commands can a data engineer use to purge stale data files of a Delta table? 

 

VACUUM 

 

In Databricks Repos, which of the following operations a data engineer can use to save local changes of a repo to its remote repository ? 

 

Commit & Push 

In Delta Lake tables, which of the following is the primary format for the transaction log files? 

 

JSON 

 

Which of the following functionalities can be performed in Databricks Repos ? 

 

Pull from a remote Git repository 

 

Which of the following locations completely hosts the customer data ? 

 

Customer’s cloud account 

 

If the default notebook language is Python, which of the following options a data engineer can use to run SQL commands in this Python Notebook ? 

 

They can add %sql at the start of a cell 

 

A junior data engineer uses the built-in Databricks Notebooks versioning for source control. A senior data engineer recommended using Databricks Repos instead. 

 

Which of the following could explain why Databricks Repos is recommended instead of Databricks Notebooks versioning? 

 

Databricks repos supports creating and managing branches for development work 

 

Which of the following services provides a data warehousing experience to its users? 

 

Databricks SQL 

 

A data engineer noticed that there are unused data files in the directory of a Delta table. They executed the VACUUM command on this table; however, only some of those unused data files have been deleted. 

 

Which of the following could explain why only some of the unused data files have been deleted after running the VACUUM command ? 

 

The deleted data files are older than the default retention threshold. While the remaining files are newer than the default retention threshold and can not be deleted 

 

The data engineering team has a Delta table called products that contains products’ details including the net price. 

 
Which of the following code blocks will apply a 50% discount on all the products where the price is greater than 1000 and save the new price to the table? 

UPDATE products SET price = price * 0.5 WHERE price > 1000 

 

The data engineering team has a Delta table called products that contains products’ details including the net price. 
 

Which of the following code blocks will apply a 50% discount on all the products where the price is greater than 1000 and save the new price to the table? 

 

Temporary view 

 

A data engineer has a database named db_hr, and they want to know where this database was created in the underlying storage. 

 

Which of the following commands can the data engineer use to complete this task? 

 

DESCRIBE DATABASE db_hr 

 

Which of the following commands a data engineer can use to register the table orders from an existing SQLite database ? 

 

CREATE table orders 

USING org.apache.spark.sql.jdbc 

OPTIONS( 

Url “jdbc:….”, 

Dbtable “orders” 

) 

 

When dropping a Delta table, which of the following explains why both the table's metadata and the data files will be deleted ? 

 

The table is managed 

 

Given the following commands: 

 

CREATE DATABASE db_hr; 

  

USE db_hr; 

CREATE TABLE employees; 

 

In which of the following locations will the employees table be located? 

 

dbfs:/user/hive/warehouse/db_hr.db 

 

Which of the following code blocks can a data engineer use to create a Python function to multiply two integers and return the result? 

Def multiply_numbers(num1, num2): 

Return num1 * num2 

 

Given the following 2 tables: 

 

 
 

Fill in the blank to make the following query returns the below result: 

 
 

SELECT students.name, students.age, enrollments.course_id 

FROM students 

_____________ enrollments 

ON students.student_id = enrollments.student_id 

 
 

 

 

LEFT JOIN 

 

Which of the following SQL keywords can be used to rotate rows of a table by turning row values into multiple columns ? 

 

PIVOT 

 

Fill in the below blank to get the number of courses incremented by 1 for each student in array column students. 

 
 

SELECT 

 faculty_id, 

 students, 

 ___________ AS new_totals 

FROM faculties 

 

TRANSFORM(students, I -> I.total_courses+1) 

 

Fill in the below blank to successfully create a table using data from CSV files located at /path/input 

 
 

CREATE TABLE my_table 

(col1 STRING, col2 STRING) 

____________ 

OPTIONS (header = "true", 

       delimiter = ";") 

LOCATION = "/path/input" 

 

USING CSV 

 

Which of the following statements best describes the usage of CREATE SCHEMA command ? 

 

It’s used to create a database 

 

Which of the following statements is Not true about CTAS statements ? 

 

CTAS statement supports manual schema declaration 

 

Which of the following SQL commands will append this new row to the existing Delta table users? 

 

 

INSERT INTO allows inserting new rows into a Delta table. 

 

Given the following Structured Streaming query: 

 
 

(spark.table("orders") 

       .withColumn("total_after_tax", col("total")+col("tax")) 

   .writeStream 

       .option("checkpointLocation", checkpointPath) 

       .outputMode("append") 

       .___________ 

       .table("new_orders") ) 

 
 

Fill in the blank to make the query executes multiple micro-batches to process all available data, then stops the trigger. 

 

trigger(availableNow=True) 

 

Which of the following techniques allows Auto Loader to track the ingestion progress and store metadata of the discovered files ? 

 

Checkpointing 

 

A data engineer has defined the following data quality constraint in a Delta Live Tables pipeline: 

 

CONSTRAINT valid_id EXPECT (id IS NOT NULL) _____________ 

 

Fill in the above blank so records violating this constraint cause the pipeline to fail. 

 

ON VIOLATION FAIL UPDATE 

 

In multi-hop architecture, which of the following statements best describes the Silver layer tables? 

 

They provide a more refined view of raw data, where it’s filtered, cleaned and enriched 

 

The data engineer team has a DLT pipeline that updates all the tables at defined intervals until manually stopped. The compute resources of the pipeline continue running to allow for quick testing. 
 

Which of the following best describes the execution modes of this DLT pipeline ? 

 

The DLT pipeline executes in Continuous pipeline mode under Development mode 

 

Given the following Structured Streaming query: 

 
 

(spark.readStream 

       .table("cleanedOrders") 

       .groupBy("productCategory") 

       .agg(sum("totalWithTax")) 

   .writeStream 

       .option("checkpointLocation", checkpointPath) 

       .outputMode("complete") 

       .table("aggregatedOrders") 

) 

 
 

Which of the following best describe the purpose of this query in a multi-hop architecture? 

 

The query is performing a hop from Silver layer to a Gold table 

 

Given the following Structured Streaming query: 

 
 

(spark.readStream 

       .table("orders") 

   .writeStream 

       .option("checkpointLocation", checkpointPath) 

       .table("Output_Table") 

) 

 
 

Which of the following is the trigger Interval for this query ? 

 

Every half second 

 

A data engineer has the following query in a Delta Live Tables pipeline 

 
 

CREATE STREAMING LIVE TABLE sales_silver 

AS 

 SELECT store_id, total + tax AS total_after_tax 

 FROM LIVE.sales_bronze 

 
The pipeline is failing to start due to an error in this query. 

Which of the following changes should be made to this query to successfully start the DLT pipeline ? 

 

To query another live table, prepend always the LIVE. keyword to the table name. 

 
 

CREATE STREAMING LIVE TABLE table_name 

AS 

   SELECT * 

   FROM STREAM(LIVE.another_table) 

 

In multi-hop architecture, which of the following statements best describes the Gold layer tables? 

 

They provide business-level aggregations that power analytics, machine learning and production applications 

 

The data engineer team has a DLT pipeline that updates all the tables once and then stops. The compute resources of the pipeline terminate when the pipeline is stopped. 

 

Which of the following best describes the execution modes of this DLT pipeline ? 

 

The DLT pipeline executes in Trigerred pipeline mode under Production mode 

 

A data engineer needs to determine whether to use Auto Loader or COPY INTO command in order to load input data files incrementally. 

 

In which of the following scenarios should the data engineer use Auto Loader over COPY INTO command ? 

 

If they are going to ingest files in the order of millions or more over time 

 

From which of the following locations can a data engineer set a schedule to automatically refresh a Databricks SQL query ? 

 

From the query’s page in Datbricks SQL 

 

Databricks provides a declarative ETL framework for building reliable and maintainable data processing pipelines, while maintaining table dependencies and data quality. 
 

Which of the following technologies is being described above? 

 

Delta live tables 

 

Which of the following services can a data engineer use for orchestration purposes in Databricks platform ? 

 

Databricks jobs 

 

A data engineer has a Job with multiple tasks that takes more than 2 hours to complete. In the last run, the final task unexpectedly failed. 
 

Which of the following actions can the data engineer perform to complete this Job Run while minimizing the execution time ? 

 

They can repair their job run so only the failed tasks will be re-executed 

 

A data engineering team has a multi-tasks Job in production. The team members need to be notified in the case of job failure. 

 

Which of the following approaches can be used to send emails to the team members in the case of job failure ? 

 

They can configure email notifications settings in the job page 

 

For production jobs, which of the following cluster types is recommended to use? 

 

Job clusters 

 

In Databricks Jobs, which of the following approaches can a data engineer use to configure a linear dependency between Task A and Task B ? 

 

They can select the Task A in the Depends on field of the Task B configuration 

 

Which part of the Databricks Platform can a data engineer use to revoke permissions from users on tables ? 

 

Data Explorer 

 

A data engineer uses the following SQL query: 
 

GRANT USAGE ON DATABASE sales_db TO finance_team 
 

Which of the following is the benefit of the USAGE  privilege ? 

 

No effect 

 

In which of the following locations can a data engineer change the owner of a table? 

 

In Data Explorer, fromthe Owners field in the table’s page 

 

Which of the following describes a benefit of a data lakehouse that is unavailable in a traditional data warehouse? 

 

A data lakehouse enables both batch and streaming analytics 

 

Which of the following locations hosts the driver and worker nodes of a Databricks-managed cluster? 

 

Data plane 

 

A data architect is designing a data model that works for both video-based machine learning workloads and highly audited batch ETL/ELT workloads. 

Which of the following describes how using a data lakehouse can help the data architect meet the needs of both workloads? 

 

A data lakehouse stores unstructured data and is ACID-compliant. 

 

Which of the following describes a scenario in which a data engineer will want to use a Job cluster instead of an all-purpose cluster? 

 

An automated workflow needs to be run every 30 minutes. 

 

A data engineer has created a Delta table as part of a data pipeline. Downstream data analysts now need SELECT permission on the Delta table. 

Assuming the data engineer is the Delta table owner, which part of the Databricks Lakehouse Platform can the data engineer use to grant the data analysts the appropriate access? 

 

Data Explorer 

 

Two junior data engineers are authoring separate parts of a single data pipeline notebook. They are working on separate Git branches so they can pair program on the same notebook simultaneously. A senior data engineer experienced in Databricks suggests there is a better alternative for this type of collaboration. 

 

Databricks Notebooks support real-time coauthoring on a single notebook 

 

Which of the following describes how Databricks Repos can help facilitate CI/CD workflows on the Databricks Lakehouse Platform? 

 

Databricks Repos can commit or push code changes to trigger a CI/CD process 

 

Which of the following statements describes Delta Lake? 

Delta Lake is an open format storage layer that delivers reliability, security, and performance. 

 

A data architect has determined that a table of the following format is necessary: 

id 

birthDate 

avgRating 

a1 

1990-01-06 

5.5 

a2 

1974-11-21 

7.1 

… 

… 

… 

  

Which of the following code blocks uses SQL DDL commands to create an empty Delta table in the above format regardless of whether a table already exists with this name? 

 

CREATE OR REPLACE TABLE table_name ( 
  id STRING, 
  birthDate DATE, 
  avgRating FLOAT 
) 

 

Which of the following SQL keywords can be used to append new rows to an existing Delta table? 

 

INSERT INTO 

 

A data engineering team needs to query a Delta table to extract rows that all meet the same condition. However, the team has noticed that the query is running slowly. The team has already tuned the size of the data files. Upon investigating, the team has concluded that the rows meeting the condition are sparsely located throughout each of the data files. 

  

Based on the scenario, which of the following optimization techniques could speed up the query? 

 

Z-Ordering 

 

A data engineer needs to create a database called customer360 at the location /customer/customer360. The data engineer is unsure if one of their colleagues has already created the database. 

  

Which of the following commands should the data engineer run to complete this task? 

 

CREATE DATABASE IF NOT EXISTS customer360 LOCATION '/customer/customer360'; 

 

A junior data engineer needs to create a Spark SQL table my_table for which Spark manages both the data and the metadata. The metadata and data should also be stored in the Databricks Filesystem (DBFS). 

  

Which of the following commands should a senior data engineer share with the junior data engineer to complete this task? 

 

CREATE TABLE my_table (id STRING, value STRING); 

 

A data engineer wants to create a relational object by pulling data from two tables. The relational object must be used by other data engineers in other sessions. In order to save on storage costs, the data engineer wants to avoid copying and storing physical data. 

  

Which of the following relational objects should the data engineer create? 

 

View 

 

A data engineering team has created a series of tables using Parquet data stored in an external system. The team is noticing that after appending new rows to the data in the external system, their queries within Databricks are not returning the new rows. They identify the caching of the previous data as the cause of this issue. 

  

Which of the following approaches will ensure that the data returned by queries is always up-to-date? 

 

The tables should be converted to the Delta format 

 

A table customerLocations exists with the following schema: 

id STRING, 

date STRING, 

city STRING, 

country STRING 

  

A senior data engineer wants to create a new table from this table using the following command: 

  

CREATE TABLE customersPerCountry AS 
SELECT country, 
      COUNT(*) AS customers 

FROM customerLocations 
GROUP BY country; 

 
A junior data engineer asks why the schema is not being declared for the new table. 
 
Which of the following responses explains why declaring the schema is not necessary? 

 

CREATE TABLE AS SELECT statements adopt schema details from the source table and query. 

 

A data engineer is overwriting data in a table by deleting the table and recreating the table. Another data engineer suggests that this is inefficient and the table should simply be overwritten instead. 

  

Which of the following reasons to overwrite the table instead of deleting and recreating the table is incorrect? 

 

Overwriting a table results in a clean table history for logging and audit purposes. 

 

Which of the following commands will return records from an existing Delta table my_table where duplicates have been removed? 

 

SELECT DISTINCT * FROM my_table; 

 

A data engineer wants to horizontally combine two tables as a part of a query. They want to use a shared column as a key column, and they only want the query result to contain rows whose value in the key column is present in both tables. 

  

Which of the following SQL commands can they use to accomplish this task? 

 

INNER JOIN 

 

A junior data engineer has ingested a JSON file into a table raw_table with the following schema: 
 
cart_id STRING, 
items ARRAY<item_id:STRING> 

  

The junior data engineer would like to unnest the items column in raw_table to result in a new table with the following schema: 

  

cart_id STRING, 

item_id STRING 

  

Which of the following commands should the junior data engineer run to complete this task? 

 

SELECT cart_id, explode(items) AS item_id FROM raw_table; 

 

A data engineer has ingested a JSON file into a table raw_table with the following schema: 
 
transaction_id STRING, 
payload ARRAY<customer_id:STRING, date:TIMESTAMP, store_id:STRING> 

  

The data engineer wants to efficiently extract the date of each transaction into a table with the following schema: 

  

transaction_id STRING, 

date TIMESTAMP 

  

Which of the following commands should the data engineer run to complete this task? 

 

SELECT transaction_id, payload.date FROM raw_table; 

 

A data analyst has provided a data engineering team with the following Spark SQL query: 

  

SELECT district, 

       avg(sales) 

FROM store_sales_20220101 

GROUP BY district; 

  

The data analyst would like the data engineering team to run this query every day. The date at the end of the table name (20220101) should automatically be replaced with the current date each time the query is run. 

  

Which of the following approaches could be used by the data engineering team to efficiently automate this process? 

 

They could wrap the query using PySpark and use Python’s string variable system to automatically update the table name. 

 

A data engineer has ingested data from an external source into a PySpark DataFrame raw_df. They need to briefly make this data available in SQL for a data analyst to perform a quality assurance check on the data. 

  

Which of the following commands should the data engineer run to make this data available in SQL for only the remainder of the Spark session? 

 

raw_df.createOrReplaceTempView("raw_df") 

 

A data engineer needs to dynamically create a table name string using three Python variables: region, store, and year. An example of a table name is below when region = "nyc", store = "100", and year = "2021": 

  

nyc100_sales_2021 

  

Which of the following commands should the data engineer use to construct the table name in Python? 

 

f"{region}{store}_sales_{year}" 

 

A data engineer has developed a code block to perform a streaming read on a data source. The code block is below: 

  

(spark 

    .read 

    .schema(schema) 

    .format("cloudFiles") 

    .option("cloudFiles.format", "json") 

    .load(dataSource) 

) 

  

The code block is returning an error. 

  

Which of the following changes should be made to the code block to configure the block to successfully perform a streaming read? 

 

The .read line should be replaced with .readStream. 

 

A data engineer has configured a Structured Streaming job to read from a table, manipulate the data, and then perform a streaming write into a new table. 

  

The code block used by the data engineer is below: 

  

(spark.table("sales") 

    .withColumn("avg_price", col("sales") / col("units"))                              

    .writeStream                                                 

    .option("checkpointLocation", checkpointPath) 

    .outputMode("complete") 

    ._____ 

    .table("new_sales") 

) 

  

If the data engineer only wants the query to execute a single micro-batch to process all of the available data, which of the following lines of code should the data engineer use to fill in the blank? 

 

trigger(once=True) 

 

A data engineer is designing a data pipeline. The source system generates files in a shared directory that is also used by other processes. As a result, the files should be kept as is and will accumulate in the directory. The data engineer needs to identify which files are new since the previous run in the pipeline, and set up the pipeline to only ingest those new files with each run. 

  

Which of the following tools can the data engineer use to solve this problem? 

 

Auto Loader 

 

A data engineering team is in the process of converting their existing data pipeline to utilize Auto Loader for incremental processing in the ingestion of JSON files. One data engineer comes across the following code block in the Auto Loader documentation: 
 
(streaming_df = spark.readStream.format("cloudFiles") 

    .option("cloudFiles.format", "json") 

    .option("cloudFiles.schemaLocation", schemaLocation) 

    .load(sourcePath)) 

 
Assuming that schemaLocation and sourcePath have been set correctly, which of the following changes does the data engineer need to make to convert this code block to use Auto Loader to ingest the data? 

 

There is no change required. The inclusion of format("cloudFiles") enables the use of Auto Loader. 
 

Which of the following data workloads will utilize a Bronze table as its source? 

 

A job that enriches data by parsing its timestamps into a human-readable format 

 

Which of the following data workloads will utilize a Silver table as its source? 

 

A job that aggregates cleaned data to create standard summary statistics 

 

Which of the following Structured Streaming queries is performing a hop from a Bronze table to a Silver table? 

 

(spark.table("sales") 
    .withColumn("avgPrice", col("sales") / col("units"))                           
    .writeStream                                             
    .option("checkpointLocation", checkpointPath) 
    .outputMode("append") 
    .table("cleanedSales") 
) 

 

Which of the following benefits does Delta Live Tables provide for ELT pipelines over standard data pipelines that utilize Spark and Delta Lake on Databricks? 

 

The ability to declare and maintain data table dependencies 

 

A data engineer has three notebooks in an ELT pipeline. The notebooks need to be executed in a specific order for the pipeline to complete successfully. The data engineer would like to use Delta Live Tables to manage this process. 

  

Which of the following steps must the data engineer take as part of implementing this pipeline using Delta Live Tables? 

 

They need to create a Delta Live Tables pipeline from the Jobs page. 

A data engineer has written the following query: 

SELECT *  

FROM json.`/path/to/json/file.json`; 

The data engineer asks a colleague for help to convert this query for use in a Delta Live Tables (DLT) pipeline. The query should create the first table in the DLT pipeline. 

Which of the following describes the change the colleague needs to make to the query? 

They need to add a CREATE LIVE TABLE table_name AS line at the beginning of the query. 

A dataset has been defined using Delta Live Tables and includes an expectations clause: 

CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01')  

What is the expected behavior when a batch of data containing data that violates these constraints is processed? 

Records that violate the expectation are added to the target dataset and recorded as invalid in the event log. 

A Delta Live Table pipeline includes two datasets defined using STREAMING LIVE TABLE. Three datasets are defined against Delta Lake table sources using LIVE TABLE. 

The table is configured to run in Development mode using the Triggered Pipeline Mode. 

Assuming previously unprocessed data exists and all definitions are valid, what is the expected outcome after clicking Start to update the pipeline? 

All datasets will be updated once and the pipeline will shut down. The compute resources will persist to allow for additional testing. 

A data engineer has a Job with multiple tasks that runs nightly. One of the tasks unexpectedly fails during 10 percent of the runs.  

Which of the following actions can the data engineer perform to ensure the Job completes each night while minimizing compute costs? 

They can institute a retry policy for the task that periodically fails 

A data engineer has set up two Jobs that each run nightly. The first Job starts at 12:00 AM, and it usually completes in about 20 minutes. The second Job depends on the first Job, and it starts at 12:30 AM. Sometimes, the second Job fails when the first Job does not complete by 12:30 AM. 

Which of the following approaches can the data engineer use to avoid this problem? 

They can utilize multiple tasks in a single job with a linear dependency 

A data engineer has set up a notebook to automatically process using a Job. The data engineer’s manager wants to version control the schedule due to its complexity. 

Which of the following approaches can the data engineer use to obtain a version-controllable configuration of the Job’s schedule? 

They can download the JSON description of the Job from the Job’s page. 

A data analyst has noticed that their Databricks SQL queries are running too slowly. They claim that this issue is affecting all of their sequentially run queries. They ask the data engineering team for help. The data engineering team notices that each of the queries uses the same SQL endpoint, but the SQL endpoint is not used by any other user. 

Which of the following approaches can the data engineering team use to improve the latency of the data analyst’s queries? 

They can increase the cluster size of the SQL endpoint. 

An engineering manager uses a Databricks SQL query to monitor their team’s progress on fixes related to customer-reported bugs. The manager checks the results of the query every day, but they are manually rerunning the query each day and waiting for the results. 

Which of the following approaches can the manager use to ensure the results of the query are updated each day? 

They can schedule the query to refresh every 1 day from the query’s page in Databricks SQL. 

A data engineering team has been using a Databricks SQL query to monitor the performance of an ELT job. The ELT job is triggered by a specific number of input records being ready to process. The Databricks SQL query returns the number of minutes since the job’s most recent runtime. 

Which of the following approaches can enable the data engineering team to be notified if the ELT job has not been run in an hour? 

They can set up an Alert for the query to notify them if the returned value is greater than 60. 

A data engineering manager has noticed that each of the queries in a Databricks SQL dashboard takes a few minutes to update when they manually click the “Refresh” button. They are curious why this might be occurring, so a team member provides a variety of reasons on why the delay might be occurring. 

Which of the following reasons fails to explain why the dashboard might be taking a few minutes to update? 

The Job associated with updating the dashboard might be using a non-pooled endpoint. 

A new data engineer has started at a company. The data engineer has recently been added to the company’s Databricks workspace as new.engineer@company.com. The data engineer needs to be able to query the table sales in the database retail. The new data engineer already has been granted USAGE on the database retail. 

Which of the following commands can be used to grant the appropriate permissions to the new data engineer? 

GRANT SELECT ON TABLE sales TO new.engineer@company.com; 

A new data engineer new.engineer@company.com has been assigned to an ELT project. The new data engineer will need full privileges on the table sales to fully manage the project. 

Which of the following commands can be used to grant full permissions on the table to the new data engineer? 

GRANT ALL PRIVILEGES ON TABLE sales TO new.engineer@company.com; 
