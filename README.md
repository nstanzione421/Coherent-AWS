# AWS Analytics Workflow Demo

## TECH STACK REQUIREMENTS
- AWS S3
- AWS Glue
- AWS Athena
- AWS Quicksight

## USING AWS
 - Admin access accross AWS account in order to ensure proper permissions
 - Abitility to set up and read/write data files to S3 Buckets
 - AWS Glue: Coding in Python and PySpark 
 - ATHENA: SQL Syntax
 - QUICKSIGHT: Typical no-code BI Tool

## Step by Step Process
1. Set up two S3 buckets
    - 1 will be used as a landing spot for your data being received from Coherent Spark
    - 1 will be used as a "cleaned" version to allow for proper naming conventions of output files
    - Save input data for Coherent Spark calls in another folder within 1 of these 2 buckets 
2. Open AWS Glue
    - The purpose of the Glue scripts is to extract the data, transofrm it to JSON request for Spark, and save result files
    - Create Spark Script or Python Script to reference the input data in S3
    - Python Shell Scripts are much easier to use for single API Calls
    - Spark Script seems to better when wanting to run batches of API Calls
    - **STILL NEEDED** The Spark Script still needs to be tested at scale 1K, 10K, 100K+
3. Set up AWS Crawler
    - Purpose is to auotmatically review new data landing in the S3 buckets and setting up tables to be queried in Athena
    - AWS Crawler can be set up to scan S3 folders
    - Will need to create a database an associate S3 bucket for data
    - Note: When looking to create output tables in database: 
        - In Step 4: use "Advanced" to set the level of the folder you wish the tables to be determined
        - For instance, S3 Bucket > Input > Files (folder with csv) would be a level 3 for those csv files to be 1 table
        - Best to organize files so schemas are consistent within folders
    - This will make a database that can be referenced by Athena/Quicksight
4. Create Quicksight account
    - Select new dataset: Athena
    - Select Direct Query
        - An option for AWSDataCrawler will appear: select the database and table you created via the crawler
        - Direct Query Option ->
        - Nothing else needed to get data table as is
        - Clicking "Edit" will allow you to use SQL to create new fields and query the database (if necessary)
5. Use Quicksight visulations (Fields should appear from direct query from Step #4)