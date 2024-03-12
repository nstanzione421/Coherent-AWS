import sys
import requests
import json
from datetime import *

from pyspark.context import *
from pyspark.sql.functions import udf, col, date_format, lit

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    'JOB_NAME'
    ])
    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
df = spark.read.csv("s3://spark-demo-data-source/Testbed_Data.csv", header=True, inferSchema=True)

print(df.head(10))

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

url = 'https://excel.uat.us.coherent.global/presales/api/v3/folders/DemoStanz/services/basic_term_sample/Execute'
headers = {
  'Content-Type': 'application/json',
  'x-tenant-name': 'presales',
  'x-synthetic-key': '5e2685f0-ee41-47ff-8fa8-650f2307de11'
}

@udf
def testSparkCall(col1, col2, col3):

    payload =json.dumps({
      "request_data": {
          "inputs": {
             "AgeEntry": col1,
             "PolicyTerm": col2,
             "SumAssured": col3
          }
      },
      "request_meta": {
          "call_purpose": "AWS Test",
          "source_system": "",
          "correlation_id": "",
          "service_category": ""
      }
    })
    
    response = requests.request("POST", url, headers=headers, data=payload, allow_redirects=False)
    resp = response.json()
    r = resp['response_data']['outputs']['NetPremium']
    
    return r
    

test_set = df.limit(20)
    
# udf_testSparkCall = udf(testSparkCall).asNondeterministic()
df_lookup = test_set.withColumn('NetPremium', testSparkCall(col('AgeEntry'), col('PolicyTerm'), col('SumAssured')))

currtime = datetime.now(timezone.utc)

df_lookup = df_lookup.withColumn("timestamp", date_format(lit(currtime), "yyyy-MM-dd HH:mm:ss"))
df_lookup.show()
df_lookup.coalesce(1).write.csv("s3://spark-demo-data-source/test/test", header=True)

## COPY OVER FILE TO NEW BUCKET ##

import boto3
client = boto3.client('s3')


source_bucket = 'spark-demo-data-source' # e.g. - 'source-s3-bucket'
srcPrefix = 'test/test/' # gzipcsvsingle/
target_bucket = 'spark-cleaned-data' # e.g. - 'target-s3-bucket'
targetPrefix = 'test/update1/' # gzipcsvsingle/output/ 

## Get a list of files with prefix (we know there will be only one file)
response = client.list_objects(
    Bucket = source_bucket,
    Prefix = srcPrefix,
    # Delimiter='/'
)
name = response["Contents"][0]["Key"]

print(name)

## Store Target File File Prefix, this is the new name of the file
target_source = {'Bucket': source_bucket, 'Key': name}

print(target_source)

timestamp_filename = currtime.strftime("%Y%m%d%H%M%S")
target_key = targetPrefix + f'output_{timestamp_filename}.csv' # output.csv - File Name 

print(target_key)

### Now Copy the file with New Name
client.copy(CopySource=target_source, Bucket=target_bucket,  Key=target_key)

### Delete the old file
client.delete_object(Bucket=source_bucket, Key=name)
    
    
    
# df_lookup.repartition(1).write.csv("s3://spark-demo-data-source/test/next.csv", header=True)
# df_lookup.write.parquet("s3://spark-demo-data-source/output_parquet/PremiumOutput_Test")
# df_lookup.write.format('json').save("s3://spark-demo-data-source/output_json/PremiumOutput_Test")


