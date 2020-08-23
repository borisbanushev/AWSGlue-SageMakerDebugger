# some change
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import time
import boto3


from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


############################ 1. GET THE DATA ############################

##### [BB] You need to run an AWS Glue crawler on the data and save into the database and table_name from the next role

## AWS GLUE MAPPINGS
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "bbb-glue-crawler-taxi-db", table_name = "taxidata_csv", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vendorid", "long", "vendorid", "long"), ("lpep_pickup_datetime", "string", "lpep_pickup_datetime", "string"), ("lpep_dropoff_datetime", "string", "lpep_dropoff_datetime", "string"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("ratecodeid", "long", "ratecodeid", "long"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("ehail_fee", "string", "ehail_fee", "string"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("payment_type", "long", "payment_type", "long"), ("trip_type", "long", "trip_type", "long")], transformation_ctx = "applymapping1")
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["vendorid", "lpep_pickup_datetime", "lpep_dropoff_datetime", "store_and_fwd_flag", "ratecodeid", "pulocationid", "dolocationid", "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee", "improvement_surcharge", "total_amount", "payment_type", "trip_type"], transformation_ctx = "selectfields2")
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "bbb-glue-crawler-taxi-db", table_name = "taxidata_csv", transformation_ctx = "resolvechoice3")


############################ 2. START PROCESSING ############################
def get_weekday(year, month, day):
    import datetime
    import calendar
    weekday = datetime.date(year, month, day)
    return calendar.day_name[weekday.weekday()]

spark.udf.register('get_weekday', get_weekday)
from pyspark.sql.types import IntegerType

resolvechoice3.toDF().createOrReplaceTempView("taxi")

###### [BB] TRY TO DO AS MUCH OF THE PROCESSING IN SPARKSQL, RATHER THAN ON PYTHON LEVEL
resolvechoice3 = spark.sql("SELECT PULocationID, DOLocationID, passenger_count, trip_distance, RatecodeID, \
                    total_amount, payment_type, trip_type, tip_amount, fare_amount, \
                    ROUND(CAST(tip_amount/fare_amount AS DOUBLE), 4) as tip_percent, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'yyyy') AS INT) as pickup_year, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'MM') AS INT) as pickup_month,\
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'dd') AS INT) as pickup_day, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'hh') AS INT) as pickup_hour, \
                    CAST(from_unixtime(unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'mm') AS INT) as pickup_minute, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'yyyy') AS INT) as dropoff_year, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'MM') AS INT) as dropoff_month,\
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'dd') AS INT) as dropoff_day, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'hh') AS INT) as dropoff_hour, \
                    CAST(from_unixtime(unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa'), 'mm') AS INT) as dropoff_minute, \
                    ROUND(CAST((unix_timestamp(lpep_dropoff_datetime, 'MM/dd/yyyy hh:mm:ss aa') - unix_timestamp(lpep_pickup_datetime, 'MM/dd/yyyy hh:mm:ss aa'))/360 AS DOUBLE), 4) as tripdurr \
                    FROM taxi WHERE fare_amount > 2.50")


resolvechoice3.createOrReplaceTempView("taxi")
sqlDF = spark.sql("SELECT *, get_weekday(pickup_year, pickup_month, pickup_day) as day_of_week FROM taxi")
sqlDF.createOrReplaceTempView("taxi")
sqlDF = spark.sql("SELECT *, \
                    CASE WHEN day_of_week = 'Sunday' OR day_of_week = 'Saturday' THEN 1 ELSE 0 END as is_weekend, \
                    CASE WHEN pickup_hour = 7 or pickup_hour = 8 THEN 1 ELSE 0 END as is_rush_hour \
                  FROM taxi")
sqlDF.createOrReplaceTempView("taxi")

sqlDF = spark.sql("SELECT *, \
    ROUND(CAST(trip_distance/tripdurr AS DOUBLE), 4) as avg_speed \
    FROM taxi WHERE pickup_month in (1, 2, 3) AND pickup_year=2017 AND tip_percent<1")

sqlDF.createOrReplaceTempView("taxi")    
def check_airport_id(id):
     return int((id == 1) | (id == 2))
    
check_airport_id_udf = udf(check_airport_id, IntegerType())
sqlDF = sqlDF.withColumn("is_airport", check_airport_id_udf(sqlDF['RateCodeID']))
    
sqlDF.createOrReplaceTempView("taxi")
sqlDF = spark.sql("SELECT tip_amount, passenger_count, trip_distance, RatecodeID, total_amount, payment_type, trip_type, fare_amount, tip_percent, pickup_year, pickup_month, pickup_day, pickup_hour, pickup_minute, dropoff_year, dropoff_month, dropoff_day, dropoff_hour, dropoff_minute, tripdurr, avg_speed, is_airport, is_weekend, is_rush_hour FROM taxi")


############################ 3. SPLIT THE DATA AND SAVE TO S3 ############################
sqlDF.createOrReplaceTempView("taxi")
train_DF = spark.sql("SELECT * FROM taxi WHERE pickup_month in (1) and pickup_year=2017")
train_DF = train_DF.drop("pickup_year").drop("pickup_month").drop("dropoff_year").drop("dropoff_month")
validation_DF = spark.sql("SELECT * FROM taxi WHERE pickup_month in (2) and pickup_year=2017")
validation_DF = validation_DF.drop("pickup_year").drop("pickup_month").drop("dropoff_year").drop("dropoff_month")
test_DF = spark.sql("SELECT * FROM taxi WHERE pickup_month in (3) and pickup_year=2017")
test_DF = test_DF.drop("pickup_year").drop("pickup_month").drop("dropoff_year").drop("dropoff_month")

###### [BB] CHANGE THE BUCKET NAME WHERE YOU WANT TO STORE
bucket = '<<bucket_name>>'
bucket_prefix = 'taxidata_v{}'.format(time.strftime("%Y%m%d%H%M%S", time.gmtime()))
train_DF.repartition(1).write.csv('s3://{}/{}/train/train.csv'.format(bucket, bucket_prefix))
validation_DF.repartition(1).write.csv('s3://{}/{}/validation/validation.csv'.format(bucket, bucket_prefix))
test_DF.repartition(1).write.csv('s3://{}/{}/test/test.csv'.format(bucket, bucket_prefix))

###### [BB] MAKE SURE YOU CREATE A DYNAMODB TABLE AND CHANGE <<dynamodb_table_name>> TO THE REAL NAME
dynamodb_table = boto3.resource('dynamodb', region_name='us-east-1').Table('<<dynamodb_table_name>>')
esponse = dynamodb_table.update_item(Key={'bucketid': 'validation'}, UpdateExpression="SET prefix= :var1", ExpressionAttributeValues={':var1': bucket_prefix},ReturnValues="UPDATED_NEW")

                    

#resolvechoice3 = DynamicFrame.fromDF(resolvechoice3, glueContext, "nested")
#datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "bbb-glue-crawler-taxi-db", table_name = "taxidata_csv", transformation_ctx = "datasink4")
#output_dir = "s3://aws-emr-resources-507786327009-us-east-1/medicare_parquet"
#glueContext.write_dynamic_frame.from_options(frame = resolvechoice3, connection_type = "s3", connection_options = {"path": output_dir}, format = "parquet")
job.commit()