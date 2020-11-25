# Unzip-taefile-into-S3


from pyspark.sql import SparkSession
#from pyspark.sql.functions import col
#from pyspark.sql.functions import lit
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark import SparkContext
from pyspark.sql import SQLContext
from  pyspark.sql.functions import input_file_name
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


glueContext = GlueContext(SparkContext.getOrCreate())

sc = SparkContext
sqlCtx = SQLContext

spark = SparkSession.builder.appName("s3://cp-prod_20200915_011001.tgz").getOrCreate()
df = spark.read.csv("s3://cp-prod-20200915_011001.tgz")
df.show()

df2=DynamicFrame.fromDF(df, glueContext, "test_nest")

asink4 = glueContext.write_dynamic_frame.from_options(frame = df2, connection_type = "s3", connection_options = {"s3://cp-prod/unzip_tar/"}, format = "csv", transformation_ctx = "datasink4")
job.commit()
