#!/usr/bin/env python
# Usage: Data pipeline from Kinesis to Redshift
# Env: Python
# Version: 1.0
# Commit History:
# 15/07/21 - Initial version

import os, sys, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json
from pyspark import StorageLevel

def write_to_redshift(target_df, batch_id):

    redshift_url = "jdbc:redshift://{0}:5439/{1}?user={2}&password={3}".format(os.getenv('REDSHIFT_CLUSTER'),
                                                                               os.getenv('REDSHIFT_DB'),
                                                                               os.getenv('REDSHIFT_USER'),
                                                                               os.getenv('REDSHIFT_PASSWORD'))

    s3_output_path = "s3a://{0}/{1}/{2}".format(os.getenv('S3_BUCKET'),
                                             os.getenv('S3_BACKUP_HOME'),
                                             datetime.datetime.now().strftime("%m-%d-%Y"))

    target_df.persist(StorageLevel.MEMORY_AND_DISK)

    target_df.write\
        .format("parquet") \
        .option("path", s3_output_path) \
        .mode('append')\
        .save()

    target_df.write\
        .format("jdbc")\
        .option("url", redshift_url)\
        .option("dbtable",os.getenv('REDSHIFT_TABLE'))\
        .option("driver","com.amazon.redshift.jdbc.Driver")\
        .mode('append')\
        .save()

if __name__ == '__main__' :

    env_list = ['AWS_ACCESS_ID', 'AWS_ACCESS_SECRET', 'AWS_S3_ENDPOINT',
                'KINESIS_URL', 'KINESIS_STREAM', 'REDSHIFT_TABLE',
                'REDSHIFT_CLUSTER', 'REDSHIFT_DB', 'REDSHIFT_USER',
                'REDSHIFT_PASSWORD', 'S3_BUCKET', 'S3_BACKUP_HOME']

    for e in env_list:
        if not os.environ.has_key(e):
            print("Environment not setup correctly. Missing {} value.".format(e))
            sys.exit(1)

    checkpoint_dir = os.path.join(os.getenv('HOME'), 'checkpoint')

    spark = SparkSession \
            .builder \
            .appName("Kinesis To Redshift") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', os.getenv('AWS_ACCESS_ID'))
    hadoopConf.set('fs.s3a.secret.key', os.getenv('AWS_ACCESS_SECRET'))
    hadoopConf.set('fs.s3a.endpoint', os.getenv('AWS_S3_ENDPOINT'))
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    hadoopConf.set('fs.s3a.multipart.size', '104857600')
    hadoopConf.set('fs.s3a.threads.max', '4')
    hadoopConf.set('fs.s3a.threads.core', '4')

    event_schema = StructType(
        [StructField('request_id', StringType()),
         StructField('request_timestamp', StringType()),
         StructField('cookie_id', StringType()),
         StructField('topic', StringType()),
         StructField('message', StringType()),
         StructField('environment', StringType()),
         StructField('website_id', StringType()),
         StructField('user_account_id', StringType()),
         StructField('location', StringType()),
         StructField('user_agent', StringType()),
         StructField('referrer', StringType())
         ])

    input_df = spark.readStream \
        .format("kinesis") \
        .option("streamName", os.getenv('KINESIS_STREAM')) \
        .option("endpointUrl", os.getenv('KINESIS_URL')) \
        .option("awsAccessKeyId", os.getenv('AWS_ACCESS_ID')) \
        .option("awsSecretKey", os.getenv('AWS_ACCESS_SECRET')) \
        .option("startingposition", "latest") \
        .load()

    output_df = input_df.select(col("data").cast("string"))\
        .withColumn("value_json", from_json(col("data"), event_schema))\
        .select("value_json.*")


    stream_query = output_df.writeStream \
                            .foreachBatch(write_to_redshift) \
                            .outputMode("append") \
                            .option("checkpointLocation", checkpoint_dir) \
                            .trigger(processingTime="1 minute") \
                            .start()

    stream_query.awaitTermination()