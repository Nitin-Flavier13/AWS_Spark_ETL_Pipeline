import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
                SparkSession.builder.appName('AWS_Spark_ETL')
                .config('spark.jars.packages',
                        'org.apache.hadoop:hadoop-aws:3.3.1,'
                        'com.amazonaws:aws-java-sdk:1.11.469')
                .config("spark.hadoop.fs.s3a.impl", 
                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.access.key", 
                        os.getenv("AWS_ACCESS_KEY_ID"))
                .config("spark.hadoop.fs.s3a.secret.key", 
                        os.getenv("AWS_SECRET_ACCESS_KEY"))
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.region", 
                        os.getenv("AWS_REGION"))
                .getOrCreate()
                # .config("spark.hadoop.fs.s3a.endpoint", 
                #         "s3.amazonaws.com")
    )