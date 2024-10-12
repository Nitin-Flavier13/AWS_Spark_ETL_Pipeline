import os
from dotenv import load_dotenv
from udf_utils import extract
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType

def define_udf():
    
    def extract_filename_udf(file_content):
        obj = extract(file_content)  # Instantiate extract class with file_content
        return obj.extract_file_name()
    
    def extract_position_udf(file_content):
        obj = extract(file_content)
        return obj.extract_position()
    
    def extract_salary_udf(file_content):
        obj = extract(file_content)
        return obj.extract_salary()  # Assuming it returns a dict with 'start_salary' and 'end_salary'
    
    def extract_startdate_udf(file_content):
        obj = extract(file_content)
        return obj.extract_start_date()
    
    def extract_enddate_udf(file_content):
        obj = extract(file_content)
        return obj.extract_end_date()
    
    def extract_classcode_udf(file_content):
        obj = extract(file_content)
        return obj.extract_class_code()
    
    def extract_requirements_udf(file_content):
        obj = extract(file_content)
        return obj.extract_requirements()
    
    def extract_notes_udf(file_content):
        obj = extract(file_content)
        return obj.extract_notes()
    
    def extract_duties_udf(file_content):
        obj = extract(file_content)
        return obj.extract_duties()
    
    def extract_selection_udf(file_content):
        obj = extract(file_content)
        return obj.extract_selection()
    
    def extract_experience_length_udf(file_content):
        obj = extract(file_content)
        return obj.extract_experience_length()
    
    def extract_education_length_udf(file_content):
        obj = extract(file_content)
        return obj.extract_education_length()
    
    def extract_job_location_udf(file_content):
        obj = extract(file_content)
        return obj.extract_job_location()
    
    # Register UDFs with appropriate return types
    return {
        'extract_filename_udf': udf(extract_filename_udf, StringType()),
        'extract_position_udf': udf(extract_position_udf, StringType()),
        'extract_salary_udf': udf(extract_salary_udf, StructType([
            StructField('start_salary', IntegerType(), True),
            StructField('end_salary', IntegerType(), True)
        ])),
        'extract_startdate_udf': udf(extract_startdate_udf, DateType()),
        'extract_enddate_udf': udf(extract_enddate_udf, DateType()),
        'extract_classcode_udf': udf(extract_classcode_udf, StringType()),
        'extract_requirements_udf': udf(extract_requirements_udf, StringType()),
        'extract_notes_udf': udf(extract_notes_udf, StringType()),
        'extract_duties_udf': udf(extract_duties_udf, StringType()),
        'extract_selection_udf': udf(extract_selection_udf, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length_udf, StringType()),
        'extract_education_length_udf': udf(extract_education_length_udf, StringType()),
        'extract_job_location_udf': udf(extract_job_location_udf, StringType()),
    }
if __name__ == "__main__":

    load_dotenv()

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
                .config("spark.master", "local[*]")
                .getOrCreate()
                # .config("spark.hadoop.fs.s3a.endpoint", 
                #         "s3.amazonaws.com")
    )


    text_data_dir = Path(os.getenv('text_dir')).as_posix()
    # text_data_dir = os.getenv('text_dir')
    # json_data_dir = os.getenv('json_dir')
    # csv_data_dir = os.getenv('csv_dir')
    # pdf_data_dir = os.getenv('pdf_dir')
    # video_data_dir = os.getenv('video_dir')
    # img_data_dir = os.getenv('img_dir')

#     text_data_dir = Path(r"C:\Users\Nitin Flavier\OneDrive\Desktop\Web_Development\Data_Engineering\AWS_Spark_ETL\data\data_text").as_posix()
    dataSchema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('classcode', StringType(), True),
        StructField('salary_start', IntegerType(), True),
        StructField('salary_end', IntegerType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('req', StringType(), True),
        StructField('notes', StringType(), True),
        StructField('duties', StringType(), True),
        StructField('selection', StringType(), True),
        StructField('experience_length', StringType(), True),
        StructField('job_type', StringType(), True),
        StructField('education_length', StringType(), True),
        StructField('school_type', StringType(), True),
        StructField('job_location', StringType(), True),

    ])

    # user defined functions
    # make sure the return type of udf functions match the schema mentioned,
    # otherwise it will take null value it will not typecast for eg
    # if udf returns INT but the schema is Doubletype then it will store
    # the values as NULL
    
    udf = define_udf()

    data_text_df = ( spark.readStream            
                     .format('text')        
                     .option('wholetext','true') 
                     .load(text_data_dir)
                )
      
    data_text_df = data_text_df.withColumn('value',regexp_replace('value',r'\r',''))
    data_text_df = data_text_df.withColumn('file_name',udf['extract_filename_udf']('value'))
    data_text_df = data_text_df.withColumn('classcode',udf['extract_classcode_udf']('value'))
    data_text_df = data_text_df.withColumn('position',udf['extract_position_udf']('value'))
    data_text_df = data_text_df.withColumn('start_date',udf['extract_startdate_udf']('value'))
    data_text_df = data_text_df.withColumn('end_date',udf['extract_enddate_udf']('value'))
    data_text_df = data_text_df.withColumn('salary_start',udf['extract_salary_udf']('value').getField('start_salary'))
    data_text_df = data_text_df.withColumn('salary_end',udf['extract_salary_udf']('value').getField('end_salary'))

    data_text_df = data_text_df.select('file_name','position','classcode','salary_start','salary_end','start_date','end_date')

#     Testing 
#     data_text_df = (spark.read             
#                 .format('text')        
#                 .option('wholeText', 'true')  # Correct option
#                 .load(text_data_dir)
#         )
    # data_text_df.printSchema()  
    # data_text_df.show(truncate=False)

    # print(data_text_df('values'))
    query = data_text_df.writeStream.outputMode('append').format('console').option('truncate',False).start()

    query.awaitTermination()



