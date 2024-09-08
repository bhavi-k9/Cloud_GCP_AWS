import sys
import re
import csv
from functools import reduce

from google.cloud import storage


from pyspark.sql.session import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


BUCKET_NAME = sys.argv[1]
print(BUCKET_NAME)
PROJECT_ID = sys.argv[2]
print(PROJECT_ID)
CHANGE_DATE = sys.argv[3]
print(CHANGE_DATE)
CONFIG_PATH = sys.argv[4]
print(CONFIG_PATH)
ARCHIVE_BUCKET = sys.argv[5]
print("archive_path", ARCHIVE_BUCKET)


client = storage.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

#############pick up files from GCS folder############

def print_gcs_csv_files(bucket_name, folder_path):
    csv_files_list = []
    storage_client = storage.Client() 
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)

    for blob in blobs:
      
        if blob.name.startswith(folder_path) and blob.name.endswith(".csv"):
            csv_files_list.append(blob.name)
    return csv_files_list


#############spark session####################

spark = SparkSession.builder.appName("RemoveTrailingFooter").getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", False)
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.enabled",True)
sc = spark.sparkContext
files_list = print_gcs_csv_files(BUCKET_NAME, 'ierpf/ongoing/{}/'.format(CHANGE_DATE))

#prj-dfdl-407-ierpf-d-407-dev-data/ierpf/ongoing/20230606000911
#prj-dfdl-407-ierpf-d-407-dev-code/config/scripts/config.json

"""
UDF for performing validation 
"""
def validation(cell):
    
    return re.sub("(.*?)(-)?$", lambda m: "-" + m.group(1) if m.group(2) else m.group(1), str(cell)) 



validate_udf = udf(validation, StringType())

"""
Archiving files to archive location
"""
def archive_datafiles(databucket,build_datafilename,archivebucket):
    datafilebucket = storage_client.get_bucket(databucket)
    source_blob = datafilebucket.blob(build_datafilename)  
    print(f"Moving {source_blob.name} to {archivebucket}")

    destination_bucket = storage_client.bucket(archivebucket)
    blob_copy = datafilebucket.copy_blob(source_blob, destination_bucket, build_datafilename)
    datafilebucket.delete_blob(build_datafilename)
    print(f"Datafile moved and replaced successfully")


# using pyspark dataframe

def process_spark_dataframe(data_bucket, files_list, config_path):

    try:
        
        df1 = spark.read.option("multiline","true").json("gs://"+config_path)
        print("config_path: ", "gs://"+config_path)
        #df1 = spark.read.option("multiline","true").json("gs://prj-dfdl-407-ierpf-d-407-dev-code/config/scripts/config.json")
        df_dict = df1.collect()[0].asDict()

        print("df_dict = ", df_dict) 
        print("Files in GCS Bucket: ", files_list)
        for files in df_dict:  
            
            for file in files_list:

                if file.find(files) > 0:

                    col_list = df_dict[files]
                    print("CSV file path :","gs://"+data_bucket+'/'+file )
                    df= spark.read.option("multiline","true").option("quoteAll","true").csv("gs://"+data_bucket+'/'+file, header=True)### 
            
                    actual_df = (reduce(lambda df, col_name: df.withColumn(col_name.lower(), validate_udf(col_name.lower())),col_list,df))
                    print("Output Path: ", file+'_output'+ files)
                    #actual_df.toPandas().to_csv('gs://'+data_bucket+'/'+file+'_output', index=False, quoting=csv.QUOTE_ALL)
                    par=actual_df.rdd.getNumPartitions()
                    print("number of partitions",par)
                    new_actual_df = actual_df.na.fill("")###
                    new_actual_df.coalesce(1).write.format("csv").option("header","true").option("quoteAll","true").option("quote","\u0010").mode("append").save('gs://'+data_bucket+'/'+file+'_output')
                
                    print("File Processed: "+files)
                    archive_datafiles(data_bucket, file , ARCHIVE_BUCKET)

                    print("file archived successfully", data_bucket,file)

    except Exception as e:

        if 'java.io.FileNotFoundException' in str(e):

            print(f"File not found error \n" f"{e}")        
           
        else:

           print("Something else--- ", e)


   
print("-----------------")

spark = SparkSession.builder.appName("SparkTrailing").getOrCreate()

process_spark_dataframe(BUCKET_NAME,files_list, CONFIG_PATH)
