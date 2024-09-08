

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *
import pyspark.sql.functions as sqlf
import sys
from google.cloud import bigquery
import json
from google.cloud import storage
from datetime import datetime

PROJECT_ID = sys.argv[1] # 'prj-dfad-7ingtool-d-8ab3'
schema_bucket = sys.argv[2] # 'prj-dfad-7ingtool-d-8ab3-7ingtool-code-dev'
fixed_width_schema = sys.argv[3] # "fixedwidth.json"
databucket = sys.argv[4] # 'prj-dfad-7ingtool-d-8ab3-7ingtoolbq-dev'
fixed_width_file_path = sys.argv[5] # "mainframe_copybook/20220713000000/rate_maker/20220914RateMakerDumpFile0001_20220915000545"
archivebucket = sys.argv[6] # 'prj-dfad-7ingtool-d-8ab3-7ingtoolbq-archive-dev'

sparkBuilder = SparkSession.builder.appName("GCS_to_BQ")
spark = sparkBuilder.getOrCreate()
storage_client = storage.Client(project=PROJECT_ID)

def read_fixed_width_file(schema_bucket,fixed_width_schema,fixed_width_file):
srcbucket = storage_client.get_bucket(schema_bucket)
blob = srcbucket.blob(fixed_width_schema)
schema_json = json.loads(blob.download_as_string())
src_folder = spark.read.text(fixed_width_file).withColumn("file_names", input_file_name())
df = src_folder.withColumn("src_file_name",split(col("file_names"),"/").getItem(7))
df = df.drop("file_names")
print(df)
head_list = [x[0] for x in df.head(1)]
print(head_list)
head = df.head(2)[0][0]
print("head",head)
tail_list = [x[0] for x in df.tail(1)]
print(tail_list)
tail = df.tail(1)[0][0]
print("tail",tail)
rows_before = df.count()
print("rows_before",rows_before)
txt = df.filter(~col("value").isin(head_list))
txt1 = txt.filter(~col("value").isin(tail_list))
rows_after = txt1.count()
print("rows_after",rows_after)
df2 = txt1
return df2,schema_json

def extract_colnames(df2,schema_json,build_outputfilepath):
print(df2,schema_json,build_outputfilepath)
for k, v in schema_json.items():
col = k.replace(' ','').strip()
df2 = df2.withColumn(col, df2.value.substr(v[0],v[1]))
df2 = df2.drop("value")
df2.show(10)
# df2.coalesce(1).write.format("json").mode("append").save(build_outputfilepath)
df2.coalesce(1).write.format("orc").mode("append").save(build_outputfilepath)

def archive_datafiles(databucket,build_datafilename,archivebucket):
datafilebucket = storage_client.get_bucket(databucket)
source_blob = datafilebucket.blob(build_datafilename)
print(f"Moving {source_blob.name} to {archivebucket}")

destination_bucket = storage_client.bucket(archivebucket)
blob_copy = datafilebucket.copy_blob(source_blob, destination_bucket, build_datafilename)
datafilebucket.delete_blob(build_datafilename)
print(f"Datafile moved and replaced successfully")

def main():
print(f"Processing fixed width file")
blobs = storage_client.list_blobs(databucket,prefix=fixed_width_file_path,delimiter="/")
print(blobs)
for blob in blobs:
if str(blob.name).endswith("/") == False:
print(f"Configuration: {schema_bucket} , {fixed_width_schema}, {blob.name}")
fixed_width_filename = "gs://"+databucket+"/"+blob.name
build_outputfilepath = "gs://"+databucket+"/"+fixed_width_file_path
fixed_width_df,schema_json = read_fixed_width_file(schema_bucket,fixed_width_schema,fixed_width_filename)
print(f"Successfully converted the fixed width file {fixed_width_filename} to dataframe")
print(f"Preparing schema")
extract_colnames(fixed_width_df,schema_json,build_outputfilepath)
print(f"Successfully transformed schema")
print(f"Archiving datafiles {blob.name}")
archive_datafiles(databucket,blob.name,archivebucket)
print("Done processing fixed width files")

main()








