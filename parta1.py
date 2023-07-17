import time
import sys, string
import os
import socket
import boto3
import operator
import json
from datetime import datetime
from pyspark.sql import SparkSession

APP_NAME = "Time_Analysis"
FILE_PATH = "/ECS765/ethereum-parvulus/transactions.csv"

def good_line_tran(line):
    """
    check the line 
    """
    try:
        fields = line.split(',')
        if len(fields) != 15:
            return False
        int(fields[11])
        return True
    except:
        return False

def time_analysis():
    spark = SparkSession\
        .builder\
        .appName(APP_NAME)\
        .getOrCreate()
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    path = "s3a://" + s3_data_repository_bucket + FILE_PATH
    lines = spark.sparkContext.textFile(path)
    
    # time_analysis
    clean_lines = lines.filter(good_line_tran) # filter the transactions
    timestamp = clean_lines.map(lambda line: int(line.split(',')[11]))
    month_years = timestamp.map(lambda t: (time.strftime("%m-%Y", time.gmtime(t)), 1))
    result = month_years.reduceByKey(operator.add)
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/time_analysis_total.txt')
    my_result_object.put(Body=json.dumps(result.take(100)))
    print(result.take(100))
    
    spark.stop()

    
    
if __name__ == "__main__":
    print("start partA Question1")
    time_analysis()
    print("the end")
    
    
    
    
    

    
    
