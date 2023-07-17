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
        float(fields[7]) # value
        int(fields[11]) # block_timestamp
        return True
    except:
        return False

def mapper(line):
    data = line.split(',')
    date = time.strftime("%m-%Y", time.gmtime(int(data[11])))
    value = float(data[7])
    
    return (date, (value, 1))

def time_analysis():
    spark = SparkSession\
        .builder\
        .appName(APP_NAME)\
        .getOrCreate()
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
    
    clean_lines = lines.filter(good_line_tran)
    date_values = clean_lines.map(mapper)
    counts = date_values.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    print('counts: {}'.format(counts))
    avg_transactions = counts.map(lambda avg: (avg[0], str(avg[1][0]/avg[1][1]))) 
    result = avg_transactions.map(lambda x: ','.join(str(i) for i in x))
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/time_analysis_average.txt')
    my_result_object.put(Body=json.dumps(result.take(100)))
    print(result.take(100))
    
    spark.stop()

    
    
if __name__ == "__main__":
    print("start partA Question1")
    time_analysis()
    print("the end")
    
    
    
    
    

    
    
