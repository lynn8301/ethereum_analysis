import time
import sys, string
import os
import socket
import boto3
import operator
import json
from datetime import datetime
from pyspark.sql import SparkSession

APP_NAME = "Top_Ten_Most_Active_Miners "
BLOCKS_FILE_PATH = "/ECS765/ethereum-parvulus/blocks.csv"

def good_line_blocks(line):
    """
    check the line for blocks
    """
    try:
        fields = line.split(',')
        if len(fields) == 19 and fields[1] != 'hash':
            return True
        else:
            return False
    except:
        return False
    
def blocks_mapper(line):
    try:
        fields = line.split(',')
        miner = str(fields[9])
        size = int(fields[12])
        return (miner, size)
    except:
        return(0, 1)

def top_ten_most_active_miners():
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
    
    # read the blocks file
    blocks_path = "s3a://" + s3_data_repository_bucket + BLOCKS_FILE_PATH
    lines = spark.sparkContext.textFile(blocks_path)
     
    # Top Ten Most Active Miners    
    clean_lines = lines.filter(good_line_blocks)
    features = clean_lines.map(blocks_mapper)
    blocks_reducing = features.reduceByKey(operator.add)
    top10 = blocks_reducing.takeOrdered(10, key=lambda x: -x[1])
    
    # save the result
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/top_ten_most_active_miners.txt')
    my_result_object.put(Body = json.dumps(top10))
    print(top10)
    
    spark.stop()


if __name__ == "__main__":
    print("start partC")
    top_ten_most_active_miners()
    print("the end")
    
    
    
    
    

    
    
