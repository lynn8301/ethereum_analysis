import time
import sys, string
import os
import socket
import boto3
import operator
import json
from datetime import datetime
from pyspark.sql import SparkSession

APP_NAME = "Top_Ten_Most_Popular_Services"
TRANSACTION_FILE_PATH = "/ECS765/ethereum-parvulus/transactions.csv"
CONTRACT_FILE_PATH = "/ECS765/ethereum-parvulus/contracts.csv"

def good_line_tran(line):
    """
    check the line for transaction
    """
    try:
        fields = line.split(',')
        if len(fields) != 15:
            return False
        int(fields[11])
        return True
    except:
        return False
    
def good_line_contract(line):
    """
    check the line for contract
    """
    try:
        fields = line.split(',')
        if len(fields) != 6:
            return False 
        else:
            return True
    except:
        return False


def top_ten_most_popular_services():
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
    
    # read the transaction file
    tran_path = "s3a://" + s3_data_repository_bucket + TRANSACTION_FILE_PATH
    tran_line = spark.sparkContext.textFile(tran_path)
    
    # read the contract file
    contr_path = "s3a://" + s3_data_repository_bucket + CONTRACT_FILE_PATH
    contr_line = spark.sparkContext.textFile(contr_path)
    
    transactions = tran_line.filter(good_line_tran)
    contracts = contr_line.filter(good_line_contract)
    
    # Top Ten Most Popular Services
    transation_value = transactions.map(lambda line: (line.split(',')[6], int(line.split(',')[7]))) # to_address and value
    contract_value = contracts.map(lambda line: (line.split(',')[0], 1)) # address
    trans_reducing = transation_value.reduceByKey(lambda x, y: x + y)
    join_address_and_to_address = trans_reducing.join(contract_value)
    address_value = join_address_and_to_address.map(lambda address:(address[0], address[1][0]))
    result = address_value.takeOrdered(10, key = lambda x: -1 * x[1])
    
    # save the result
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/top_ten_popular_service.txt')
    my_result_object.put(Body=json.dumps(result))
    print(result)
    
    spark.stop()


if __name__ == "__main__":
    print("start partB")
    top_ten_most_popular_services()
    print("the end")
    
    
    
    
    

    
    
