import time
import sys, string
import os
import socket
import boto3
import operator
import json
from datetime import datetime
from pyspark.sql import SparkSession

APP_NAME = "Gas_Guzzlers"
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
    
def trans_mapping_1(line):
    items = line.split(',')
    gas_price = float(items[9])
    date = time.strftime("%m/%Y",time.gmtime(int(items[11])))
    return (date, (gas_price, 1))

def trans_mapping_2(line):
    items = line.split(',')
    date = time.strftime("%m/%Y",time.gmtime(int(items[11])))
    gas = float(items[8])
    to_address = str(items[6])
    return (to_address, (date, gas))



def gas_guzzlers():
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
    
    transactions_clean_lines = tran_line.filter(good_line_tran)
    contracts_clean_lines = contr_line.filter(good_line_contract)
    
    # average gas price
    transactions_features = transactions_clean_lines.map(trans_mapping_1)
    transactions_reducing = transactions_features.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_gas_price = transactions_reducing.map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 
    
    # average gas used
    transactions_features_2 = transactions_clean_lines.map(trans_mapping_2)
    contracts_features = contracts_clean_lines.map(lambda x: (x.split(',')[0], 1))
    joins = transactions_features_2.join(contracts_features)
    mapp = joins.map(lambda x: (x[1][0][0], (x[1][0][1],x[1][1])))
    trans_reducing_1 = mapp.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_gas_used = trans_reducing_1.map(lambda a: (a[0], str(a[1][0]/a[1][1])))
    average_gas_used = average_gas_used.sortByKey(ascending = True)
    
    # save the result
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/average_gas_price.txt')
    my_result_object.put(Body=json.dumps(average_gas_price.take(100)))
    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum' + date_time + '/average_gas_used.txt')
    my_result_object.put(Body=json.dumps(average_gas_used.take(100)))
    
    spark.stop()


if __name__ == "__main__":
    print("start partD - Gas Guzzlers")
    gas_guzzlers()
    print("the end")
    
    
