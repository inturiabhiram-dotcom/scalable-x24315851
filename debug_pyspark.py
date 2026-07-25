from pyspark.sql import SparkSession
import boto3
import json

spark = SparkSession.builder.appName("debug").master("local[*]").getOrCreate()
s3 = boto3.client('s3', region_name='us-east-1')

# Read raw data
response = s3.list_objects_v2(Bucket="x24315851-scalable-s3", Prefix='raw/', MaxKeys=10)
trades = []
for obj in response['Contents']:
    resp = s3.get_object(Bucket="x24315851-scalable-s3", Key=obj['Key'])
    trades.append(json.loads(resp['Body'].read().decode('utf-8')))

# Create RDD and check side field
rdd = spark.sparkContext.parallelize(trades)

# Check if side exists
def check_side(trade):
    side = trade.get('side')
    return (side is not None, side)

results = rdd.map(check_side).collect()

print("Checking side field in RDD:")
for i, (has_side, side) in enumerate(results):
    print(f"  Trade {i+1}: has_side={has_side}, side='{side}'")
    
spark.stop()
