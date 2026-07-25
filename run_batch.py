# # run_batch.py
# import boto3
# import json
# from datetime import datetime, timezone
# from collections import defaultdict
# import csv
# import io

# S3_BUCKET = "x24315851-scalable-s3"
# REGION = "us-east-1"

# def run_batch():
#     print("🔹 Starting Batch Processing...")
#     s3 = boto3.client('s3', region_name=REGION)
    
#     # Get all speed files
#     response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=3000)
    
#     if 'Contents' not in response:
#         print("No data found!")
#         return
    
#     print(f"📥 Processing {len(response['Contents'])} files...")
    
#     # Aggregate data
#     products = defaultdict(lambda: {
#         'count': 0,
#         'price_sum': 0,
#         'max_price': float('-inf'),
#         'min_price': float('inf'),
#         'volume_sum': 0,
#         'buy_count': 0,
#         'sell_count': 0,
#         'latency_sum': 0
#     })
    
#     for obj in response['Contents']:
#         try:
#             key = obj['Key']
#             resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
#             content = resp['Body'].read().decode('utf-8')
#             trade = json.loads(content)
            
#             product = trade.get('product', 'unknown')
#             price = float(trade.get('price', 0))
#             size = float(trade.get('size', 0))
#             side = trade.get('side', 'unknown')
#             latency = float(trade.get('latency_ms', 0))
            
#             agg = products[product]
#             agg['count'] += 1
#             agg['price_sum'] += price
#             agg['max_price'] = max(agg['max_price'], price)
#             agg['min_price'] = min(agg['min_price'], price)
#             agg['volume_sum'] += size
#             if side == 'buy':
#                 agg['buy_count'] += 1
#             elif side == 'sell':
#                 agg['sell_count'] += 1
#             agg['latency_sum'] += latency
            
#         except Exception as e:
#             pass
    
#     # Generate batch summary
#     timestamp = datetime.now(timezone.utc).isoformat()
#     batch_results = []
    
#     print("\n📊 Batch Results:")
#     for product, agg in products.items():
#         if agg['count'] > 0:
#             avg_price = agg['price_sum'] / agg['count']
#             avg_latency = agg['latency_sum'] / agg['count']
            
#             summary = {
#                 'generated_at': timestamp,
#                 'product': product,
#                 'total_trades': agg['count'],
#                 'average_price': round(avg_price, 2),
#                 'maximum_price': round(agg['max_price'], 2),
#                 'minimum_price': round(agg['min_price'], 2),
#                 'total_volume': round(agg['volume_sum'], 4),
#                 'buy_trades': agg['buy_count'],
#                 'sell_trades': agg['sell_count'],
#                 'avg_latency_ms': round(avg_latency, 2)
#             }
#             batch_results.append(summary)
#             print(f"  ✅ {product}: {agg['count']} trades, avg ${avg_price:.2f}")
    
#     # Save to S3
#     print("\n💾 Saving batch results...")
    
#     s3.put_object(
#         Bucket=S3_BUCKET,
#         Key='batch/batch_summary.json',
#         Body=json.dumps(batch_results, indent=2),
#         ContentType='application/json'
#     )
    
#     # Save as CSV
#     if batch_results:
#         csv_buffer = io.StringIO()
#         writer = csv.DictWriter(csv_buffer, fieldnames=batch_results[0].keys())
#         writer.writeheader()
#         writer.writerows(batch_results)
#         s3.put_object(
#             Bucket=S3_BUCKET,
#             Key='batch/batch_summary_latest.csv',
#             Body=csv_buffer.getvalue(),
#             ContentType='text/csv'
#         )
    
#     print(f"✅ Batch processing complete! Processed {len(products)} products")
#     return batch_results

# if __name__ == "__main__":
#     run_batch()


# run_batch.py - Simple batch processor reading from Kinesis
import boto3
import json
from datetime import datetime, timezone
from collections import defaultdict
import csv
import io
import time

S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"
STREAM_NAME = "x24315851-kinesis-stream"

def get_all_records_from_kinesis(max_records=10000):
    """Read ALL records from Kinesis (full history)"""
    print("📥 Reading data directly from Kinesis...")
    
    kinesis = boto3.client('kinesis', region_name=REGION)
    all_trades = []
    processed = 0
    
    try:
        # Get all shards
        response = kinesis.list_shards(StreamName=STREAM_NAME)
        shards = response['Shards']
        
        print(f"   Found {len(shards)} shards")
        
        for shard in shards:
            shard_id = shard['ShardId']
            print(f"   Reading from shard: {shard_id}")
            
            # Get shard iterator - read from TRIM_HORIZON (all records)
            iterator_response = kinesis.get_shard_iterator(
                StreamName=STREAM_NAME,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'  # Read ALL records from beginning
            )
            
            shard_iterator = iterator_response['ShardIterator']
            records_read = 0
            
            while True:
                records_response = kinesis.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                
                records = records_response.get('Records', [])
                
                if not records:
                    break
                
                for record in records:
                    try:
                        trade_data = json.loads(record['Data'])
                        trade_data['batch_processed_at'] = datetime.now(timezone.utc).isoformat()
                        all_trades.append(trade_data)
                        records_read += 1
                        processed += 1
                        
                        if processed % 100 == 0:
                            print(f"      Processed {processed} records...")
                            
                        if processed >= max_records:
                            break
                            
                    except Exception as e:
                        print(f"      Error parsing record: {e}")
                
                if processed >= max_records:
                    break
                
                shard_iterator = records_response.get('NextShardIterator')
                if not shard_iterator:
                    break
                
                time.sleep(0.1)
            
            print(f"   Shard {shard_id}: read {records_read} records")
            
            if processed >= max_records:
                break
        
        print(f"✅ Total records read from Kinesis: {len(all_trades)}")
        return all_trades
        
    except Exception as e:
        print(f"❌ Error reading from Kinesis: {e}")
        return []

def run_batch():
    """Main batch processing function"""
    print("="*60)
    print("🔹 Starting Batch Processing from Kinesis")
    print("="*60)
    
    s3 = boto3.client('s3', region_name=REGION)
    
    # Step 1: Read data from Kinesis
    trades = get_all_records_from_kinesis(10000)
    
    if not trades:
        print("❌ No data found in Kinesis!")
        return
    
    print(f"\n📊 Processing {len(trades)} trades...")
    
    # Aggregate data
    products = defaultdict(lambda: {
        'count': 0,
        'price_sum': 0,
        'max_price': float('-inf'),
        'min_price': float('inf'),
        'volume_sum': 0,
        'buy_count': 0,
        'sell_count': 0,
        'latency_sum': 0
    })
    
    for trade in trades:
        try:
            product = trade.get('product', 'unknown')
            price = float(trade.get('price', 0))
            size = float(trade.get('size', 0))
            side = trade.get('side', 'unknown')
            latency = float(trade.get('latency_ms', 0))
            
            agg = products[product]
            agg['count'] += 1
            agg['price_sum'] += price
            agg['max_price'] = max(agg['max_price'], price)
            agg['min_price'] = min(agg['min_price'], price)
            agg['volume_sum'] += size
            if side == 'buy':
                agg['buy_count'] += 1
            elif side == 'sell':
                agg['sell_count'] += 1
            agg['latency_sum'] += latency
            
        except Exception as e:
            pass
    
    # Generate batch summary
    timestamp = datetime.now(timezone.utc).isoformat()
    batch_results = []
    
    print("\n📊 Batch Results:")
    for product, agg in products.items():
        if agg['count'] > 0:
            avg_price = agg['price_sum'] / agg['count']
            avg_latency = agg['latency_sum'] / agg['count'] if agg['count'] > 0 else 0
            
            summary = {
                'generated_at': timestamp,
                'product': product,
                'total_trades': agg['count'],
                'average_price': round(avg_price, 2),
                'maximum_price': round(agg['max_price'], 2),
                'minimum_price': round(agg['min_price'], 2),
                'total_volume': round(agg['volume_sum'], 4),
                'buy_trades': agg['buy_count'],
                'sell_trades': agg['sell_count'],
                'avg_latency_ms': round(avg_latency, 2)
            }
            batch_results.append(summary)
            print(f"  ✅ {product}: {agg['count']} trades, avg ${avg_price:.2f}")
    
    # Save to S3
    print("\n💾 Saving batch results...")
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key='batch/batch_summary.json',
        Body=json.dumps(batch_results, indent=2),
        ContentType='application/json'
    )
    
    # Save as CSV
    if batch_results:
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=batch_results[0].keys())
        writer.writeheader()
        writer.writerows(batch_results)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key='batch/batch_summary_latest.csv',
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
    
    print(f"\n✅ Batch processing complete! Processed {len(products)} products")
    print(f"📊 Total trades processed: {len(trades)}")
    return batch_results

if __name__ == "__main__":
    run_batch()
