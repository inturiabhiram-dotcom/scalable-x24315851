

# # batch_orchestrator.py - Fixed with region
# import boto3
# import json
# from datetime import datetime, timezone
# from collections import defaultdict
# import csv
# import io

# S3_BUCKET = "x24315851-scalable-s3"
# REGION = "us-east-1"

# class BatchOrchestrator:
#     def __init__(self):
#         self.s3 = boto3.client('s3', region_name=REGION)
#         # Use simplified processing instead of Hadoop
        
#     def run_batch(self):
#         """Run batch processing"""
#         print("🔹 Starting Batch Processing...")
        
#         # Get all speed files
#         response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=1000)
        
#         if 'Contents' not in response:
#             print("No data found!")
#             return
        
#         print(f"📥 Processing {len(response['Contents'])} files...")
        
#         # Aggregate data
#         products = defaultdict(lambda: {
#             'count': 0,
#             'price_sum': 0,
#             'max_price': float('-inf'),
#             'min_price': float('inf'),
#             'volume_sum': 0,
#             'buy_count': 0,
#             'sell_count': 0,
#             'latency_sum': 0
#         })
        
#         for obj in response['Contents']:
#             try:
#                 key = obj['Key']
#                 resp = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
#                 content = resp['Body'].read().decode('utf-8')
#                 trade = json.loads(content)
                
#                 product = trade.get('product', 'unknown')
#                 price = float(trade.get('price', 0))
#                 size = float(trade.get('size', 0))
#                 side = trade.get('side', 'unknown')
#                 latency = float(trade.get('latency_ms', 0))
                
#                 agg = products[product]
#                 agg['count'] += 1
#                 agg['price_sum'] += price
#                 agg['max_price'] = max(agg['max_price'], price)
#                 agg['min_price'] = min(agg['min_price'], price)
#                 agg['volume_sum'] += size
#                 if side == 'buy':
#                     agg['buy_count'] += 1
#                 elif side == 'sell':
#                     agg['sell_count'] += 1
#                 agg['latency_sum'] += latency
                
#             except Exception as e:
#                 print(f"Error processing {key}: {e}")
        
#         # Generate batch summary
#         timestamp = datetime.now(timezone.utc).isoformat()
#         batch_results = []
        
#         print("\n📊 Batch Results:")
#         for product, agg in products.items():
#             avg_price = agg['price_sum'] / agg['count'] if agg['count'] > 0 else 0
#             avg_latency = agg['latency_sum'] / agg['count'] if agg['count'] > 0 else 0
            
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
        
#         # Save to S3
#         print("\n💾 Saving batch results...")
        
#         self.s3.put_object(
#             Bucket=S3_BUCKET,
#             Key='batch/batch_summary.json',
#             Body=json.dumps(batch_results, indent=2),
#             ContentType='application/json'
#         )
        
#         # Save as CSV
#         if batch_results:
#             csv_buffer = io.StringIO()
#             writer = csv.DictWriter(csv_buffer, fieldnames=batch_results[0].keys())
#             writer.writeheader()
#             writer.writerows(batch_results)
#             self.s3.put_object(
#                 Bucket=S3_BUCKET,
#                 Key='batch/batch_summary_latest.csv',
#                 Body=csv_buffer.getvalue(),
#                 ContentType='text/csv'
#             )
        
#         print("✅ Batch processing complete!")
#         return batch_results

# if __name__ == "__main__":
#     orchestrator = BatchOrchestrator()
#     orchestrator.run_batch()




# batch_kinesis.py - Batch Processing Reading Directly from Kinesis
import boto3
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import csv
import io
import time

S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"
STREAM_NAME = "x24315851-kinesis-stream"

class BatchProcessor:
    def __init__(self):
        self.s3 = boto3.client('s3', region_name=REGION)
        self.kinesis = boto3.client('kinesis', region_name=REGION)
        
    def get_all_records_from_kinesis(self, max_records=10000):
        """Read ALL records from Kinesis (full history)"""
        print("📥 Reading data directly from Kinesis...")
        
        all_trades = []
        processed = 0
        
        try:
            # Get all shards
            response = self.kinesis.list_shards(StreamName=STREAM_NAME)
            shards = response['Shards']
            
            print(f"   Found {len(shards)} shards")
            
            for shard in shards:
                shard_id = shard['ShardId']
                print(f"   Reading from shard: {shard_id}")
                
                # Get shard iterator - read from TRIM_HORIZON (all records)
                iterator_response = self.kinesis.get_shard_iterator(
                    StreamName=STREAM_NAME,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'  # Read ALL records from beginning
                )
                
                shard_iterator = iterator_response['ShardIterator']
                records_read = 0
                
                while True:
                    # Get records from Kinesis
                    records_response = self.kinesis.get_records(
                        ShardIterator=shard_iterator,
                        Limit=100  # Max 100 records per call
                    )
                    
                    records = records_response.get('Records', [])
                    
                    if not records:
                        # No more records in this shard
                        break
                    
                    for record in records:
                        try:
                            # Parse the trade data
                            trade_data = json.loads(record['Data'])
                            # Add processing timestamp
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
                    
                    # Get next shard iterator
                    shard_iterator = records_response.get('NextShardIterator')
                    if not shard_iterator:
                        break
                    
                    # Add small delay to avoid throttling
                    time.sleep(0.1)
                
                print(f"   Shard {shard_id}: read {records_read} records")
                
                if processed >= max_records:
                    break
            
            print(f"✅ Total records read from Kinesis: {len(all_trades)}")
            return all_trades
            
        except Exception as e:
            print(f"❌ Error reading from Kinesis: {e}")
            return []

    def process_batch(self, trades):
        """Process trades using MapReduce-style aggregation"""
        if not trades:
            print("No data to process!")
            return []
        
        print(f"\n📊 Processing {len(trades)} trades...")
        
        # Aggregate data (MapReduce style)
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
        
        # MAP phase: Extract key-value pairs
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
                print(f"Error processing trade: {e}")
        
        # REDUCE phase: Generate summaries
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
        
        return batch_results

    def save_results(self, results):
        """Save batch results to S3"""
        if not results:
            print("No results to save!")
            return
        
        timestamp = datetime.now(timezone.utc).isoformat()
        
        print("\n💾 Saving batch results to S3...")
        
        # Save as JSON
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key='batch/batch_kinesis.json',
            Body=json.dumps(results, indent=2),
            ContentType='application/json'
        )
        print("  ✅ JSON saved to batch/batch_kinesis.json")
        
        # Save as CSV for Athena
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=f'batch/batch_kinesis_{timestamp.split("T")[0]}.csv',
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"  ✅ CSV saved to batch/batch_kinesis_{timestamp.split('T')[0]}.csv")
        
        # Also save as latest
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key='batch/batch_summary.json',
            Body=json.dumps(results, indent=2),
            ContentType='application/json'
        )
        print("  ✅ Updated batch/batch_summary.json")

    def run_batch(self, max_records=10000):
        """Main batch processing function"""
        print("="*60)
        print("🔹 Starting Batch Processing from Kinesis")
        print("="*60)
        
        # Step 1: Read data from Kinesis
        trades = self.get_all_records_from_kinesis(max_records)
        
        if not trades:
            print("❌ No data found in Kinesis!")
            return
        
        # Step 2: Process batch
        results = self.process_batch(trades)
        
        if not results:
            print("❌ No results generated!")
            return
        
        # Step 3: Save results
        self.save_results(results)
        
        print("\n" + "="*60)
        print(f"✅ Batch processing complete! Processed {len(results)} products")
        print(f"📊 Total trades processed: {len(trades)}")
        print("="*60)
        
        return results

# ============================================
# BENCHMARK VERSION - Measures Performance
# ============================================

def run_benchmark_batch(workers=4):
    """Run batch with benchmark"""
    import time
    from concurrent.futures import ThreadPoolExecutor
    
    print("="*60)
    print("📊 Batch Processing Benchmark")
    print("="*60)
    
    start_time = time.time()
    
    processor = BatchProcessor()
    trades = processor.get_all_records_from_kinesis(1000)
    
    if not trades:
        print("No data available!")
        return
    
    # Sequential processing
    print("\n🔹 Sequential Processing...")
    seq_start = time.time()
    
    # Process sequentially
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
    
    seq_time = time.time() - seq_start
    print(f"   Sequential time: {seq_time:.2f}s")
    
    # Parallel processing
    print(f"\n🔹 Parallel Processing ({workers} workers)...")
    par_start = time.time()
    
    def process_chunk(chunk):
        local_products = defaultdict(lambda: {
            'count': 0,
            'price_sum': 0,
            'max_price': float('-inf'),
            'min_price': float('inf'),
            'volume_sum': 0,
            'buy_count': 0,
            'sell_count': 0,
            'latency_sum': 0
        })
        
        for trade in chunk:
            try:
                product = trade.get('product', 'unknown')
                price = float(trade.get('price', 0))
                size = float(trade.get('size', 0))
                side = trade.get('side', 'unknown')
                latency = float(trade.get('latency_ms', 0))
                
                agg = local_products[product]
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
        
        return local_products
    
    # Split data into chunks
    chunk_size = max(1, len(trades) // workers)
    chunks = [trades[i:i+chunk_size] for i in range(0, len(trades), chunk_size)]
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        all_results = []
        for future in futures:
            try:
                all_results.append(future.result(timeout=60))
            except:
                pass
    
    # Merge results
    merged = defaultdict(lambda: {
        'count': 0,
        'price_sum': 0,
        'max_price': float('-inf'),
        'min_price': float('inf'),
        'volume_sum': 0,
        'buy_count': 0,
        'sell_count': 0,
        'latency_sum': 0
    })
    
    for result in all_results:
        for product, agg in result.items():
            merged[product]['count'] += agg['count']
            merged[product]['price_sum'] += agg['price_sum']
            merged[product]['max_price'] = max(merged[product]['max_price'], agg['max_price'])
            merged[product]['min_price'] = min(merged[product]['min_price'], agg['min_price'])
            merged[product]['volume_sum'] += agg['volume_sum']
            merged[product]['buy_count'] += agg['buy_count']
            merged[product]['sell_count'] += agg['sell_count']
            merged[product]['latency_sum'] += agg['latency_sum']
    
    par_time = time.time() - par_start
    print(f"   Parallel time: {par_time:.2f}s")
    
    # Speedup
    speedup = seq_time / par_time if par_time > 0 else 0
    print(f"\n📈 Speedup: {speedup:.2f}x")
    
    total_time = time.time() - start_time
    print(f"\n✅ Total benchmark time: {total_time:.2f}s")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'benchmark':
        run_benchmark_batch(4)
    else:
        # Run normal batch processing
        processor = BatchProcessor()
        processor.run_batch()
