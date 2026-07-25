



# # mapreduce_complete.py - PySpark MapReduce Reading from S3/raw/ (FULLY FIXED)
# from pyspark.sql import SparkSession
# import boto3
# from datetime import datetime, timezone
# import json
# import time
# import schedule
# import signal
# import sys

# class PySparkBatchProcessor:
#     def __init__(self):
#         # Create Spark session (local mode)
#         self.spark = SparkSession.builder \
#             .appName("ScalableCloudAnalytics") \
#             .config("spark.master", "local[*]") \
#             .config("spark.executor.memory", "2g") \
#             .config("spark.driver.memory", "2g") \
#             .config("spark.sql.shuffle.partitions", "8") \
#             .getOrCreate()
        
#         self.s3 = boto3.client('s3', region_name='us-east-1')
#         self.S3_BUCKET = "x24315851-scalable-s3"
#         self.running = True
#         self.last_batch_time = None
        
#         # Configuration - Batch interval in minutes
#         self.BATCH_INTERVAL_MINUTES = 1
        
#         print(f"📋 Batch Configuration:")
#         print(f"   Interval: {self.BATCH_INTERVAL_MINUTES} minutes")
#         print(f"   Source: S3/raw/ (all historical data)")
        
#     def read_data_from_s3(self):
#         """Read data directly from S3/raw/"""
#         print(f"\n📥 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reading data from S3/raw/...")
        
#         all_trades = []
#         processed = 0
#         start_time = time.time()
        
#         try:
#             response = self.s3.list_objects_v2(
#                 Bucket=self.S3_BUCKET, 
#                 Prefix='raw/', 
#                 MaxKeys=100
#             )
            
#             if 'Contents' not in response:
#                 print("❌ No data found in S3/raw/!")
#                 return self.spark.sparkContext.parallelize([])
            
#             total_files = len(response['Contents'])
#             print(f"   Found {total_files} raw files")
            
#             for obj in response['Contents']:
#                 try:
#                     key = obj['Key']
#                     resp = self.s3.get_object(Bucket=self.S3_BUCKET, Key=key)
#                     content = resp['Body'].read().decode('utf-8')
#                     trade = json.loads(content)
                    
#                     if processed == 0:
#                         print(f"   Sample trade: {json.dumps(trade, indent=2)}")
                    
#                     all_trades.append(trade)
#                     processed += 1
                    
#                     if processed % 50 == 0:
#                         print(f"      Processed {processed} files...")
                        
#                 except Exception as e:
#                     print(f"      Error reading {key}: {e}")
            
#             elapsed = time.time() - start_time
#             print(f"✅ Total records read from S3: {len(all_trades)} in {elapsed:.2f}s")
            
#         except Exception as e:
#             print(f"❌ Error reading from S3: {e}")
        
#         rdd = self.spark.sparkContext.parallelize(all_trades)
#         return rdd
    
#     def process_batch(self):
#         """Process batch using PySpark MapReduce"""
#         print("\n" + "="*60)
#         print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting PySpark MapReduce Batch")
#         print("   (Reading from S3/raw/ - Full History)")
#         print("="*60)
        
#         rdd = self.read_data_from_s3()
        
#         if rdd.isEmpty():
#             print("❌ No data found in S3/raw/!")
#             return None
        
#         start_time = time.time()
#         total_trades = rdd.count()
#         print(f"\n📊 Processing {total_trades} trades using MapReduce...")
        
#         # MAP phase: Extract key-value pairs
#         def mapper(trade):
#             product = trade.get('product', 'unknown')
#             price = float(trade.get('price', 0))
#             size = float(trade.get('size', 0))
#             side = trade.get('side', 'unknown')
#             latency = float(trade.get('latency_ms', 0))
            
#             if product == 'unknown' or price == 0:
#                 return []
            
#             # Key format: product_metric (e.g., BTC-USD_count)
#             return [
#                 (f"{product}_count", 1),
#                 (f"{product}_price_sum", price),
#                 (f"{product}_price_max", price),
#                 (f"{product}_price_min", price),
#                 (f"{product}_volume", size),
#                 (f"{product}_latency_sum", latency),
#                 (f"{product}_latency_count", 1 if latency > 0 else 0),
#                 (f"{product}_{side}_count", 1 if side in ['buy', 'sell'] else 0)
#             ]
        
#         map_start = time.time()
#         mapped = rdd.flatMap(mapper)
#         map_count = mapped.count()
#         print(f"   Map phase: {map_count} key-value pairs generated in {time.time() - map_start:.2f}s")
        
#         if map_count == 0:
#             print("❌ No key-value pairs generated! Check data format.")
#             return None
        
#         # SHUFFLE & REDUCE phase
#         reduce_start = time.time()
#         reduced = mapped.reduceByKey(lambda a, b: a + b)
#         results = reduced.collect()
#         print(f"   Reduce phase: {len(results)} keys aggregated in {time.time() - reduce_start:.2f}s")
        
#         # Parse results - FIXED: Better parsing
#         products = {}
        
#         # Define which metrics to look for
#         metrics = ['count', 'price_sum', 'price_max', 'price_min', 'volume', 
#                    'latency_sum', 'latency_count', 'buy_count', 'sell_count']
        
#         for key, value in results:
#             val = float(value) if isinstance(value, (int, float)) else value
            
#             # Find which metric this is
#             metric = None
#             product = None
            
#             for m in metrics:
#                 if key.endswith(f"_{m}"):
#                     metric = m
#                     product = key[:-len(f"_{m}")]
#                     break
            
#             if product is None or metric is None:
#                 continue
            
#             # Initialize product if not exists
#             if product not in products:
#                 products[product] = {
#                     'count': 0, 
#                     'price_sum': 0.0, 
#                     'price_max': float('-inf'), 
#                     'price_min': float('inf'), 
#                     'volume': 0.0, 
#                     'latency_sum': 0.0,
#                     'latency_count': 0, 
#                     'buy_count': 0, 
#                     'sell_count': 0
#                 }
            
#             # Update the specific metric
#             if metric == 'price_sum':
#                 products[product]['price_sum'] = float(val)
#             elif metric == 'count':
#                 products[product]['count'] = int(val)
#             elif metric == 'price_max':
#                 products[product]['price_max'] = float(val)
#             elif metric == 'price_min':
#                 products[product]['price_min'] = float(val)
#             elif metric == 'volume':
#                 products[product]['volume'] = float(val)
#             elif metric == 'latency_sum':
#                 products[product]['latency_sum'] = float(val)
#             elif metric == 'latency_count':
#                 products[product]['latency_count'] = int(val)
#             elif metric == 'buy_count':
#                 products[product]['buy_count'] = int(val)
#             elif metric == 'sell_count':
#                 products[product]['sell_count'] = int(val)
        
#         # Generate final results
#         batch_results = []
#         for product, agg in products.items():
#             if agg['count'] > 0 and agg['price_sum'] > 0:
#                 avg_price = agg['price_sum'] / agg['count']
#                 avg_latency = agg['latency_sum'] / agg['latency_count'] if agg['latency_count'] > 0 else 0
                
#                 batch_results.append({
#                     'product': product,
#                     'total_trades': agg['count'],
#                     'average_price': round(avg_price, 2),
#                     'maximum_price': round(agg['price_max'], 2) if agg['price_max'] != float('-inf') else 0,
#                     'minimum_price': round(agg['price_min'], 2) if agg['price_min'] != float('inf') else 0,
#                     'total_volume': round(agg['volume'], 4),
#                     'buy_trades': agg['buy_count'],
#                     'sell_trades': agg['sell_count'],
#                     'avg_latency_ms': round(avg_latency, 2)
#                 })
#                 print(f"   {product}: {agg['count']} trades, avg ${avg_price:.2f}")
        
#         # Save results
#         if batch_results:
#             self.save_results(batch_results, datetime.now(timezone.utc).isoformat())
#             print(f"\n✅ Generated {len(batch_results)} product summaries")
#         else:
#             print("⚠️ No results generated!")
#             # Print sample keys for debugging
#             print("   Sample keys from reduce phase:")
#             for key, val in results[:5]:
#                 print(f"      {key}: {val}")
#             return None
        
#         total_time = time.time() - start_time
#         self.last_batch_time = datetime.now(timezone.utc)
        
#         print("\n" + "="*60)
#         print(f"✅ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Batch Processing Complete!")
#         print(f"   Products: {len(batch_results)}")
#         print(f"   Total Trades: {sum(r['total_trades'] for r in batch_results)}")
#         print(f"   Total Time: {total_time:.2f}s")
#         print("="*60)
        
#         return batch_results
    
#     def save_results(self, results, timestamp):
#         """Save batch results to S3"""
#         import csv
#         import io
        
#         if not results:
#             return
        
#         # Save JSON
#         self.s3.put_object(
#             Bucket=self.S3_BUCKET,
#             Key='batch/batch_mapreduce.json',
#             Body=json.dumps(results, indent=2),
#             ContentType='application/json'
#         )
#         print("  ✅ JSON saved to batch/batch_mapreduce.json")
        
#         # Also save as batch_summary.json (for dashboard)
#         self.s3.put_object(
#             Bucket=self.S3_BUCKET,
#             Key='batch/batch_summary.json',
#             Body=json.dumps(results, indent=2),
#             ContentType='application/json'
#         )
#         print("  ✅ Updated batch/batch_summary.json")
        
#         # Save CSV with timestamp
#         csv_buffer = io.StringIO()
#         writer = csv.DictWriter(csv_buffer, fieldnames=results[0].keys())
#         writer.writeheader()
#         writer.writerows(results)
#         self.s3.put_object(
#             Bucket=self.S3_BUCKET,
#             Key=f'batch/batch_mapreduce_{timestamp.split("T")[0]}_{timestamp.split("T")[1].split(".")[0].replace(":", "-")}.csv',
#             Body=csv_buffer.getvalue(),
#             ContentType='text/csv'
#         )
#         print(f"  ✅ CSV saved to S3")
    
#     def run_scheduled(self):
#         """Run batch processing on a schedule"""
#         print("\n" + "="*60)
#         print(f"🔄 Scheduled Batch Processing Started")
#         print(f"   Interval: Every {self.BATCH_INTERVAL_MINUTES} minutes")
#         print(f"   Source: S3/raw/ (All historical data)")
#         print(f"   Press Ctrl+C to stop")
#         print("="*60)
        
#         print("\n🔄 Running initial batch...")
#         result = self.process_batch()
        
#         if result:
#             print("\n✅ Initial batch completed successfully! Scheduling next runs...")
#         else:
#             print("\n⚠️ Initial batch failed. Will retry on next schedule...")
        
#         schedule.every(self.BATCH_INTERVAL_MINUTES).minutes.do(self.process_batch)
        
#         try:
#             while self.running:
#                 schedule.run_pending()
#                 time.sleep(1)
#         except KeyboardInterrupt:
#             print("\n\n🛑 Stopping scheduled batch processor...")
#             self.running = False
#             self.stop()
    
#     def stop(self):
#         self.spark.stop()
#         print("✅ PySpark stopped")

# def signal_handler(sig, frame):
#     print("\n\n🛑 Received stop signal. Shutting down...")
#     sys.exit(0)

# if __name__ == "__main__":
#     signal.signal(signal.SIGINT, signal_handler)
    
#     processor = PySparkBatchProcessor()
    
#     try:
#         if len(sys.argv) > 1 and sys.argv[1] == '--scheduled':
#             processor.run_scheduled()
#         else:
#             result = processor.process_batch()
#             processor.stop()
#             if result:
#                 print(f"\n✅ Batch completed successfully! Processed {len(result)} products.")
#             else:
#                 print("\n❌ Batch failed!")
#                 sys.exit(1)
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         import traceback
#         traceback.print_exc()
#         processor.stop()
#         sys.exit(1)




# mapreduce_complete.py - Reads BATCHED raw data from S3
from pyspark.sql import SparkSession
import boto3
from datetime import datetime, timezone
import json
import time
import schedule
import signal
import sys

class PySparkBatchProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ScalableCloudAnalytics") \
            .config("spark.master", "local[*]") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.S3_BUCKET = "x24315851-scalable-s3"
        self.running = True
        self.last_batch_time = None
        
        self.BATCH_INTERVAL_MINUTES = 1
        
        print(f"📋 Batch Configuration:")
        print(f"   Interval: {self.BATCH_INTERVAL_MINUTES} minutes")
        print(f"   Source: S3/raw_batch/ (batched files - 100 records each)")
        
    def read_all_files_from_s3(self):
        """Read ALL batched files from S3/raw_batch/"""
        print(f"\n📥 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reading batched data from S3/raw_batch/...")
        
        all_trades = []
        processed = 0
        skipped = 0
        start_time = time.time()
        total_files = 0
        
        try:
            # Use pagination
            continuation_token = None
            page = 1
            
            while True:
                params = {
                    'Bucket': self.S3_BUCKET,
                    'Prefix': 'raw_batch/',
                    'MaxKeys': 100
                }
                if continuation_token:
                    params['ContinuationToken'] = continuation_token
                
                response = self.s3.list_objects_v2(**params)
                
                if 'Contents' not in response:
                    break
                
                page_files = len(response['Contents'])
                total_files += page_files
                print(f"   Page {page}: found {page_files} batch files (total: {total_files})")
                
                for obj in response['Contents']:
                    try:
                        key = obj['Key']
                        resp = self.s3.get_object(Bucket=self.S3_BUCKET, Key=key)
                        content = resp['Body'].read().decode('utf-8')
                        trades = json.loads(content)  # This is an array of trades
                        
                        # Filter bad prices
                        for trade in trades:
                            price = float(trade.get('price', 0))
                            product = trade.get('product', '')
                            
                            if price <= 0:
                                skipped += 1
                                continue
                            
                            # Skip unrealistic prices
                            if product == 'BTC-USD' and (price < 1000 or price > 200000):
                                skipped += 1
                                continue
                            elif product == 'ETH-USD' and (price < 10 or price > 10000):
                                skipped += 1
                                continue
                            elif product == 'SOL-USD' and (price < 1 or price > 500):
                                skipped += 1
                                continue
                            elif product == 'DOGE-USD' and (price < 0.001 or price > 1):
                                skipped += 1
                                continue
                            elif price > 1000000:
                                skipped += 1
                                continue
                            
                            all_trades.append(trade)
                            processed += 1
                            
                    except Exception as e:
                        print(f"      Error reading {key}: {e}")
                
                if not response.get('IsTruncated', False):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
                page += 1
                
                if total_files > 1000:
                    print(f"   Reached 1000 file limit, stopping")
                    break
            
            elapsed = time.time() - start_time
            print(f"✅ Total batch files: {total_files}, Valid records: {len(all_trades)} (skipped {skipped} invalid) in {elapsed:.2f}s")
            
        except Exception as e:
            print(f"❌ Error reading from S3: {e}")
            import traceback
            traceback.print_exc()
        
        if all_trades:
            print(f"   Sample valid trade: {json.dumps(all_trades[0], indent=2)}")
        
        rdd = self.spark.sparkContext.parallelize(all_trades)
        return rdd
    
    def process_batch(self):
        print("\n" + "="*60)
        print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting PySpark MapReduce Batch")
        print("   (Reading batched data from S3/raw_batch/)")
        print("="*60)
        
        rdd = self.read_all_files_from_s3()
        
        if rdd.isEmpty():
            print("❌ No valid data found in S3/raw_batch/!")
            return None
        
        start_time = time.time()
        total_trades = rdd.count()
        print(f"\n📊 Processing {total_trades} valid trades using MapReduce...")
        
        def mapper(trade):
            try:
                product = trade.get('product', 'unknown')
                price = float(trade.get('price', 0))
                size = float(trade.get('size', 0))
                side = str(trade.get('side', '')).strip().lower()
                latency = float(trade.get('latency_ms', 0))
                
                if product == 'unknown' or price <= 0:
                    return []
                
                results = [
                    (f"{product}_count", 1),
                    (f"{product}_price_sum", price),
                    (f"{product}_price_max", price),
                    (f"{product}_price_min", price),
                    (f"{product}_volume", size),
                ]
                
                if latency > 0:
                    results.append((f"{product}_latency_sum", latency))
                    results.append((f"{product}_latency_count", 1))
                
                if side == 'buy':
                    results.append((f"{product}_buy_count", 1))
                elif side == 'sell':
                    results.append((f"{product}_sell_count", 1))
                
                return results
            except Exception as e:
                return []
        
        map_start = time.time()
        mapped = rdd.flatMap(mapper)
        map_count = mapped.count()
        print(f"   Map phase: {map_count} key-value pairs generated in {time.time() - map_start:.2f}s")
        
        if map_count == 0:
            print("❌ No key-value pairs generated!")
            return None
        
        reduce_start = time.time()
        reduced = mapped.reduceByKey(lambda a, b: a + b)
        results = reduced.collect()
        print(f"   Reduce phase: {len(results)} keys aggregated in {time.time() - reduce_start:.2f}s")
        
        # Parse results
        products = {}
        
        for key, value in results:
            if key.endswith('_count') and 'buy' not in key and 'sell' not in key:
                product = key[:-6]
                val = int(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['count'] = val
                
            elif key.endswith('_price_sum'):
                product = key[:-10]
                val = float(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['price_sum'] = val
                
            elif key.endswith('_price_max'):
                product = key[:-10]
                val = float(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['price_max'] = val
                
            elif key.endswith('_price_min'):
                product = key[:-10]
                val = float(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['price_min'] = val
                
            elif key.endswith('_volume'):
                product = key[:-7]
                val = float(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['volume'] = val
                
            elif key.endswith('_buy_count'):
                product = key[:-10]
                val = int(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['buy_count'] = val
                
            elif key.endswith('_sell_count'):
                product = key[:-11]
                val = int(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['sell_count'] = val
        
        # Generate final results
        batch_results = []
        for product, agg in products.items():
            if agg['count'] > 0 and agg['price_sum'] > 0:
                avg_price = agg['price_sum'] / agg['count']
                
                batch_results.append({
                    'product': product,
                    'total_trades': agg['count'],
                    'average_price': round(avg_price, 2),
                    'maximum_price': round(agg['price_max'], 2) if agg['price_max'] != float('-inf') else 0,
                    'minimum_price': round(agg['price_min'], 2) if agg['price_min'] != float('inf') else 0,
                    'total_volume': round(agg['volume'], 4),
                    'buy_trades': agg['buy_count'],
                    'sell_trades': agg['sell_count'],
                    'avg_latency_ms': 0
                })
                print(f"   {product}: {agg['count']} trades, avg ${avg_price:.2f}, Buys: {agg['buy_count']}, Sells: {agg['sell_count']}")
        
        if batch_results:
            self.save_results(batch_results, datetime.now(timezone.utc).isoformat())
            print(f"\n✅ Generated {len(batch_results)} product summaries")
        else:
            print("⚠️ No results generated!")
            return None
        
        total_time = time.time() - start_time
        self.last_batch_time = datetime.now(timezone.utc)
        
        print("\n" + "="*60)
        print(f"✅ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Batch Processing Complete!")
        print(f"   Products: {len(batch_results)}")
        print(f"   Total Valid Trades: {sum(r['total_trades'] for r in batch_results)}")
        print(f"   Total Time: {total_time:.2f}s")
        print("="*60)
        
        return batch_results
    
    def save_results(self, results, timestamp):
        import csv
        import io
        
        if not results:
            return
        
        self.s3.put_object(
            Bucket=self.S3_BUCKET,
            Key='batch/batch_mapreduce.json',
            Body=json.dumps(results, indent=2),
            ContentType='application/json'
        )
        print("  ✅ JSON saved to batch/batch_mapreduce.json")
        
        self.s3.put_object(
            Bucket=self.S3_BUCKET,
            Key='batch/batch_summary.json',
            Body=json.dumps(results, indent=2),
            ContentType='application/json'
        )
        print("  ✅ Updated batch/batch_summary.json")
        
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
        self.s3.put_object(
            Bucket=self.S3_BUCKET,
            Key=f'batch/batch_mapreduce_{timestamp.split("T")[0]}_{timestamp.split("T")[1].split(".")[0].replace(":", "-")}.csv',
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"  ✅ CSV saved to S3")
    
    def run_scheduled(self):
        print("\n" + "="*60)
        print(f"🔄 Scheduled Batch Processing Started")
        print(f"   Interval: Every {self.BATCH_INTERVAL_MINUTES} minutes")
        print(f"   Source: S3/raw_batch/ (batched files)")
        print(f"   Press Ctrl+C to stop")
        print("="*60)
        
        print("\n🔄 Running initial batch...")
        result = self.process_batch()
        
        if result:
            print("\n✅ Initial batch completed successfully! Scheduling next runs...")
        else:
            print("\n⚠️ Initial batch failed. Will retry on next schedule...")
        
        schedule.every(self.BATCH_INTERVAL_MINUTES).minutes.do(self.process_batch)
        
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping scheduled batch processor...")
            self.running = False
            self.stop()
    
    def stop(self):
        self.spark.stop()
        print("✅ PySpark stopped")

def signal_handler(sig, frame):
    print("\n\n🛑 Received stop signal. Shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    processor = PySparkBatchProcessor()
    
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--scheduled':
            processor.run_scheduled()
        else:
            result = processor.process_batch()
            processor.stop()
            if result:
                print(f"\n✅ Batch completed successfully! Processed {len(result)} products.")
            else:
                print("\n❌ Batch failed!")
                sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        processor.stop()
        sys.exit(1)
