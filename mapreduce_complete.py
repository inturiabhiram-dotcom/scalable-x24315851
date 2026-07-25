

# # mapreduce_complete.py - PySpark MapReduce with Simple Loop Scheduler
# from pyspark.sql import SparkSession
# import boto3
# from datetime import datetime, timezone
# import json
# import time
# import signal
# import sys

# class PySparkBatchProcessor:
#     def __init__(self):
#         # Create Spark session
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
        
#         # Batch interval in seconds (60 seconds = 1 minute)
#         self.BATCH_INTERVAL_SECONDS = 60
        
#         print(f"📋 Batch Configuration:")
#         print(f"   Interval: {self.BATCH_INTERVAL_SECONDS} seconds")
#         print(f"   Source: S3/raw_batch/ (batched files - 100 records each)")
#         print(f"   Mode: Continuous loop (runs every {self.BATCH_INTERVAL_SECONDS}s)")
        
#     def read_all_files_from_s3(self):
#         """Read ALL batched files from S3/raw_batch/"""
#         print(f"\n📥 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reading batched data from S3/raw_batch/...")
        
#         all_trades = []
#         processed = 0
#         skipped = 0
#         start_time = time.time()
#         total_files = 0
        
#         try:
#             continuation_token = None
#             page = 1
            
#             while True:
#                 params = {
#                     'Bucket': self.S3_BUCKET,
#                     'Prefix': 'raw_batch/',
#                     'MaxKeys': 100
#                 }
#                 if continuation_token:
#                     params['ContinuationToken'] = continuation_token
                
#                 response = self.s3.list_objects_v2(**params)
                
#                 if 'Contents' not in response:
#                     break
                
#                 page_files = len(response['Contents'])
#                 total_files += page_files
#                 print(f"   Page {page}: found {page_files} batch files (total: {total_files})")
                
#                 for obj in response['Contents']:
#                     try:
#                         key = obj['Key']
#                         resp = self.s3.get_object(Bucket=self.S3_BUCKET, Key=key)
#                         content = resp['Body'].read().decode('utf-8')
#                         trades = json.loads(content)
                        
#                         for trade in trades:
#                             price = float(trade.get('price', 0))
#                             product = trade.get('product', '')
                            
#                             if price <= 0:
#                                 skipped += 1
#                                 continue
                            
#                             # Skip unrealistic prices
#                             if product == 'BTC-USD' and (price < 1000 or price > 200000):
#                                 skipped += 1
#                                 continue
#                             elif product == 'ETH-USD' and (price < 10 or price > 10000):
#                                 skipped += 1
#                                 continue
#                             elif product == 'SOL-USD' and (price < 1 or price > 500):
#                                 skipped += 1
#                                 continue
#                             elif product == 'DOGE-USD' and (price < 0.001 or price > 1):
#                                 skipped += 1
#                                 continue
#                             elif price > 1000000:
#                                 skipped += 1
#                                 continue
                            
#                             all_trades.append(trade)
#                             processed += 1
                            
#                     except Exception as e:
#                         print(f"      Error reading {key}: {e}")
                
#                 if not response.get('IsTruncated', False):
#                     break
                    
#                 continuation_token = response.get('NextContinuationToken')
#                 page += 1
                
#                 if total_files > 1000:
#                     print(f"   Reached 1000 file limit, stopping")
#                     break
            
#             elapsed = time.time() - start_time
#             print(f"✅ Total batch files: {total_files}, Valid records: {len(all_trades)} (skipped {skipped} invalid) in {elapsed:.2f}s")
            
#         except Exception as e:
#             print(f"❌ Error reading from S3: {e}")
#             import traceback
#             traceback.print_exc()
        
#         rdd = self.spark.sparkContext.parallelize(all_trades)
#         return rdd
    
#     def process_batch(self):
#         """Process batch using PySpark MapReduce"""
#         print("\n" + "="*60)
#         print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting PySpark MapReduce Batch")
#         print("   (Reading batched data from S3/raw_batch/)")
#         print("="*60)
        
#         rdd = self.read_all_files_from_s3()
        
#         if rdd.isEmpty():
#             print("❌ No valid data found in S3/raw_batch/!")
#             return None
        
#         start_time = time.time()
#         total_trades = rdd.count()
#         print(f"\n📊 Processing {total_trades} valid trades using MapReduce...")
        
#         def mapper(trade):
#             try:
#                 product = trade.get('product', 'unknown')
#                 price = float(trade.get('price', 0))
#                 size = float(trade.get('size', 0))
#                 side = str(trade.get('side', '')).strip().lower()
#                 latency = float(trade.get('latency_ms', 0))
                
#                 if product == 'unknown' or price <= 0:
#                     return []
                
#                 results = [
#                     (f"{product}_count", 1),
#                     (f"{product}_price_sum", price),
#                     (f"{product}_price_max", price),
#                     (f"{product}_price_min", price),
#                     (f"{product}_volume", size),
#                 ]
                
#                 if latency > 0:
#                     results.append((f"{product}_latency_sum", latency))
#                     results.append((f"{product}_latency_count", 1))
                
#                 if side == 'buy':
#                     results.append((f"{product}_buy_count", 1))
#                 elif side == 'sell':
#                     results.append((f"{product}_sell_count", 1))
                
#                 return results
#             except Exception as e:
#                 return []
        
#         map_start = time.time()
#         mapped = rdd.flatMap(mapper)
#         map_count = mapped.count()
#         print(f"   Map phase: {map_count} key-value pairs generated in {time.time() - map_start:.2f}s")
        
#         if map_count == 0:
#             print("❌ No key-value pairs generated!")
#             return None
        
#         reduce_start = time.time()
#         reduced = mapped.reduceByKey(lambda a, b: a + b)
#         results = reduced.collect()
#         print(f"   Reduce phase: {len(results)} keys aggregated in {time.time() - reduce_start:.2f}s")
        
#         # Parse results
#         products = {}
        
#         for key, value in results:
#             if key.endswith('_count') and 'buy' not in key and 'sell' not in key:
#                 product = key[:-6]
#                 val = int(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['count'] = val
                
#             elif key.endswith('_price_sum'):
#                 product = key[:-10]
#                 val = float(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['price_sum'] = val
                
#             elif key.endswith('_price_max'):
#                 product = key[:-10]
#                 val = float(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['price_max'] = val
                
#             elif key.endswith('_price_min'):
#                 product = key[:-10]
#                 val = float(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['price_min'] = val
                
#             elif key.endswith('_volume'):
#                 product = key[:-7]
#                 val = float(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['volume'] = val
                
#             elif key.endswith('_buy_count'):
#                 product = key[:-10]
#                 val = int(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['buy_count'] = val
                
#             elif key.endswith('_sell_count'):
#                 product = key[:-11]
#                 val = int(value)
#                 if product not in products:
#                     products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
#                                         'price_min': float('inf'), 'volume': 0.0, 
#                                         'buy_count': 0, 'sell_count': 0}
#                 products[product]['sell_count'] = val
        
#         # Generate final results
#         batch_results = []
#         for product, agg in products.items():
#             if agg['count'] > 0 and agg['price_sum'] > 0:
#                 avg_price = agg['price_sum'] / agg['count']
                
#                 batch_results.append({
#                     'product': product,
#                     'total_trades': agg['count'],
#                     'average_price': round(avg_price, 2),
#                     'maximum_price': round(agg['price_max'], 2) if agg['price_max'] != float('-inf') else 0,
#                     'minimum_price': round(agg['price_min'], 2) if agg['price_min'] != float('inf') else 0,
#                     'total_volume': round(agg['volume'], 4),
#                     'buy_trades': agg['buy_count'],
#                     'sell_trades': agg['sell_count'],
#                     'avg_latency_ms': 0
#                 })
#                 print(f"   {product}: {agg['count']} trades, avg ${avg_price:.2f}, Buys: {agg['buy_count']}, Sells: {agg['sell_count']}")
        
#         if batch_results:
#             self.save_results(batch_results, datetime.now(timezone.utc).isoformat())
#             print(f"\n✅ Generated {len(batch_results)} product summaries")
#         else:
#             print("⚠️ No results generated!")
#             return None
        
#         total_time = time.time() - start_time
#         print(f"\n✅ Batch completed in {total_time:.2f}s")
        
#         return batch_results
    
#     def save_results(self, results, timestamp):
#         import csv
#         import io
        
#         if not results:
#             return
        
#         self.s3.put_object(
#             Bucket=self.S3_BUCKET,
#             Key='batch/batch_summary.json',
#             Body=json.dumps(results, indent=2),
#             ContentType='application/json'
#         )
#         print("  ✅ JSON saved to batch/batch_summary.json")
        
#         self.s3.put_object(
#             Bucket=self.S3_BUCKET,
#             Key='batch/batch_summary.json',
#             Body=json.dumps(results, indent=2),
#             ContentType='application/json'
#         )
#         print("  ✅ Updated batch/batch_summary.json")
        
#         csv_buffer = io.StringIO()
#         writer = csv.DictWriter(csv_buffer, fieldnames=results[0].keys())
#         writer.writeheader()
#         writer.writerows(results)
#         self.s3.put_object(
#             Bucket=self.S3_BUCKET,
#             Key=f'batch/batch_summary_{timestamp.split("T")[0]}_{timestamp.split("T")[1].split(".")[0].replace(":", "-")}.csv',
#             Body=csv_buffer.getvalue(),
#             ContentType='text/csv'
#         )
#         print(f"  ✅ CSV saved to S3")
    
#     def run_continuous(self):
#         """Run batch processing continuously with a loop"""
#         print("\n" + "="*60)
#         print(f"🔄 Continuous Batch Processing Started")
#         print(f"   Interval: Every {self.BATCH_INTERVAL_SECONDS} seconds")
#         print(f"   Source: S3/raw_batch/ (batched files)")
#         print(f"   Press Ctrl+C to stop")
#         print("="*60)
        
#         batch_count = 0
        
#         try:
#             while self.running:
#                 batch_count += 1
#                 print(f"\n{'#'*60}")
#                 print(f"# BATCH RUN #{batch_count}")
#                 print(f"{'#'*60}")
                
#                 # Run the batch
#                 result = self.process_batch()
                
#                 if result:
#                     print(f"✅ Batch #{batch_count} completed successfully!")
#                 else:
#                     print(f"⚠️ Batch #{batch_count} failed or no data")
                
#                 # Wait for next interval
#                 if self.running:
#                     print(f"\n⏳ Waiting {self.BATCH_INTERVAL_SECONDS} seconds until next batch...")
#                     time.sleep(self.BATCH_INTERVAL_SECONDS)
                    
#         except KeyboardInterrupt:
#             print("\n\n🛑 Stopping continuous batch processor...")
#             self.running = False
#         finally:
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
#         # Run in continuous mode (runs every 60 seconds)
#         processor.run_continuous()
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         import traceback
#         traceback.print_exc()
#         processor.stop()
#         sys.exit(1)







# mapreduce_complete.py - PySpark MapReduce with COMPREHENSIVE DEBUGGING
from pyspark.sql import SparkSession
import boto3
from datetime import datetime, timezone
import json
import time
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
        
        self.BATCH_INTERVAL_SECONDS = 60
        
        print(f"📋 Batch Configuration:")
        print(f"   Interval: {self.BATCH_INTERVAL_SECONDS} seconds")
        print(f"   Source: S3/raw_batch/ (batched files)")
        print(f"   Mode: DEBUG MODE - Printing min/max values")
        
    def read_all_files_from_s3(self):
        print(f"\n📥 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reading batched data from S3/raw_batch/...")
        
        all_trades = []
        processed = 0
        skipped = 0
        start_time = time.time()
        total_files = 0
        
        # Track min/max prices per product for debugging
        price_tracker = {}
        
        try:
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
                        trades = json.loads(content)
                        
                        for trade in trades:
                            price = float(trade.get('price', 0))
                            product = trade.get('product', '')
                            
                            if price <= 0:
                                skipped += 1
                                continue
                            
                            # Skip unrealistic prices
                            if product == 'BTC-USD' and (price < 1000 or price > 200000):
                                skipped += 1
                                print(f"   ⚠️ Skipping BTC bad price: ${price:,.2f}")
                                continue
                            elif product == 'ETH-USD' and (price < 10 or price > 10000):
                                skipped += 1
                                print(f"   ⚠️ Skipping ETH bad price: ${price:,.2f}")
                                continue
                            elif product == 'SOL-USD' and (price < 1 or price > 500):
                                skipped += 1
                                print(f"   ⚠️ Skipping SOL bad price: ${price:,.2f}")
                                continue
                            elif product == 'DOGE-USD' and (price < 0.001 or price > 1):
                                skipped += 1
                                print(f"   ⚠️ Skipping DOGE bad price: ${price:,.2f}")
                                continue
                            elif price > 1000000:
                                skipped += 1
                                print(f"   ⚠️ Skipping bad price > $1M: ${price:,.2f} for {product}")
                                continue
                            
                            # Track min/max for debugging
                            if product not in price_tracker:
                                price_tracker[product] = {'min': price, 'max': price, 'count': 0}
                            price_tracker[product]['min'] = min(price_tracker[product]['min'], price)
                            price_tracker[product]['max'] = max(price_tracker[product]['max'], price)
                            price_tracker[product]['count'] += 1
                            
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
            print(f"\n✅ Total batch files: {total_files}, Valid records: {len(all_trades)} (skipped {skipped} invalid)")
            print(f"   Time: {elapsed:.2f}s")
            
            # Print tracked min/max values
            print(f"\n📊 DEBUG - Min/Max from raw data (before MapReduce):")
            for product, stats in price_tracker.items():
                print(f"   {product}: min=${stats['min']:,.2f}, max=${stats['max']:,.2f}, count={stats['count']}")
            
        except Exception as e:
            print(f"❌ Error reading from S3: {e}")
            import traceback
            traceback.print_exc()
        
        rdd = self.spark.sparkContext.parallelize(all_trades)
        return rdd
    
    def process_batch(self):
        print("\n" + "="*60)
        print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting PySpark MapReduce Batch")
        print("   (DEBUG MODE - Tracking min/max values)")
        print("="*60)
        
        rdd = self.read_all_files_from_s3()
        
        if rdd.isEmpty():
            print("❌ No valid data found in S3/raw_batch/!")
            return None
        
        start_time = time.time()
        total_trades = rdd.count()
        print(f"\n📊 Processing {total_trades} valid trades using MapReduce...")
        
        # DEBUG: Take a sample of trades to inspect
        sample = rdd.take(10)
        print("\n🔍 DEBUG - Sample trades from RDD:")
        for i, trade in enumerate(sample):
            print(f"   Trade {i+1}: product={trade.get('product')}, price={trade.get('price')}, side={trade.get('side')}")
        
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
        print(f"\n   Map phase: {map_count} key-value pairs generated in {time.time() - map_start:.2f}s")
        
        if map_count == 0:
            print("❌ No key-value pairs generated!")
            return None
        
        # DEBUG: Check mapped keys
        print("\n🔍 DEBUG - Sample mapped keys:")
        mapped_keys = mapped.keys().take(20)
        for key in mapped_keys:
            print(f"   {key}")
        
        reduce_start = time.time()
        reduced = mapped.reduceByKey(lambda a, b: a + b)
        results = reduced.collect()
        print(f"\n   Reduce phase: {len(results)} keys aggregated in {time.time() - reduce_start:.2f}s")
        
        # DEBUG: Print all price-related keys
        print("\n🔍 DEBUG - All price-related keys from reduce phase:")
        price_keys = [k for k, v in results if '_price_' in k or '_price' in k]
        for key in sorted(price_keys):
            val = [v for k, v in results if k == key][0]
            print(f"   {key}: {val}")
        
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
                print(f"   🔍 DEBUG - price_sum for {product}: {val}")
                
            elif key.endswith('_price_max'):
                product = key[:-10]
                val = float(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['price_max'] = val
                print(f"   🔍 DEBUG - price_max for {product}: {val}")
                
            elif key.endswith('_price_min'):
                product = key[:-10]
                val = float(value)
                if product not in products:
                    products[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                        'price_min': float('inf'), 'volume': 0.0, 
                                        'buy_count': 0, 'sell_count': 0}
                products[product]['price_min'] = val
                print(f"   🔍 DEBUG - price_min for {product}: {val}")
                
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
        
        # Print final product data before generating results
        print("\n🔍 DEBUG - Final product aggregations:")
        for product, agg in products.items():
            if agg['count'] > 0:
                print(f"   {product}:")
                print(f"      count: {agg['count']}")
                print(f"      price_sum: {agg['price_sum']}")
                print(f"      price_max: {agg['price_max']}")
                print(f"      price_min: {agg['price_min']}")
                print(f"      volume: {agg['volume']}")
                print(f"      buy_count: {agg['buy_count']}")
                print(f"      sell_count: {agg['sell_count']}")
        
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
                print(f"\n   ✅ FINAL: {product}: {agg['count']} trades, avg ${avg_price:.2f}, "
                      f"Price Range: ${agg['price_min']:,.2f} - ${agg['price_max']:,.2f}")
        
        if batch_results:
            self.save_results(batch_results, datetime.now(timezone.utc).isoformat())
            print(f"\n✅ Generated {len(batch_results)} product summaries")
        else:
            print("⚠️ No results generated!")
            return None
        
        total_time = time.time() - start_time
        print(f"\n✅ Batch completed in {total_time:.2f}s")
        
        return batch_results
    
    def save_results(self, results, timestamp):
        import csv
        import io
        
        if not results:
            return
        
        self.s3.put_object(
            Bucket=self.S3_BUCKET,
            Key='batch/batch_summary.json',
            Body=json.dumps(results, indent=2),
            ContentType='application/json'
        )
        print("  ✅ JSON saved to batch/batch_summary.json")
        
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
        self.s3.put_object(
            Bucket=self.S3_BUCKET,
            Key=f'batch/batch_summary_{timestamp.split("T")[0]}_{timestamp.split("T")[1].split(".")[0].replace(":", "-")}.csv',
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"  ✅ CSV saved to S3")
    
    def run_continuous(self):
        print("\n" + "="*60)
        print(f"🔄 Continuous Batch Processing Started (DEBUG MODE)")
        print(f"   Interval: Every {self.BATCH_INTERVAL_SECONDS} seconds")
        print(f"   Press Ctrl+C to stop")
        print("="*60)
        
        batch_count = 0
        
        try:
            while self.running:
                batch_count += 1
                print(f"\n{'#'*60}")
                print(f"# BATCH RUN #{batch_count} (DEBUG MODE)")
                print(f"{'#'*60}")
                
                result = self.process_batch()
                
                if result:
                    print(f"✅ Batch #{batch_count} completed successfully!")
                else:
                    print(f"⚠️ Batch #{batch_count} failed or no data")
                
                if self.running:
                    print(f"\n⏳ Waiting {self.BATCH_INTERVAL_SECONDS} seconds until next batch...")
                    time.sleep(self.BATCH_INTERVAL_SECONDS)
                    
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping continuous batch processor...")
            self.running = False
        finally:
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
        processor.run_continuous()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        processor.stop()
        sys.exit(1)
