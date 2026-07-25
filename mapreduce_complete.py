


# mapreduce_complete.py - FIXED: Proper min/max aggregation (NO MULTIPLICATION)
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
        print(f"   Mode: FIXED - Proper min/max (NO MULTIPLICATION)")
        
    def read_all_files_from_s3(self):
        print(f"\n📥 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Reading batched data from S3/raw_batch/...")
        
        all_trades = []
        processed = 0
        skipped = 0
        start_time = time.time()
        total_files = 0
        
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
        
        rdd = self.spark.sparkContext.parallelize(all_trades)
        return rdd
    
    def process_batch(self):
        print("\n" + "="*60)
        print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting PySpark MapReduce Batch")
        print("   (FIXED: Proper min/max - NO MULTIPLICATION)")
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
        
        # SHUFFLE & REDUCE phase - FIXED: Use proper aggregation functions
        reduce_start = time.time()
        
        # Split into different RDDs based on key type
        count_sum = mapped.filter(lambda x: x[0].endswith('_count') and 'buy' not in x[0] and 'sell' not in x[0])
        price_sum = mapped.filter(lambda x: x[0].endswith('_price_sum'))
        price_max = mapped.filter(lambda x: x[0].endswith('_price_max'))
        price_min = mapped.filter(lambda x: x[0].endswith('_price_min'))
        volume_sum = mapped.filter(lambda x: x[0].endswith('_volume'))
        latency_sum = mapped.filter(lambda x: x[0].endswith('_latency_sum'))
        latency_count = mapped.filter(lambda x: x[0].endswith('_latency_count'))
        buy_count = mapped.filter(lambda x: x[0].endswith('_buy_count'))
        sell_count = mapped.filter(lambda x: x[0].endswith('_sell_count'))
        
        # Reduce with proper functions
        reduced_count = count_sum.reduceByKey(lambda a, b: a + b)           # SUM
        reduced_price_sum = price_sum.reduceByKey(lambda a, b: a + b)       # SUM
        reduced_price_max = price_max.reduceByKey(lambda a, b: max(a, b))   # MAX (NO MULTIPLICATION!)
        reduced_price_min = price_min.reduceByKey(lambda a, b: min(a, b))   # MIN (NO MULTIPLICATION!)
        reduced_volume = volume_sum.reduceByKey(lambda a, b: a + b)         # SUM
        reduced_latency_sum = latency_sum.reduceByKey(lambda a, b: a + b)   # SUM
        reduced_latency_count = latency_count.reduceByKey(lambda a, b: a + b) # SUM
        reduced_buy = buy_count.reduceByKey(lambda a, b: a + b)             # SUM
        reduced_sell = sell_count.reduceByKey(lambda a, b: a + b)           # SUM
        
        # Collect all results
        results = {}
        
        for key, val in reduced_count.collect():
            product = key[:-6]  # Remove '_count'
            if product not in results:
                results[product] = {'count': 0, 'price_sum': 0.0, 'price_max': float('-inf'), 
                                   'price_min': float('inf'), 'volume': 0.0, 
                                   'buy_count': 0, 'sell_count': 0}
            results[product]['count'] = val
            
        for key, val in reduced_price_sum.collect():
            product = key[:-10]  # Remove '_price_sum'
            if product in results:
                results[product]['price_sum'] = float(val)
                
        for key, val in reduced_price_max.collect():
            product = key[:-10]  # Remove '_price_max'
            if product in results:
                results[product]['price_max'] = float(val)  # This is the MAX, not multiplied!
                
        for key, val in reduced_price_min.collect():
            product = key[:-10]  # Remove '_price_min'
            if product in results:
                results[product]['price_min'] = float(val)  # This is the MIN, not multiplied!
                
        for key, val in reduced_volume.collect():
            product = key[:-7]  # Remove '_volume'
            if product in results:
                results[product]['volume'] = float(val)
                
        for key, val in reduced_buy.collect():
            product = key[:-10]  # Remove '_buy_count'
            if product in results:
                results[product]['buy_count'] = int(val)
                
        for key, val in reduced_sell.collect():
            product = key[:-11]  # Remove '_sell_count'
            if product in results:
                results[product]['sell_count'] = int(val)
        
        print(f"   Reduce phase completed in {time.time() - reduce_start:.2f}s")
        
        # Print final results for debugging
        print("\n🔍 DEBUG - Final product aggregations (NO MULTIPLICATION):")
        for product, agg in results.items():
            if agg['count'] > 0:
                avg_price = agg['price_sum'] / agg['count']
                print(f"   {product}: {agg['count']} trades")
                print(f"      Avg Price: ${avg_price:,.2f}")
                print(f"      Min Price: ${agg['price_min']:,.2f}  ← NOT multiplied!")
                print(f"      Max Price: ${agg['price_max']:,.2f}  ← NOT multiplied!")
                print(f"      Buys: {agg['buy_count']}, Sells: {agg['sell_count']}")
        
        # Generate final results
        batch_results = []
        for product, agg in results.items():
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
        print(f"🔄 Continuous Batch Processing Started (FIXED - NO MULTIPLICATION)")
        print(f"   Interval: Every {self.BATCH_INTERVAL_SECONDS} seconds")
        print(f"   Press Ctrl+C to stop")
        print("="*60)
        
        batch_count = 0
        
        try:
            while self.running:
                batch_count += 1
                print(f"\n{'#'*60}")
                print(f"# BATCH RUN #{batch_count} (FIXED)")
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
