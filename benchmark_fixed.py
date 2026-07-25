
# benchmark_fixed.py - Fixed benchmark showing proper speedup
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import boto3
from datetime import datetime, timezone

S3_BUCKET = "x24315851-scalable-s3"

def run_benchmark():
    """Run proper benchmark with real data"""
    print("="*60)
    print(" Running Performance Benchmark")
    print("="*60)
    
    # Get real data
    s3 = boto3.client('s3', region_name='us-east-1')
    trades = []
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=200)
        if 'Contents' in response:
            for obj in response['Contents'][:100]:
                try:
                    resp = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                    content = resp['Body'].read().decode('utf-8')
                    trades.append(json.loads(content))
                except:
                    pass
    except:
        pass
    
    print(f"📥 Using {len(trades)} trades for benchmark")
    
    # Sequential processing
    print("\n🔹 Sequential Processing...")
    start = time.time()
    sequential_results = process_sequential(trades)
    seq_time = (time.time() - start) * 1000
    
    print(f"   Time: {seq_time:.2f}ms")
    print(f"   Results: {len(sequential_results)}")
    
    # Parallel processing with different threads
    parallel_results = {}
    thread_counts = [2, 4, 8]
    
    for num_threads in thread_counts:
        print(f"\n🔹 Parallel Processing ({num_threads} threads)...")
        start = time.time()
        results = process_parallel(trades, num_threads)
        par_time = (time.time() - start) * 1000
        
        parallel_results[f"{num_threads}_threads"] = {
            "time_ms": round(par_time, 2),
            "threads": num_threads,
            "results": len(results),
            "success": len(results) > 0
        }
        
        print(f"   Time: {par_time:.2f}ms")
        print(f"   Results: {len(results)}")
    
    # Calculate speedup
    best_parallel = min([v['time_ms'] for v in parallel_results.values()])
    speedup = round(seq_time / best_parallel, 2) if best_parallel > 0 else 0
    
    print("\n" + "="*60)
    print("📈 Benchmark Results")
    print("="*60)
    print(f"  Sequential: {seq_time:.2f}ms")
    print(f"  Best Parallel: {best_parallel:.2f}ms")
    print(f"  Speedup: {speedup}x")
    print("="*60)
    
    return {
        "sequential": {
            "time_ms": round(seq_time, 2),
            "records": len(sequential_results)
        },
        "parallel": parallel_results,
        "speedup": speedup,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def process_sequential(trades):
    """Sequential processing of trades"""
    results = {}
    for trade in trades:
        product = trade.get('product', 'unknown')
        price = float(trade.get('price', 0))
        size = float(trade.get('size', 0))
        
        if product not in results:
            results[product] = {'count': 0, 'sum_price': 0, 'sum_size': 0}
        
        results[product]['count'] += 1
        results[product]['sum_price'] += price
        results[product]['sum_size'] += size
    
    return results

def process_parallel(trades, num_threads):
    """Parallel processing of trades"""
    from concurrent.futures import ThreadPoolExecutor
    
    def process_chunk(chunk):
        results = {}
        for trade in chunk:
            product = trade.get('product', 'unknown')
            price = float(trade.get('price', 0))
            size = float(trade.get('size', 0))
            
            if product not in results:
                results[product] = {'count': 0, 'sum_price': 0, 'sum_size': 0}
            
            results[product]['count'] += 1
            results[product]['sum_price'] += price
            results[product]['sum_size'] += size
        return results
    
    # Split trades into chunks
    chunk_size = max(1, len(trades) // num_threads)
    chunks = [trades[i:i+chunk_size] for i in range(0, len(trades), chunk_size)]
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        results = {}
        for future in futures:
            chunk_results = future.result()
            for product, data in chunk_results.items():
                if product not in results:
                    results[product] = {'count': 0, 'sum_price': 0, 'sum_size': 0}
                results[product]['count'] += data['count']
                results[product]['sum_price'] += data['sum_price']
                results[product]['sum_size'] += data['sum_size']
    
    return results

if __name__ == "__main__":
    run_benchmark()
