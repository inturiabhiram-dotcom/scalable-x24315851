# benchmark.py - Performance Testing with Load Variation
import boto3
import json
import time
import threading
import concurrent.futures
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone
import io
import os

S3_BUCKET = "x24315851-scalable-s3"
STREAM_NAME = "x24315851-kinesis-stream"

class PerformanceBenchmark:
    def __init__(self):
        self.kinesis = boto3.client('kinesis', region_name='us-east-1')
        self.athena = boto3.client('athena', region_name='us-east-1')
        self.s3 = boto3.client('s3')
        self.results = []
        
    def produce_test_data(self, num_records, rate):
        """Produce test data at specified rate"""
        print(f"Producing {num_records} records at {rate}/sec...")
        
        # Get current timestamp
        timestamp = datetime.now(timezone.utc).isoformat()
        
        for i in range(num_records):
            trade = {
                "trade_id": f"bench_{i}_{timestamp}",
                "product": "BTC-USD",
                "price": 40000 + (i % 1000),
                "size": 0.001 + (i % 100) / 100000,
                "side": "buy" if i % 2 == 0 else "sell",
                "time": datetime.now(timezone.utc).isoformat(),
                "latency_ms": 50 + (i % 100),
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "benchmark": True
            }
            
            try:
                self.kinesis.put_record(
                    StreamName=STREAM_NAME,
                    Data=json.dumps(trade),
                    PartitionKey=trade["product"]
                )
            except Exception as e:
                print(f"Error producing record {i}: {e}")
            
            if rate > 0:
                time.sleep(1/rate)
        
        print(f"✅ Produced {num_records} records")
        return timestamp
    
    def measure_latency(self, num_queries):
        """Measure query latency for speed layer"""
        latencies = []
        
        for i in range(num_queries):
            start = time.time()
            try:
                response = self.athena.start_query_execution(
                    QueryString="""
                        SELECT COUNT(*) as count
                        FROM speed_trades
                        WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1' MINUTE
                    """,
                    QueryExecutionContext={'Database': 'coinbase_analytics'},
                    ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/athena-results/'}
                )
                
                query_id = response['QueryExecutionId']
                while True:
                    status = self.athena.get_query_execution(QueryExecutionId=query_id)
                    if status['QueryExecution']['Status']['State'] in ['SUCCEEDED', 'FAILED']:
                        break
                    time.sleep(0.1)
                
                latency = (time.time() - start) * 1000
                latencies.append(latency)
            except Exception as e:
                print(f"Query error: {e}")
        
        return latencies
    
    def measure_throughput(self, duration_sec):
        """Measure throughput over time"""
        print(f"Measuring throughput for {duration_sec} seconds...")
        
        start_time = time.time()
        counts = []
        timestamps = []
        
        while time.time() - start_time < duration_sec:
            try:
                response = self.athena.start_query_execution(
                    QueryString="""
                        SELECT COUNT(*) as count
                        FROM speed_trades
                        WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '5' SECOND
                    """,
                    QueryExecutionContext={'Database': 'coinbase_analytics'},
                    ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/athena-results/'}
                )
                
                query_id = response['QueryExecutionId']
                while True:
                    status = self.athena.get_query_execution(ExecutionId=query_id)
                    if status['QueryExecution']['Status']['State'] in ['SUCCEEDED', 'FAILED']:
                        break
                    time.sleep(0.1)
                
                result = self.athena.get_query_results(QueryExecutionId=query_id)
                if result['ResultSet']['Rows']:
                    count = int(result['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0'))
                    counts.append(count)
                    timestamps.append(time.time())
            except Exception as e:
                print(f"Throughput query error: {e}")
            
            time.sleep(1)
        
        return timestamps, counts
    
    def measure_auto_scaling(self):
        """Measure auto-scaling behavior"""
        print("Measuring auto-scaling behavior...")
        
        asg = boto3.client('autoscaling')
        
        # Get initial capacity
        response = asg.describe_auto_scaling_groups(
            AutoScalingGroupNames=['speed-layer-asg']
        )
        if response['AutoScalingGroups']:
            initial = len(response['AutoScalingGroups'][0]['Instances'])
        else:
            initial = 1
        
        print(f"Initial instances: {initial}")
        
        # Simulate high load
        print("Simulating high load...")
        for i in range(100):
            trade = {
                "trade_id": f"load_{i}",
                "product": "BTC-USD",
                "price": 50000,
                "size": 0.01,
                "side": "buy",
                "time": datetime.now(timezone.utc).isoformat(),
                "latency_ms": 10,
                "processed_at": datetime.now(timezone.utc).isoformat()
            }
            self.kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(trade),
                PartitionKey="BTC-USD"
            )
            time.sleep(0.01)
        
        # Monitor scaling
        scaling_events = []
        for _ in range(60):
            time.sleep(5)
            response = asg.describe_auto_scaling_groups(
                AutoScalingGroupNames=['speed-layer-asg']
            )
            if response['AutoScalingGroups']:
                current = len(response['AutoScalingGroups'][0]['Instances'])
                scaling_events.append({
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'instances': current
                })
                print(f"Instances: {current}")
        
        return scaling_events
    
    def run_comprehensive_benchmark(self):
        """Run all benchmarks and generate report"""
        print("="*60)
        print("🚀 Starting Comprehensive Benchmark")
        print("="*60)
        
        benchmark_report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tests": {}
        }
        
        # Test 1: Different ingestion rates
        print("\n📊 Test 1: Varying Ingestion Rates")
        rates = [10, 50, 100, 200]
        rate_results = []
        
        for rate in rates:
            print(f"\nTesting rate: {rate} records/sec")
            
            # Produce data
            self.produce_test_data(rate * 5, rate)
            time.sleep(2)
            
            # Measure latency
            latencies = self.measure_latency(10)
            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            
            rate_results.append({
                "rate": rate,
                "avg_latency_ms": avg_latency,
                "p95_latency_ms": sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
                "queries": len(latencies)
            })
            
            print(f"  Avg latency: {avg_latency:.2f}ms")
        
        benchmark_report["tests"]["ingestion_rates"] = rate_results
        
        # Test 2: Parallelism speedup
        print("\n📊 Test 2: Parallelism Speedup")
        thread_counts = [1, 2, 4, 8]
        speedup_results = []
        
        for threads in thread_counts:
            print(f"\nTesting with {threads} threads")
            start = time.time()
            
            def query_worker():
                return self.measure_latency(5)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                futures = [executor.submit(query_worker) for _ in range(threads)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
            
            elapsed = (time.time() - start) * 1000
            all_latencies = []
            for r in results:
                all_latencies.extend(r)
            
            avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else 0
            speedup_results.append({
                "threads": threads,
                "elapsed_ms": elapsed,
                "avg_latency_ms": avg_latency,
                "total_queries": len(all_latencies)
            })
            
            print(f"  Elapsed: {elapsed:.2f}ms")
        
        benchmark_report["tests"]["parallelism"] = speedup_results
        
        # Test 3: Auto-scaling behavior
        print("\n📊 Test 3: Auto-Scaling Behavior")
        scaling_events = self.measure_auto_scaling()
        benchmark_report["tests"]["auto_scaling"] = scaling_events
        
        # Generate graphs
        self.generate_graphs(rate_results, speedup_results, scaling_events)
        
        # Save report
        report_key = f"benchmark/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=report_key,
            Body=json.dumps(benchmark_report, indent=2),
            ContentType='application/json'
        )
        
        print(f"\n✅ Benchmark complete! Report saved to s3://{S3_BUCKET}/{report_key}")
        return benchmark_report
    
    def generate_graphs(self, rate_results, speedup_results, scaling_events):
        """Generate performance graphs"""
        print("\n📊 Generating performance graphs...")
        
        # Graph 1: Latency vs Ingestion Rate
        if rate_results:
            plt.figure(figsize=(10, 6))
            rates = [r['rate'] for r in rate_results]
            latencies = [r['avg_latency_ms'] for r in rate_results]
            
            plt.plot(rates, latencies, 'bo-', linewidth=2, markersize=8)
            plt.xlabel('Ingestion Rate (records/sec)')
            plt.ylabel('Average Latency (ms)')
            plt.title('Speed Layer Latency vs Ingestion Rate')
            plt.grid(True, alpha=0.3)
            
            # Save to S3            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key='benchmark/graphs/latency_vs_rate.png',
                Body=buf.getvalue(),
                ContentType='image/png'
            )
            plt.close()
            print("  ✅ Latency vs Rate graph saved")
        
        # Graph 2: Parallel Speedup
        if speedup_results:
            plt.figure(figsize=(10, 6))
            threads = [r['threads'] for r in speedup_results]
            elapsed = [r['elapsed_ms'] for r in speedup_results]
            
            # Calculate speedup relative to 1 thread
            base = elapsed[0] if elapsed else 1
            speedups = [base / e if e > 0 else 0 for e in elapsed]
            
            plt.plot(threads, speedups, 'ro-', linewidth=2, markersize=8)
            plt.plot(threads, threads, 'g--', alpha=0.5, label='Ideal Speedup')
            plt.xlabel('Number of Threads')
            plt.ylabel('Speedup')
            plt.title('Parallel Speedup vs Thread Count')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key='benchmark/graphs/speedup_vs_threads.png',
                Body=buf.getvalue(),
                ContentType='image/png'
            )
            plt.close()
            print("  ✅ Speedup graph saved")
        
        # Graph 3: Auto-Scaling
        if scaling_events:
            plt.figure(figsize=(10, 6))
            times = [e['timestamp'] for e in scaling_events]
            instances = [e['instances'] for e in scaling_events]
            
            plt.plot(range(len(instances)), instances, 'go-', linewidth=2, markersize=8)
            plt.xlabel('Time (5 second intervals)')
            plt.ylabel('Number of Instances')
            plt.title('Auto-Scaling Behavior Under Load')
            plt.grid(True, alpha=0.3)
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key='benchmark/graphs/auto_scaling.png',
                Body=buf.getvalue(),
                ContentType='image/png'
            )
            plt.close()
            print("  ✅ Auto-scaling graph saved")

if __name__ == "__main__":
    benchmark = PerformanceBenchmark()
    benchmark.run_comprehensive_benchmark()