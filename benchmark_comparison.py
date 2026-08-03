
# benchmark_comparison.py - Sequential vs Parallel Processing Comparison
import boto3
import time
import json
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import pandas as pd
import os

S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"

class PerformanceComparison:
    def __init__(self):
        self.s3 = boto3.client('s3', region_name=REGION)
        self.results = {
            'sequential': [],
            'parallel_2': [],
            'parallel_4': [],
            'parallel_8': []
        }
        self.timestamps = []
        self.speedup_data = []
        
        # Create results directory
        os.makedirs('benchmark_results', exist_ok=True)
        
    def get_trades_from_s3(self, limit=200):
        """Get trades from S3 for testing"""
        trades = []
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=limit)
            if 'Contents' not in response:
                print("No data found in S3!")
                return []
            
            print(f"Loading {len(response['Contents'])} trades from S3...")
            for obj in response['Contents']:
                try:
                    key = obj['Key']
                    resp = self.s3.get_object(Bucket=S3_BUCKET, Key=key)
                    trade = json.loads(resp['Body'].read().decode('utf-8'))
                    trades.append(trade)
                except Exception as e:
                    pass
            print(f"Loaded {len(trades)} trades")
            return trades
        except Exception as e:
            print(f"Error loading trades: {e}")
            return []
    
    def process_trades_sequential(self, trades):
        """Process trades sequentially (1 worker)"""
        results = {}
        for trade in trades:
            product = trade.get('product', 'unknown')
            price = float(trade.get('price', 0))
            size = float(trade.get('size', 0))
            side = trade.get('side', 'unknown')
            
            if product not in results:
                results[product] = {
                    'count': 0,
                    'price_sum': 0,
                    'price_max': float('-inf'),
                    'price_min': float('inf'),
                    'volume': 0,
                    'buy_count': 0,
                    'sell_count': 0
                }
            
            agg = results[product]
            agg['count'] += 1
            agg['price_sum'] += price
            agg['price_max'] = max(agg['price_max'], price)
            agg['price_min'] = min(agg['price_min'], price)
            agg['volume'] += size
            if side == 'buy':
                agg['buy_count'] += 1
            elif side == 'sell':
                agg['sell_count'] += 1
        
        return results
    
    def process_trades_parallel(self, trades, num_workers):
        """Process trades in parallel with multiple workers"""
        def process_chunk(chunk):
            results = {}
            for trade in chunk:
                product = trade.get('product', 'unknown')
                price = float(trade.get('price', 0))
                size = float(trade.get('size', 0))
                side = trade.get('side', 'unknown')
                
                if product not in results:
                    results[product] = {
                        'count': 0,
                        'price_sum': 0,
                        'price_max': float('-inf'),
                        'price_min': float('inf'),
                        'volume': 0,
                        'buy_count': 0,
                        'sell_count': 0
                    }
                
                agg = results[product]
                agg['count'] += 1
                agg['price_sum'] += price
                agg['price_max'] = max(agg['price_max'], price)
                agg['price_min'] = min(agg['price_min'], price)
                agg['volume'] += size
                if side == 'buy':
                    agg['buy_count'] += 1
                elif side == 'sell':
                    agg['sell_count'] += 1
            
            return results
        
        # Split data into chunks
        chunk_size = max(1, len(trades) // num_workers)
        chunks = [trades[i:i+chunk_size] for i in range(0, len(trades), chunk_size)]
        
        # Process in parallel
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
            all_results = []
            for future in futures:
                try:
                    all_results.append(future.result())
                except Exception as e:
                    print(f"Worker error: {e}")
        
        # Merge results
        merged = {}
        for result in all_results:
            for product, data in result.items():
                if product not in merged:
                    merged[product] = {
                        'count': 0,
                        'price_sum': 0,
                        'price_max': float('-inf'),
                        'price_min': float('inf'),
                        'volume': 0,
                        'buy_count': 0,
                        'sell_count': 0
                    }
                merged[product]['count'] += data['count']
                merged[product]['price_sum'] += data['price_sum']
                merged[product]['price_max'] = max(merged[product]['price_max'], data['price_max'])
                merged[product]['price_min'] = min(merged[product]['price_min'], data['price_min'])
                merged[product]['volume'] += data['volume']
                merged[product]['buy_count'] += data['buy_count']
                merged[product]['sell_count'] += data['sell_count']
        
        return merged
    
    def run_benchmark(self, trades, iterations=5):
        """Run benchmark with multiple iterations"""
        print("\n" + "="*60)
        print("Running Performance Benchmark")
        print("="*60)
        print(f"   Trades: {len(trades)}")
        print(f"   Iterations per test: {iterations}")
        print("="*60)
        
        results = []
        
        # Test configurations
        configs = [
            ('Sequential (1 worker)', 1),
            ('Parallel (2 workers)', 2),
            ('Parallel (4 workers)', 4),
            ('Parallel (8 workers)', 8)
        ]
        
        for name, workers in configs:
            print(f"\nTesting: {name}...")
            times = []
            
            for i in range(iterations):
                start = time.time()
                
                if workers == 1:
                    self.process_trades_sequential(trades)
                else:
                    self.process_trades_parallel(trades, workers)
                
                elapsed = (time.time() - start) * 1000  # Convert to ms
                times.append(elapsed)
                print(f"   Run {i+1}: {elapsed:.2f}ms")
            
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            results.append({
                'name': name,
                'workers': workers,
                'avg_ms': avg_time,
                'min_ms': min_time,
                'max_ms': max_time,
                'times': times
            })
            
            print(f"   Avg: {avg_time:.2f}ms | Min: {min_time:.2f}ms | Max: {max_time:.2f}ms")
        
        return results
    
    def generate_report(self, results):
        """Generate detailed report with graphs"""
        print("\n" + "="*60)
        print("Generating Performance Report")
        print("="*60)
        
        # Create DataFrame
        df = pd.DataFrame(results)
        df.to_csv('benchmark_results/performance_data.csv', index=False)
        
        # Calculate speedup
        base_time = results[0]['avg_ms']
        speedup_data = []
        for r in results:
            speedup = base_time / r['avg_ms'] if r['avg_ms'] > 0 else 1
            efficiency = (speedup / r['workers']) * 100 if r['workers'] > 0 else 0
            speedup_data.append({
                'workers': r['workers'],
                'speedup': speedup,
                'efficiency': efficiency,
                'time_ms': r['avg_ms']
            })
        
        # Save speedup data
        df_speedup = pd.DataFrame(speedup_data)
        df_speedup.to_csv('benchmark_results/speedup_data.csv', index=False)
        
        # Print speedup summary
        print("\nSpeedup Summary:")
        print("-"*60)
        print(f"  {'Workers':<10} {'Time (ms)':<12} {'Speedup':<12} {'Efficiency':<12}")
        print("-"*60)
        for r in speedup_data:
            print(f"  {r['workers']:<10} {r['time_ms']:<12.2f} {r['speedup']:<12.2f}x {r['efficiency']:<12.1f}%")
        print("-"*60)
        
        # Generate graphs
        self.generate_graphs(results, speedup_data)
        
        # Save report
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'trades_processed': len(self.get_trades_from_s3(1)) * 0,
            'results': results,
            'speedup': speedup_data,
            'summary': {
                'best_speedup': max([r['speedup'] for r in speedup_data]),
                'best_efficiency': max([r['efficiency'] for r in speedup_data]),
                'fastest_time': min([r['time_ms'] for r in speedup_data]),
                'sequential_time': results[0]['avg_ms']
            }
        }
        
        with open('benchmark_results/performance_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nReport saved to benchmark_results/performance_report.json")
        
        return report
    
    def generate_graphs(self, results, speedup_data):
        """Generate performance graphs"""
        # Graph 1: Processing Time Comparison
        names = [r['name'] for r in results]
        avg_times = [r['avg_ms'] for r in results]
        min_times = [r['min_ms'] for r in results]
        max_times = [r['max_ms'] for r in results]
        
        x = np.arange(len(names))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(x, avg_times, width, label='Average', color='steelblue')
        
        # Add error bars
        yerr_lower = [avg_times[i] - min_times[i] for i in range(len(avg_times))]
        yerr_upper = [max_times[i] - avg_times[i] for i in range(len(avg_times))]
        ax.errorbar(x, avg_times, yerr=[yerr_lower, yerr_upper], 
                   fmt='none', ecolor='red', capsize=5, label='Min-Max Range')
        
        ax.set_xlabel('Processing Method', fontsize=12)
        ax.set_ylabel('Time (ms)', fontsize=12)
        ax.set_title('Sequential vs Parallel Processing Performance', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(names, rotation=15, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, val in zip(bars, avg_times):
            height = bar.get_height()
            ax.annotate(f'{val:.0f}ms',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),
                       textcoords="offset points",
                       ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('benchmark_results/sequential_vs_parallel.png', dpi=150, bbox_inches='tight')
        plt.savefig('benchmark_results/sequential_vs_parallel.pdf', bbox_inches='tight')
        plt.close()
        print("  Generated sequential_vs_parallel.png")
        
        # Graph 2: Speedup vs Worker Count
        workers = [r['workers'] for r in speedup_data]
        speedup = [r['speedup'] for r in speedup_data]
        ideal = workers  # Ideal speedup line
        
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(workers, speedup, 'bo-', linewidth=2, markersize=10, label='Actual Speedup')
        ax.plot(workers, ideal, 'r--', linewidth=2, alpha=0.6, label='Ideal Speedup')
        
        ax.set_xlabel('Number of Workers', fontsize=12)
        ax.set_ylabel('Speedup', fontsize=12)
        ax.set_title('Parallel Speedup vs Worker Count', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Add value labels
        for x, y in zip(workers, speedup):
            ax.annotate(f'{y:.2f}x', (x, y), textcoords="offset points", xytext=(0, 10), ha='center')
        
        plt.tight_layout()
        plt.savefig('benchmark_results/speedup_vs_workers.png', dpi=150, bbox_inches='tight')
        plt.savefig('benchmark_results/speedup_vs_workers.pdf', bbox_inches='tight')
        plt.close()
        print("  Generated speedup_vs_workers.png")
        
        # Graph 3: Combined Dashboard
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Performance Benchmark Dashboard', fontsize=16, fontweight='bold')
        
        # Subplot 1: Processing Time
        ax = axes[0, 0]
        ax.bar(names, avg_times, color='steelblue', alpha=0.7)
        ax.set_xlabel('Method')
        ax.set_ylabel('Time (ms)')
        ax.set_title('Processing Time Comparison')
        ax.tick_params(axis='x', rotation=15)
        ax.grid(True, alpha=0.3)
        
        # Subplot 2: Speedup
        ax = axes[0, 1]
        ax.plot(workers, speedup, 'bo-', linewidth=2, markersize=8)
        ax.plot(workers, ideal, 'r--', alpha=0.5, label='Ideal')
        ax.set_xlabel('Workers')
        ax.set_ylabel('Speedup')
        ax.set_title('Speedup vs Workers')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Subplot 3: Efficiency
        ax = axes[1, 0]
        efficiency = [r['efficiency'] for r in speedup_data]
        ax.bar(workers, efficiency, color='green', alpha=0.7)
        ax.set_xlabel('Workers')
        ax.set_ylabel('Efficiency (%)')
        ax.set_title('Parallel Efficiency')
        ax.grid(True, alpha=0.3)
        
        # Subplot 4: Summary Stats
        ax = axes[1, 1]
        ax.axis('off')
        
        summary_text = "Performance Summary\n" + "="*30 + "\n\n"
        summary_text += f"Best Speedup: {max(speedup):.2f}x\n"
        summary_text += f"Best Efficiency: {max(efficiency):.1f}%\n"
        summary_text += f"Fastest Time: {min([r['time_ms'] for r in speedup_data]):.0f}ms\n"
        summary_text += f"Sequential Time: {results[0]['avg_ms']:.0f}ms\n"
        summary_text += f"Parallel (4): {results[2]['avg_ms']:.0f}ms\n"
        summary_text += "\n" + "="*30 + "\n"
        
        if max(speedup) > 3:
            summary_text += "Excellent parallel scaling\n"
        elif max(speedup) > 1.5:
            summary_text += "Good parallel scaling\n"
        else:
            summary_text += "Limited parallel scaling\n"
        
        ax.text(0.1, 0.5, summary_text, transform=ax.transAxes, fontsize=12,
                verticalalignment='center', family='monospace')
        
        plt.tight_layout()
        plt.savefig('benchmark_results/performance_dashboard.png', dpi=150, bbox_inches='tight')
        plt.savefig('benchmark_results/performance_dashboard.pdf', bbox_inches='tight')
        plt.close()
        print("  Generated performance_dashboard.png")
    
    def run_complete_benchmark(self):
        """Run complete benchmark with all tests"""
        print("\n" + "="*60)
        print("SEQUENTIAL VS PARALLEL PERFORMANCE COMPARISON")
        print("="*60)
        
        # Get trades
        trades = self.get_trades_from_s3(200)
        if not trades:
            print("No trades found! Please run the pipeline first.")
            return
        
        # Run benchmark
        results = self.run_benchmark(trades, iterations=5)
        
        # Generate report
        report = self.generate_report(results)
        
        print("\n" + "="*60)
        print("Benchmark Complete!")
        print("="*60)
        print("\nResults saved to:")
        print("  benchmark_results/performance_data.csv")
        print("  benchmark_results/speedup_data.csv")
        print("  benchmark_results/performance_report.json")
        print("\nGraphs saved to:")
        print("  benchmark_results/sequential_vs_parallel.png")
        print("  benchmark_results/speedup_vs_workers.png")
        print("  benchmark_results/performance_dashboard.png")
        print("\nSummary:")
        print(f"  Sequential (1 worker): {results[0]['avg_ms']:.0f}ms")
        print(f"  Parallel (2 workers): {results[1]['avg_ms']:.0f}ms ({results[0]['avg_ms']/results[1]['avg_ms']:.2f}x)")
        print(f"  Parallel (4 workers): {results[2]['avg_ms']:.0f}ms ({results[0]['avg_ms']/results[2]['avg_ms']:.2f}x)")
        print(f"  Parallel (8 workers): {results[3]['avg_ms']:.0f}ms ({results[0]['avg_ms']/results[3]['avg_ms']:.2f}x)")
        print("="*60)

if __name__ == "__main__":
    benchmark = PerformanceComparison()
    benchmark.run_complete_benchmark()
