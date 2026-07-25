#!/usr/bin/env python3
# test_batch.py - Comprehensive Batch Processor Test Script
import boto3
import json
import time
from datetime import datetime, timezone
import subprocess
import sys
import os

S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"

class BatchTester:
    def __init__(self):
        self.s3 = boto3.client('s3', region_name=REGION)
        self.results = {
            'tests': [],
            'passed': 0,
            'failed': 0,
            'total': 0
        }
        
    def print_header(self, title):
        print("\n" + "="*70)
        print(f"📋 {title}")
        print("="*70)
    
    def print_result(self, test_name, passed, details=""):
        status = "✅ PASS" if passed else "❌ FAIL"
        self.results['total'] += 1
        if passed:
            self.results['passed'] += 1
        else:
            self.results['failed'] += 1
        self.results['tests'].append({'name': test_name, 'passed': passed})
        print(f"  {status} - {test_name}")
        if details:
            print(f"       {details}")
    
    def test_s3_batch_exists(self):
        """Test 1: Check if batch files exist in S3"""
        self.print_header("TEST 1: Check Batch Files in S3")
        
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='batch/', MaxKeys=10)
            
            if 'Contents' not in response or len(response['Contents']) == 0:
                self.print_result("Batch Files Exist", False, "No batch files found in S3!")
                return False
            
            files = [obj['Key'] for obj in response['Contents']]
            self.print_result("Batch Files Exist", True, f"Found {len(files)} batch files")
            
            # Check for specific files
            has_summary = any('batch_summary.json' in f for f in files)
            self.print_result("batch_summary.json exists", has_summary)
            
            has_mapreduce = any('batch_mapreduce.json' in f for f in files)
            self.print_result("batch_mapreduce.json exists", has_mapreduce)
            
            has_csv = any('.csv' in f for f in files)
            self.print_result("CSV file exists", has_csv)
            
            return True
        except Exception as e:
            self.print_result("S3 Batch Check", False, str(e))
            return False
    
    def test_batch_data_content(self):
        """Test 2: Check batch data content"""
        self.print_header("TEST 2: Validate Batch Data Content")
        
        try:
            # Read batch_summary.json
            resp = self.s3.get_object(Bucket=S3_BUCKET, Key='batch/batch_summary.json')
            data = json.loads(resp['Body'].read().decode('utf-8'))
            
            if not data:
                self.print_result("Batch Data Content", False, "Empty batch data!")
                return False
            
            self.print_result("Batch Data Exists", True, f"Found {len(data)} products")
            
            # Check each product's data
            all_valid = True
            for product in data:
                product_name = product.get('product', 'unknown')
                trades = product.get('total_trades', 0)
                avg_price = product.get('average_price', 0)
                max_price = product.get('maximum_price', 0)
                min_price = product.get('minimum_price', 0)
                volume = product.get('total_volume', 0)
                buys = product.get('buy_trades', 0)
                sells = product.get('sell_trades', 0)
                
                # Validate fields
                valid = True
                details = []
                
                if trades <= 0:
                    valid = False
                    details.append("trades=0")
                if avg_price <= 0:
                    valid = False
                    details.append("avg_price=0")
                if max_price <= 0:
                    valid = False
                    details.append("max_price=0")
                if min_price <= 0:
                    valid = False
                    details.append("min_price=0")
                if volume <= 0:
                    valid = False
                    details.append("volume=0")
                
                # Check buy/sell counts - should not both be 0 if trades > 0
                if trades > 0 and buys == 0 and sells == 0:
                    valid = False
                    details.append("buys=0 AND sells=0 (should have at least one)")
                
                status = "✅" if valid else "❌"
                detail_str = f"Trades: {trades}, Buys: {buys}, Sells: {sells}, Price: ${avg_price}"
                if details:
                    detail_str += f" ⚠️ Issues: {', '.join(details)}"
                
                self.print_result(f"Product: {product_name}", valid, detail_str)
                
                if not valid:
                    all_valid = False
            
            return all_valid
        except Exception as e:
            self.print_result("Batch Data Validation", False, str(e))
            return False
    
    def test_buy_sell_counts(self):
        """Test 3: Verify buy/sell counts are correct"""
        self.print_header("TEST 3: Verify Buy/Sell Counts")
        
        try:
            # Read batch_summary.json
            resp = self.s3.get_object(Bucket=S3_BUCKET, Key='batch/batch_summary.json')
            data = json.loads(resp['Body'].read().decode('utf-8'))
            
            if not data:
                self.print_result("Buy/Sell Counts", False, "No data to verify")
                return False
            
            # Also get raw data to verify counts
            raw_response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='raw/', MaxKeys=500)
            raw_trades = []
            
            if 'Contents' in raw_response:
                for obj in raw_response['Contents'][:100]:  # Sample 100 trades
                    try:
                        resp = self.s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                        trade = json.loads(resp['Body'].read().decode('utf-8'))
                        raw_trades.append(trade)
                    except:
                        pass
            
            all_valid = True
            
            for product in data:
                product_name = product.get('product', 'unknown')
                batch_buys = product.get('buy_trades', 0)
                batch_sells = product.get('sell_trades', 0)
                batch_total = product.get('total_trades', 0)
                
                # Count from raw data for this product
                raw_buys = sum(1 for t in raw_trades if t.get('product') == product_name and t.get('side', '').lower() == 'buy')
                raw_sells = sum(1 for t in raw_trades if t.get('product') == product_name and t.get('side', '').lower() == 'sell')
                raw_total = raw_buys + raw_sells
                
                # If raw data has trades for this product, compare
                if raw_total > 0:
                    # Check if batch total matches raw total (approximately, since we only sampled)
                    match = True
                    details = f"Batch: {batch_buys}B/{batch_sells}S ({batch_total} total), Raw: {raw_buys}B/{raw_sells}S ({raw_total} total)"
                    
                    # Allow some discrepancy since we only sampled raw data
                    if abs(batch_total - raw_total) > 10:
                        match = False
                        details += " ⚠️ Large discrepancy"
                    
                    self.print_result(f"Buy/Sell counts for {product_name}", match, details)
                    
                    if not match:
                        all_valid = False
                else:
                    # No raw data for this product, check if batch has reasonable values
                    if batch_total > 0 and (batch_buys == 0 and batch_sells == 0):
                        self.print_result(f"Buy/Sell counts for {product_name}", False, 
                                         f"Batch shows {batch_total} trades but 0 buys and 0 sells!")
                        all_valid = False
                    else:
                        self.print_result(f"Buy/Sell counts for {product_name}", True, 
                                         f"Batch: {batch_buys}B/{batch_sells}S ({batch_total} total)")
            
            return all_valid
        except Exception as e:
            self.print_result("Buy/Sell Verification", False, str(e))
            return False
    
    def test_batch_processor_execution(self):
        """Test 4: Run batch processor and check if it completes"""
        self.print_header("TEST 4: Batch Processor Execution")
        
        try:
            # Kill existing batch process
            os.system("pkill -f 'mapreduce_complete.py' 2>/dev/null")
            time.sleep(2)
            
            # Run batch processor
            print("  ⏳ Running batch processor...")
            result = subprocess.run(
                ['python3', 'mapreduce_complete.py'],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                self.print_result("Batch Processor Execution", True, "Completed successfully")
                
                # Check if new batch files were created
                time.sleep(2)
                response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='batch/', MaxKeys=5)
                if 'Contents' in response:
                    self.print_result("New Batch Files Created", True, f"Found {len(response['Contents'])} files")
                else:
                    self.print_result("New Batch Files Created", False, "No files created")
                
                return True
            else:
                error_msg = result.stderr[:100] if result.stderr else "Unknown error"
                self.print_result("Batch Processor Execution", False, f"Exit code: {result.returncode}, Error: {error_msg}")
                return False
                
        except subprocess.TimeoutExpired:
            self.print_result("Batch Processor Execution", False, "Timeout (60s)")
            return False
        except Exception as e:
            self.print_result("Batch Processor Execution", False, str(e))
            return False
    
    def test_athena_integration(self):
        """Test 5: Check Athena table integration"""
        self.print_header("TEST 5: Athena Table Integration")
        
        try:
            athena = boto3.client('athena', region_name=REGION)
            
            # Query batch_summary table
            query = "SELECT COUNT(*) as count FROM coinbase_analytics.batch_summary"
            response = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': 'coinbase_analytics'},
                ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/athena-results/'}
            )
            
            query_id = response['QueryExecutionId']
            
            # Wait for completion
            start_time = time.time()
            while time.time() - start_time < 30:
                status = athena.get_query_execution(QueryExecutionId=query_id)
                state = status['QueryExecution']['Status']['State']
                if state == 'SUCCEEDED':
                    break
                elif state in ['FAILED', 'CANCELLED']:
                    self.print_result("Athena Integration", False, f"Query {state}")
                    return False
                time.sleep(1)
            
            # Get results
            result = athena.get_query_results(QueryExecutionId=query_id)
            if result['ResultSet']['Rows'] and len(result['ResultSet']['Rows']) > 1:
                count = result['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0')
                self.print_result("Athena Integration", True, f"batch_summary has {count} records")
                return True
            else:
                self.print_result("Athena Integration", False, "No results from Athena")
                return False
                
        except Exception as e:
            self.print_result("Athena Integration", False, str(e))
            return False
    
    def test_compare_speed_vs_batch(self):
        """Test 6: Compare Speed and Batch values (should be different)"""
        self.print_header("TEST 6: Speed vs Batch Comparison")
        
        try:
            # Get speed data
            speed_response = self.s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=100)
            speed_trades = []
            if 'Contents' in speed_response:
                for obj in speed_response['Contents']:
                    try:
                        resp = self.s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                        trade = json.loads(resp['Body'].read().decode('utf-8'))
                        if trade.get('product') == 'BTC-USD':
                            speed_trades.append(trade)
                    except:
                        pass
            
            speed_count = len(speed_trades)
            
            # Get batch data for BTC-USD
            resp = self.s3.get_object(Bucket=S3_BUCKET, Key='batch/batch_summary.json')
            batch_data = json.loads(resp['Body'].read().decode('utf-8'))
            batch_count = 0
            for item in batch_data:
                if item.get('product') == 'BTC-USD':
                    batch_count = item.get('total_trades', 0)
                    break
            
            # Speed should be less than or equal to batch (speed is last 5 min, batch is all time)
            if speed_count > 0 and batch_count > 0:
                if speed_count <= batch_count:
                    self.print_result("Speed <= Batch", True, f"Speed: {speed_count}, Batch: {batch_count} (Speed is subset of batch)")
                else:
                    self.print_result("Speed <= Batch", False, f"Speed: {speed_count} > Batch: {batch_count} (Speed should not exceed batch)")
            else:
                if speed_count == 0 and batch_count == 0:
                    self.print_result("Speed vs Batch", True, "Both have 0 trades")
                else:
                    self.print_result("Speed vs Batch", False, f"Speed: {speed_count}, Batch: {batch_count}")
            
            return True
        except Exception as e:
            self.print_result("Speed vs Batch Comparison", False, str(e))
            return False
    
    def run_all_tests(self):
        """Run all tests and print summary"""
        print("\n" + "="*70)
        print("🧪 BATCH PROCESSOR TEST SUITE")
        print("="*70)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        # Run all tests
        self.test_s3_batch_exists()
        self.test_batch_data_content()
        self.test_buy_sell_counts()
        self.test_batch_processor_execution()
        self.test_athena_integration()
        self.test_compare_speed_vs_batch()
        
        # Print summary
        self.print_header("TEST SUMMARY")
        total = self.results['total']
        passed = self.results['passed']
        failed = self.results['failed']
        
        print(f"\n  ✅ Passed: {passed}/{total}")
        print(f"  ❌ Failed: {failed}/{total}")
        
        if failed == 0:
            print(f"\n  🎉 ALL TESTS PASSED! Batch processor is working correctly!")
        else:
            print(f"\n  ⚠️ {failed} test(s) failed. Please check the errors above.")
        
        # Print detailed results
        print("\n📋 Detailed Results:")
        for test in self.results['tests']:
            status = "✅" if test['passed'] else "❌"
            print(f"  {status} - {test['name']}")
        
        print("\n" + "="*70)
        
        return failed == 0

if __name__ == "__main__":
    tester = BatchTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
