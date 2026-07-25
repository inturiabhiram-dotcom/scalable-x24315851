
#!/usr/bin/env python3
"""
System Test Script - Tests all components of the Scalable Cloud Analytics Pipeline
Run: python3 test_system.py
"""

import boto3
import json
import requests
import time
from datetime import datetime, timezone
import os
import sys

print("="*70)
print("🔍 SCALABLE CLOUD ANALYTICS - SYSTEM TEST")
print("="*70)

# Configuration
S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"
DASHBOARD_URL = "http://localhost:5000"

# Colors for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
NC = '\033[0m'  # No Color

def print_section(title):
    print("\n" + "="*70)
    print(f"{BLUE}{title}{NC}")
    print("="*70)

def print_result(test_name, passed, details=""):
    status = f"{GREEN}✅ PASS{NC}" if passed else f"{RED}❌ FAIL{NC}"
    print(f"  {status} - {test_name}")
    if details:
        print(f"       {details}")

def test_aws_credentials():
    """Test 1: AWS Credentials"""
    print_section("TEST 1: AWS Credentials")
    try:
        sts = boto3.client('sts', region_name=REGION)
        identity = sts.get_caller_identity()
        print_result("AWS Credentials", True, f"User: {identity['Arn']}")
        return True
    except Exception as e:
        print_result("AWS Credentials", False, str(e))
        return False

def test_s3_bucket():
    """Test 2: S3 Bucket Access"""
    print_section("TEST 2: S3 Bucket")
    try:
        s3 = boto3.client('s3', region_name=REGION)
        s3.head_bucket(Bucket=S3_BUCKET)
        print_result("S3 Bucket Access", True, f"Bucket: {S3_BUCKET}")
        
        # Check for speed files
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=10)
        speed_count = len(response.get('Contents', []))
        print_result("Speed Files Exist", speed_count > 0, f"Found {speed_count} speed files")
        
        # Check for window files
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='window/', MaxKeys=10)
        window_count = len(response.get('Contents', []))
        print_result("Window Files Exist", window_count > 0, f"Found {window_count} window files")
        
        # Check for batch files
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='batch/', MaxKeys=10)
        batch_count = len(response.get('Contents', []))
        print_result("Batch Files Exist", batch_count > 0, f"Found {batch_count} batch files")
        
        return True
    except Exception as e:
        print_result("S3 Bucket Access", False, str(e))
        return False

def test_kinesis():
    """Test 3: Kinesis Stream"""
    print_section("TEST 3: Kinesis Stream")
    try:
        kinesis = boto3.client('kinesis', region_name=REGION)
        stream_name = "x24315851-kinesis-stream"
        response = kinesis.describe_stream(StreamName=stream_name)
        status = response['StreamDescription']['StreamStatus']
        print_result("Kinesis Stream", status == 'ACTIVE', f"Status: {status}")
        
        # Get shard count
        shard_count = len(response['StreamDescription']['Shards'])
        print_result("Shards Available", shard_count > 0, f"Shards: {shard_count}")
        
        return True
    except Exception as e:
        print_result("Kinesis Stream", False, str(e))
        return False

def test_athena():
    """Test 4: Athena Tables"""
    print_section("TEST 4: Athena Tables")
    try:
        athena = boto3.client('athena', region_name=REGION)
        
        # Test speed_trades table
        query = "SELECT COUNT(*) as count FROM coinbase_analytics.speed_trades"
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'coinbase_analytics'},
            ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/athena-results/'}
        )
        query_id = response['QueryExecutionId']
        
        # Wait for completion
        time.sleep(3)
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            result = athena.get_query_results(QueryExecutionId=query_id)
            if result['ResultSet']['Rows'] and len(result['ResultSet']['Rows']) > 1:
                count = result['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0')
                print_result("speed_trades table", True, f"Records: {count}")
            else:
                print_result("speed_trades table", True, "Empty table")
        else:
            print_result("speed_trades table", False, f"Query state: {state}")
        
        # Test batch_summary table
        query = "SELECT COUNT(*) as count FROM coinbase_analytics.batch_summary"
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'coinbase_analytics'},
            ResultConfiguration={'OutputLocation': f's3://{S3_BUCKET}/athena-results/'}
        )
        query_id = response['QueryExecutionId']
        time.sleep(3)
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            result = athena.get_query_results(QueryExecutionId=query_id)
            if result['ResultSet']['Rows'] and len(result['ResultSet']['Rows']) > 1:
                count = result['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0')
                print_result("batch_summary table", True, f"Records: {count}")
            else:
                print_result("batch_summary table", True, "Empty table")
        else:
            print_result("batch_summary table", False, f"Query state: {state}")
        
        return True
    except Exception as e:
        print_result("Athena Test", False, str(e))
        return False

def test_processes():
    """Test 5: Running Processes"""
    print_section("TEST 5: Running Processes")
    try:
        import subprocess
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        output = result.stdout
        
        producer_running = 'producer.py' in output
        speed_running = 'speed_processor.py' in output
        app_running = 'app.py' in output
        
        print_result("Producer Process", producer_running)
        print_result("Speed Processor Process", speed_running)
        print_result("Flask App Process", app_running)
        
        return producer_running and speed_running and app_running
    except Exception as e:
        print_result("Process Check", False, str(e))
        return False

def test_dashboard_api():
    """Test 6: Dashboard API Endpoints"""
    print_section("TEST 6: Dashboard API")
    try:
        # Test status endpoint
        response = requests.get(f"{DASHBOARD_URL}/api/status", timeout=5)
        status_ok = response.status_code == 200
        print_result("Status Endpoint", status_ok, f"Status: {response.status_code}")
        
        # Test speed endpoint
        response = requests.get(f"{DASHBOARD_URL}/api/speed", timeout=5)
        speed_ok = response.status_code == 200
        if speed_ok:
            data = response.json()
            trades = data.get('trades', 0)
            avg_price = data.get('avg_price', 0)
            print_result("Speed Endpoint", True, f"Trades: {trades}, Avg Price: ${avg_price}")
        else:
            print_result("Speed Endpoint", False, f"Status: {response.status_code}")
        
        # Test batch endpoint
        response = requests.get(f"{DASHBOARD_URL}/api/batch", timeout=5)
        batch_ok = response.status_code == 200
        if batch_ok:
            data = response.json()
            trades = data.get('trades', 0)
            avg_price = data.get('avg_price', 0)
            print_result("Batch Endpoint", True, f"Trades: {trades}, Avg Price: ${avg_price}")
        else:
            print_result("Batch Endpoint", False, f"Status: {response.status_code}")
        
        # Test windows endpoint
        response = requests.get(f"{DASHBOARD_URL}/api/windows", timeout=5)
        windows_ok = response.status_code == 200
        if windows_ok:
            data = response.json()
            windows = len(data.get('windows', []))
            print_result("Windows Endpoint", True, f"Windows: {windows}")
        else:
            print_result("Windows Endpoint", False, f"Status: {response.status_code}")
        
        # Test merged endpoint (most important)
        response = requests.get(f"{DASHBOARD_URL}/api/merged", timeout=5)
        merged_ok = response.status_code == 200
        if merged_ok:
            data = response.json()
            speed_trades = data.get('speed_layer', {}).get('trades', 0)
            batch_trades = data.get('batch_layer', {}).get('trades', 0)
            windows = len(data.get('window_metrics', []))
            print_result("Merged Endpoint", True, f"Speed: {speed_trades}, Batch: {batch_trades}, Windows: {windows}")
            
            # Print the actual data for inspection
            print(f"\n  {YELLOW}Merged Data Sample:{NC}")
            print(f"    Speed Layer: {json.dumps(data.get('speed_layer', {}), indent=2)}")
            print(f"    Batch Layer: {json.dumps(data.get('batch_layer', {}), indent=2)}")
            if windows > 0:
                print(f"    Windows: {windows} windows available")
        else:
            print_result("Merged Endpoint", False, f"Status: {response.status_code}")
        
        return merged_ok
    except requests.exceptions.ConnectionError:
        print_result("Dashboard API", False, "Connection refused - Dashboard not running")
        return False
    except Exception as e:
        print_result("Dashboard API", False, str(e))
        return False

def test_s3_data_content():
    """Test 7: S3 Data Content"""
    print_section("TEST 7: S3 Data Content")
    try:
        s3 = boto3.client('s3', region_name=REGION)
        
        # Read a sample speed file
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=5)
        if 'Contents' in response and len(response['Contents']) > 0:
            key = response['Contents'][0]['Key']
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            content = resp['Body'].read().decode('utf-8')
            try:
                trade = json.loads(content)
                print_result("Sample Speed File", True, f"Product: {trade.get('product')}, Price: ${trade.get('price')}")
                print(f"  {YELLOW}Sample Trade:{NC}")
                print(f"    {json.dumps(trade, indent=2)}")
            except json.JSONDecodeError:
                print_result("Sample Speed File", False, "Invalid JSON")
        else:
            print_result("Sample Speed File", False, "No speed files found")
        
        # Read a sample window file
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='window/', MaxKeys=5)
        if 'Contents' in response and len(response['Contents']) > 0:
            key = response['Contents'][0]['Key']
            resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
            content = resp['Body'].read().decode('utf-8')
            try:
                window = json.loads(content)
                print_result("Sample Window File", True, f"Product: {window.get('product')}, Trades: {window.get('trade_count')}")
            except json.JSONDecodeError:
                print_result("Sample Window File", False, "Invalid JSON")
        else:
            print_result("Sample Window File", False, "No window files found")
        
        return True
    except Exception as e:
        print_result("S3 Data Content", False, str(e))
        return False

def main():
    """Run all tests"""
    print("\n" + "="*70)
    print(f"{GREEN}🚀 Starting System Tests...{NC}")
    print("="*70)
    
    results = []
    
    # Run all tests
    results.append(("AWS Credentials", test_aws_credentials()))
    results.append(("S3 Bucket", test_s3_bucket()))
    results.append(("Kinesis Stream", test_kinesis()))
    results.append(("Athena Tables", test_athena()))
    results.append(("Running Processes", test_processes()))
    results.append(("Dashboard API", test_dashboard_api()))
    results.append(("S3 Data Content", test_s3_data_content()))
    
    # Summary
    print_section("TEST SUMMARY")
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for test_name, result in results:
        status = f"{GREEN}✅ PASS{NC}" if result else f"{RED}❌ FAIL{NC}"
        print(f"  {status} - {test_name}")
    
    print("\n" + "="*70)
    if passed == total:
        print(f"{GREEN}✅ ALL TESTS PASSED! ({passed}/{total}){NC}")
        print("\n🎉 Your system is fully operational!")
        print(f"🌐 Dashboard URL: http://localhost:5000")
    else:
        print(f"{RED}❌ {total - passed} TEST(S) FAILED ({passed}/{total}){NC}")
        print("\n🔧 Please fix the failed tests above.")
        
        # Provide specific recommendations
        if not results[0][1]:  # AWS Credentials
            print("\n  💡 Fix AWS credentials: Run 'aws configure'")
        if not results[4][1]:  # Running Processes
            print("\n  💡 Start services: ./run_all.sh")
        if not results[5][1]:  # Dashboard API
            print("\n  💡 Check dashboard: tail -f logs/dashboard.log")
        if not results[2][1]:  # Kinesis
            print("\n  💡 Check Kinesis: aws kinesis describe-stream --stream-name x24315851-kinesis-stream")
    
    print("="*70 + "\n")
    
    # Print the merged API response for debugging
    print_section("DEBUG: Raw API Response")
    try:
        response = requests.get(f"{DASHBOARD_URL}/api/merged", timeout=5)
        if response.status_code == 200:
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
