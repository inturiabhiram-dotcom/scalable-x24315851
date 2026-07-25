
# run_complete.sh - Run all remaining tasks

echo "=========================================="
echo "🚀 Running Complete Project Tasks"
echo "=========================================="

# 1. Generate Performance Graphs
echo -e "\n📊 Generating Performance Graphs..."
python3 generate_graphs.py

# 2. Run MapReduce Batch
echo -e "\n📈 Running MapReduce Batch Processing..."
python3 mapreduce_complete.py

# 3. Run Benchmark
echo -e "\n⚡ Running Performance Benchmark..."
python3 benchmark_fixed.py

# 4. Check Results
echo -e "\n✅ Checking Results..."
echo "Graphs generated in graphs/"
ls -la graphs/

echo -e "\nS3 Data Check:"
aws s3 ls s3://x24315851-scalable-s3/speed/ --recursive | wc -l
echo "Speed files"

echo -e "\n=========================================="
echo "✅ Complete! All tasks completed"
echo "=========================================="


