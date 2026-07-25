# #!/bin/bash
# # run_all.sh - Complete pipeline with S3 raw storage

# echo "=========================================="
# echo "🚀 Starting Scalable Cloud Analytics Pipeline"
# echo "=========================================="

# mkdir -p logs

# # Stop existing processes
# echo -e "\n🛑 Stopping existing processes..."
# pkill -f "producer.py" 2>/dev/null
# pkill -f "speed_processor.py" 2>/dev/null
# pkill -f "app.py" 2>/dev/null
# pkill -f "mapreduce_complete.py" 2>/dev/null
# pkill -f "batch_from_s3.py" 2>/dev/null
# sleep 2

# # Start Producer (sends to Kinesis AND S3/raw/)
# echo -e "\n📡 Starting Producer..."
# nohup python3 producer.py > logs/producer.log 2>&1 &
# sleep 2
# echo "✅ Producer started"

# # Start Speed Processor (reads from Kinesis, saves to S3/speed/)
# echo -e "\n⚡ Starting Speed Processor..."
# nohup python3 speed_processor.py > logs/speed_processor.log 2>&1 &
# sleep 2
# echo "✅ Speed Processor started"

# # Wait for data
# echo -e "\n⏳ Waiting for data to accumulate (15 seconds)..."
# sleep 15

# # Run PySpark MapReduce Batch (reads from S3/raw/)
# echo -e "\n📦 Running PySpark MapReduce Batch Processing..."
# echo "   Reading from S3/raw/ (Full History)..."
# python3 mapreduce_complete.py > logs/batch.log 2>&1

# if [ $? -eq 0 ]; then
#     echo "✅ Batch processing completed successfully"
# else
#     echo "⚠️ Batch processing had issues (check logs/batch.log)"
# fi

# # Start Dashboard
# echo -e "\n🌐 Starting Dashboard..."
# nohup python3 app.py > logs/dashboard.log 2>&1 &
# sleep 3
# echo "✅ Dashboard started"

# # Get IP
# PUBLIC_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "localhost")

# echo -e "\n=========================================="
# echo "✅ All Services Started!"
# echo "=========================================="
# echo "🌐 Dashboard: http://$PUBLIC_IP:5000"
# echo ""
# echo "📊 Service Types:"
# echo "  • Producer:      Coinbase → Kinesis + S3/raw/"
# echo "  • Speed Layer:   Kinesis → S3/speed/ (Real-time windows)"
# echo "  • Batch Layer:   S3/raw/ → S3/batch/ (PySpark MapReduce)"
# echo "  • Dashboard:     S3 → Web UI"
# echo ""
# echo "📝 Logs:"
# echo "  • Producer:       tail -f logs/producer.log"
# echo "  • Speed Processor: tail -f logs/speed_processor.log"
# echo "  • Batch (PySpark): tail -f logs/batch.log"
# echo "  • Dashboard:      tail -f logs/dashboard.log"
# echo ""
# echo "🔍 Check data:"
# echo "  aws s3 ls s3://x24315851-scalable-s3/raw/ | wc -l   (raw files)"
# echo "  aws s3 ls s3://x24315851-scalable-s3/batch/         (batch results)"
# echo ""
# echo "🛑 Stop all: pkill -f 'producer|speed_processor|app|mapreduce_complete'"
# echo "=========================================="










#!/bin/bash
# run_all.sh - Complete pipeline with continuous batch

echo "=========================================="
echo "🚀 Starting Scalable Cloud Analytics Pipeline"
echo "=========================================="

mkdir -p logs

# Stop existing processes
echo -e "\n🛑 Stopping existing processes..."
pkill -f "producer.py" 2>/dev/null
pkill -f "speed_processor.py" 2>/dev/null
pkill -f "app.py" 2>/dev/null
pkill -f "mapreduce_complete.py" 2>/dev/null
sleep 2

# Start Producer
echo -e "\n📡 Starting Producer..."
nohup python3 producer.py > logs/producer.log 2>&1 &
sleep 2
echo "✅ Producer started"

# Start Speed Processor
echo -e "\n⚡ Starting Speed Processor..."
nohup python3 speed_processor.py > logs/speed_processor.log 2>&1 &
sleep 2
echo "✅ Speed Processor started"

# Wait for data
echo -e "\n⏳ Waiting for data to accumulate (15 seconds)..."
sleep 15

# Start Continuous Batch Processing
echo -e "\n📦 Starting Continuous Batch Processing..."
echo "   Batch will run every 60 seconds"
nohup python3 mapreduce_complete.py > logs/batch.log 2>&1 &
echo "✅ Continuous batch processor started"

# Start Dashboard
echo -e "\n🌐 Starting Dashboard..."
nohup python3 app.py > logs/dashboard.log 2>&1 &
sleep 3
echo "✅ Dashboard started"

# Get IP
PUBLIC_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "localhost")

echo -e "\n=========================================="
echo "✅ All Services Started!"
echo "=========================================="
echo "🌐 Dashboard: http://$PUBLIC_IP:5000"
echo ""
echo "📊 Service Types:"
echo "  • Producer:      Coinbase → Kinesis + S3/raw_batch/"
echo "  • Speed Layer:   Kinesis → S3/speed/ (Real-time windows)"
echo "  • Batch Layer:   S3/raw_batch/ → S3/batch/ (Runs every 60s)"
echo "  • Dashboard:     S3 → Web UI"
echo ""
echo "📝 Logs:"
echo "  • Producer:       tail -f logs/producer.log"
echo "  • Speed Processor: tail -f logs/speed_processor.log"
echo "  • Batch:          tail -f logs/batch.log"
echo "  • Dashboard:      tail -f logs/dashboard.log"
echo ""
echo "🛑 Stop all: pkill -f 'producer|speed_processor|app|mapreduce_complete'"
echo "=========================================="
