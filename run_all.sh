# #!/bin/bash
# # run_all.sh - Complete pipeline with scheduled PySpark MapReduce

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
# sleep 2

# # Start Producer
# echo -e "\n📡 Starting Producer (REAL-TIME INGESTION)..."
# nohup python3 producer.py > logs/producer.log 2>&1 &
# sleep 2
# echo "✅ Producer started"

# # Start Speed Processor
# echo -e "\n⚡ Starting Speed Processor (REAL-TIME PROCESSING)..."
# nohup python3 speed_processor.py > logs/speed_processor.log 2>&1 &
# sleep 2
# echo "✅ Speed Processor started"

# # Start Scheduled Batch Processing (runs every 30 minutes)
# echo -e "\n📦 Starting PySpark MapReduce Batch Processing (SCHEDULED)..."
# echo "   Batch will run every 30 minutes"
# echo "   First batch running immediately..."
# nohup python3 mapreduce_complete.py --scheduled > logs/batch.log 2>&1 &
# echo "✅ PySpark MapReduce batch processor started (scheduled)"

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
# echo ""
# echo "📊 Service Types:"
# echo "  • Producer:      REAL-TIME (Coinbase → Kinesis)"
# echo "  • Speed Layer:   REAL-TIME (Kinesis → S3/speed/, every 5 min windows)"
# echo "  • Batch Layer:   BATCH (Kinesis → S3/batch/, every 30 minutes)"
# echo "  • Dashboard:     QUERY (S3 → Web UI, real-time updates)"
# echo ""
# echo "🌐 Dashboard: http://$PUBLIC_IP:5000"
# echo ""
# echo "📝 Log Files:"
# echo "  • Producer:       tail -f logs/producer.log"
# echo "  • Speed Processor: tail -f logs/speed_processor.log"
# echo "  • Batch (PySpark): tail -f logs/batch.log"
# echo "  • Dashboard:      tail -f logs/dashboard.log"
# echo ""
# echo "🔧 Batch Configuration:"
# echo "  • Interval: 30 minutes (configurable in mapreduce_complete.py)"
# echo "  • Max Records: 10,000 per batch"
# echo ""
# echo "🛑 Stop all: pkill -f 'producer|speed_processor|app|mapreduce_complete'"
# echo "=========================================="



#!/bin/bash
# run_all.sh - Complete pipeline with S3 raw storage

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
pkill -f "batch_from_s3.py" 2>/dev/null
sleep 2

# Start Producer (sends to Kinesis AND S3/raw/)
echo -e "\n📡 Starting Producer..."
nohup python3 producer.py > logs/producer.log 2>&1 &
sleep 2
echo "✅ Producer started"

# Start Speed Processor (reads from Kinesis, saves to S3/speed/)
echo -e "\n⚡ Starting Speed Processor..."
nohup python3 speed_processor.py > logs/speed_processor.log 2>&1 &
sleep 2
echo "✅ Speed Processor started"

# Wait for data
echo -e "\n⏳ Waiting for data to accumulate (15 seconds)..."
sleep 15

# Run PySpark MapReduce Batch (reads from S3/raw/)
echo -e "\n📦 Running PySpark MapReduce Batch Processing..."
echo "   Reading from S3/raw/ (Full History)..."
python3 mapreduce_complete.py > logs/batch.log 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Batch processing completed successfully"
else
    echo "⚠️ Batch processing had issues (check logs/batch.log)"
fi

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
echo "  • Producer:      Coinbase → Kinesis + S3/raw/"
echo "  • Speed Layer:   Kinesis → S3/speed/ (Real-time windows)"
echo "  • Batch Layer:   S3/raw/ → S3/batch/ (PySpark MapReduce)"
echo "  • Dashboard:     S3 → Web UI"
echo ""
echo "📝 Logs:"
echo "  • Producer:       tail -f logs/producer.log"
echo "  • Speed Processor: tail -f logs/speed_processor.log"
echo "  • Batch (PySpark): tail -f logs/batch.log"
echo "  • Dashboard:      tail -f logs/dashboard.log"
echo ""
echo "🔍 Check data:"
echo "  aws s3 ls s3://x24315851-scalable-s3/raw/ | wc -l   (raw files)"
echo "  aws s3 ls s3://x24315851-scalable-s3/batch/         (batch results)"
echo ""
echo "🛑 Stop all: pkill -f 'producer|speed_processor|app|mapreduce_complete'"
echo "=========================================="
