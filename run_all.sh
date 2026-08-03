

#!/bin/bash
# run_all.sh - Complete pipeline with Custom Auto-Scaler

echo "=========================================="
echo " Starting Scalable Cloud Analytics Pipeline"
echo "=========================================="

mkdir -p logs

# Stop existing processes
echo -e "\n Stopping existing processes..."
pkill -f "producer.py" 2>/dev/null
pkill -f "speed_processor.py" 2>/dev/null
pkill -f "app.py" 2>/dev/null
pkill -f "mapreduce_complete.py" 2>/dev/null
pkill -f "custom_autoscaler.py" 2>/dev/null
sleep 2

# Start Producer
echo -e "\n Starting Producer..."
nohup python3 producer.py > logs/producer.log 2>&1 &
sleep 2
echo " Producer started"

# Start Speed Processor
echo -e "\n Starting Speed Processor..."
nohup python3 speed_processor.py > logs/speed_processor.log 2>&1 &
sleep 2
echo " Speed Processor started"

# Wait for data
echo -e "\n Waiting for data to accumulate (15 seconds)..."
sleep 15

# Start Continuous Batch Processing
echo -e "\n Starting Continuous Batch Processing..."
echo "   Batch will run every 60 seconds"
nohup python3 mapreduce_complete.py > logs/batch.log 2>&1 &
echo " Continuous batch processor started"

# Start Dashboard
echo -e "\n Starting Dashboard..."
nohup python3 app.py > logs/dashboard.log 2>&1 &
sleep 3
echo " Dashboard started"
<<<<<<< HEAD
=======

# Start Custom Auto-Scaler (NEW)
echo -e "\n Starting Custom Auto-Scaler..."
nohup python3 custom_autoscaler.py > logs/autoscaler.log 2>&1 &
echo " Auto-Scaler started (CPU threshold: 20%)"
>>>>>>> 6cc3086 (changes)

# Get IP
PUBLIC_IP=$(curl -s --max-time 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "localhost")

echo -e "\n=========================================="
echo " All Services Started!"
echo "=========================================="
echo " Dashboard: http://$PUBLIC_IP:5000"
echo ""
echo " Service Types:"
echo "  • Producer:      Coinbase → Kinesis + S3/raw_batch/"
echo "  • Speed Layer:   Kinesis → S3/speed/ (Real-time windows)"
echo "  • Batch Layer:   S3/raw_batch/ → S3/batch/ (Runs every 60s)"
echo "  • Dashboard:     S3 → Web UI"
echo "  • Auto-Scaler:   Custom (CPU-based, threshold: 20%)"
echo ""
echo " Logs:"
echo "  • Producer:       tail -f logs/producer.log"
echo "  • Speed Processor: tail -f logs/speed_processor.log"
echo "  • Batch:          tail -f logs/batch.log"
echo "  • Dashboard:      tail -f logs/dashboard.log"
echo "  • Auto-Scaler:    tail -f logs/autoscaler.log"
echo ""
<<<<<<< HEAD
echo " Stop all: pkill -f 'producer|speed_processor|app|mapreduce_complete'"
echo "=========================================="



=======
echo " Stop all: pkill -f 'producer|speed_processor|app|mapreduce_complete|custom_autoscaler'"
echo "=========================================="
>>>>>>> 6cc3086 (changes)
