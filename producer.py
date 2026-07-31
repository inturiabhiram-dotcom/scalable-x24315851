# producer.py - Sends to Kinesis AND batches raw data to S3
import json
import time
import boto3
from websocket import WebSocketApp
from datetime import datetime, timezone
import threading

STREAM_NAME = "x24315851-kinesis-stream"
S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"
BATCH_SIZE = 100  # Number of records per batch file

kinesis = boto3.client("kinesis", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

WS_URL = "wss://ws-feed.exchange.coinbase.com"

last_sent_time = 0
trade_buffer = []
buffer_lock = threading.Lock()

def flush_buffer():
    """Flush the buffer to S3 as a single JSON file"""
    global trade_buffer
    
    with buffer_lock:
        if not trade_buffer:
            return
        
        # Create a batch file with timestamp
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        batch_key = f"raw_batch/batch_{timestamp}_{len(trade_buffer)}.json"
        
        # Write all trades as a JSON array
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=batch_key,
            Body=json.dumps(trade_buffer, indent=2),
            ContentType='application/json'
        )
        
        print(f"📦 Flushed {len(trade_buffer)} trades to S3: {batch_key}")
        trade_buffer = []

def on_open(ws):
    print("Connected to Coinbase WebSocket")
    products = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"]

    ws.send(json.dumps({
        "type": "subscribe",
        "channels": [{
            "name": "matches",
            "product_ids": products
        }]
    }))

def on_message(ws, message):
    global last_sent_time, trade_buffer

    current_time = time.time()

    # Allow only one record every second
    if current_time - last_sent_time < 1:
        return

    data = json.loads(message)

    if data.get("type") != "match":
        return

    trade = {
        "time": data["time"],
        "product": data["product_id"],
        "price": float(data["price"]),
        "size": float(data["size"]),
        "side": data["side"],
        "trade_id": data["trade_id"],
        "received_at": datetime.now(timezone.utc).isoformat()
    }

    # 1. Send to Kinesis (for speed layer)
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(trade),
        PartitionKey=trade["product"]
    )

    # 2. Add to buffer for batch storage
    with buffer_lock:
        trade_buffer.append(trade)
        
        # If buffer reaches BATCH_SIZE, flush it
        if len(trade_buffer) >= BATCH_SIZE:
            flush_buffer()

    last_sent_time = current_time
    print(f"Sent: {trade['product']} | ${trade['price']} (Buffer: {len(trade_buffer)}/{BATCH_SIZE})")

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    # Flush remaining trades on close
    flush_buffer()

# Schedule periodic flush every 60 seconds (in case buffer doesn't fill)
def periodic_flush():
    while True:
        time.sleep(10)
        flush_buffer()

# Start periodic flush thread
flush_thread = threading.Thread(target=periodic_flush, daemon=True)
flush_thread.start()

ws = WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()
