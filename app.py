

# app.py - Flask Dashboard with Real-Time Data for Graphs
from flask import Flask, render_template, jsonify, request
import boto3
import json
from datetime import datetime, timezone, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
S3_BUCKET = "x24315851-scalable-s3"
REGION = "us-east-1"

s3 = boto3.client('s3', region_name=REGION)

def get_speed_data(product, limit=100):
    """SPEED LAYER: Read from S3/speed/"""
    trades = []
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='speed/', MaxKeys=limit)
        if 'Contents' not in response:
            return []
        
        for obj in response['Contents']:
            try:
                key = obj['Key']
                resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
                trade = json.loads(resp['Body'].read().decode('utf-8'))
                if trade.get('product') == product:
                    trades.append(trade)
            except Exception as e:
                pass
    except Exception as e:
        logger.error(f"S3 error: {e}")
    return trades

def get_windows(product):
    """Read window data from S3/window/"""
    windows = []
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'window/{product}/', MaxKeys=20)
        if 'Contents' not in response:
            return windows
        
        sorted_objs = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        for obj in sorted_objs[:10]:
            try:
                resp = s3.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                w = json.loads(resp['Body'].read().decode('utf-8'))
                windows.append({
                    "window_start": w.get('window_start', ''),
                    "window_end": w.get('window_end', ''),
                    "trade_count": w.get('trade_count', 0),
                    "avg_price": w.get('avg_price', 0),
                    "max_price": w.get('max_price', 0),
                    "min_price": w.get('min_price', 0),
                    "total_volume": w.get('total_volume', 0),
                    "buy_count": w.get('buy_count', 0),
                    "sell_count": w.get('sell_count', 0)
                })
            except Exception as e:
                pass
    except Exception as e:
        pass
    return windows

def get_batch_data(product):
    """BATCH LAYER: Read ONLY from S3/batch/batch_summary.json"""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key='batch/batch_summary.json')
        data = json.loads(resp['Body'].read().decode('utf-8'))
        for item in data:
            if item.get('product') == product:
                return {
                    "trades": item.get('total_trades', 0),
                    "avg_price": item.get('average_price', 0),
                    "max_price": item.get('maximum_price', 0),
                    "min_price": item.get('minimum_price', 0),
                    "volume": item.get('total_volume', 0),
                    "buys": item.get('buy_trades', 0),
                    "sells": item.get('sell_trades', 0),
                    "avg_latency": item.get('avg_latency_ms', 0)
                }
    except Exception as e:
        logger.warning(f"Batch data not found: {e}")
    return None

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/status")
def status():
    try:
        return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})
    except:
        return jsonify({"status": "error"}), 500

@app.route("/api/speed")
def speed():
    """SPEED LAYER - Only last 5 minutes from S3/speed/"""
    try:
        product = request.args.get('product', 'BTC-USD')
        trades = get_speed_data(product, 200)
        
        if not trades:
            return jsonify({"product": product, "trades": 0, "avg_price": 0, "volume": 0, "avg_latency": 0})
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=5)
        recent = []
        for t in trades:
            try:
                ts_str = t.get('processed_at', '').replace('Z', '+00:00')
                ts = datetime.fromisoformat(ts_str)
                if ts >= cutoff:
                    recent.append(t)
            except Exception as e:
                recent.append(t)
        
        if not recent:
            recent = trades[-30:] if len(trades) > 30 else trades
        
        prices = [t['price'] for t in recent]
        sizes = [t['size'] for t in recent]
        latencies = [t.get('latency_ms', 0) for t in recent]
        
        return jsonify({
            "product": product,
            "trades": len(recent),
            "avg_price": round(sum(prices)/len(prices), 2) if prices else 0,
            "volume": round(sum(sizes), 4) if sizes else 0,
            "avg_latency": round(sum(latencies)/len(latencies), 0) if latencies else 0
        })
    except Exception as e:
        logger.error(f"Speed error: {e}")
        return jsonify({"product": product, "trades": 0, "avg_price": 0, "volume": 0, "avg_latency": 0})

@app.route("/api/batch")
def batch():
    try:
        product = request.args.get('product', 'BTC-USD')
        batch_data = get_batch_data(product)
        
        if batch_data:
            return jsonify({"product": product, **batch_data})
        
        return jsonify({
            "product": product,
            "trades": 0,
            "avg_price": 0,
            "max_price": 0,
            "min_price": 0,
            "volume": 0,
            "buys": 0,
            "sells": 0,
            "avg_latency": 0,
            "status": "Batch processing not yet run. Please run batch processor.",
            "source": "batch_layer"
        })
    except Exception as e:
        logger.error(f"Batch error: {e}")
        return jsonify({
            "product": product,
            "trades": 0,
            "avg_price": 0,
            "max_price": 0,
            "min_price": 0,
            "volume": 0,
            "buys": 0,
            "sells": 0,
            "avg_latency": 0,
            "error": str(e)
        })

@app.route("/api/windows")
def windows():
    try:
        product = request.args.get('product', 'BTC-USD')
        return jsonify({"windows": get_windows(product)})
    except Exception as e:
        logger.error(f"Windows error: {e}")
        return jsonify({"windows": []})

@app.route("/api/merged")
def merged():
    try:
        product = request.args.get('product', 'BTC-USD')
        
        speed_resp = speed()
        speed_json = speed_resp.get_json() if hasattr(speed_resp, 'get_json') else {}
        
        batch_resp = batch()
        batch_json = batch_resp.get_json() if hasattr(batch_resp, 'get_json') else {}
        
        window_resp = windows()
        window_json = window_resp.get_json() if hasattr(window_resp, 'get_json') else {}
        
        response = {
            "product": product,
            "speed_layer": {
                "trades": speed_json.get('trades', 0),
                "avg_price": speed_json.get('avg_price', 0),
                "volume": speed_json.get('volume', 0),
                "avg_latency": speed_json.get('avg_latency', 0)
            },
            "batch_layer": {
                "trades": batch_json.get('trades', 0),
                "avg_price": batch_json.get('avg_price', 0),
                "max_price": batch_json.get('max_price', 0),
                "min_price": batch_json.get('min_price', 0),
                "volume": batch_json.get('volume', 0),
                "buys": batch_json.get('buys', 0),
                "sells": batch_json.get('sells', 0),
                "avg_latency": batch_json.get('avg_latency', 0),
                "status": batch_json.get('status', 'batch_available')
            },
            "window_metrics": window_json.get('windows', []),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"Merged error: {e}")
        return jsonify({
            "product": product,
            "speed_layer": {"trades": 0, "avg_price": 0, "volume": 0, "avg_latency": 0},
            "batch_layer": {"trades": 0, "avg_price": 0, "max_price": 0, "min_price": 0, "volume": 0, "buys": 0, "sells": 0, "avg_latency": 0},
            "window_metrics": [],
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

@app.route("/api/benchmark")
def benchmark():
    try:
        trades = get_speed_data('BTC-USD', 200)
        if not trades:
            return jsonify({"sequential": {"time_ms": 0, "records": 0}, "parallel": {}, "speedup": 0})
        
        start = time.time()
        results = {}
        for t in trades:
            p = t.get('product', 'unknown')
            if p not in results:
                results[p] = {'count': 0, 'sum': 0}
            results[p]['count'] += 1
            results[p]['sum'] += t.get('price', 0)
        seq_time = (time.time() - start) * 1000
        
        def worker(chunk):
            r = {}
            for t in chunk:
                p = t.get('product', 'unknown')
                if p not in r:
                    r[p] = {'count': 0, 'sum': 0}
                r[p]['count'] += 1
                r[p]['sum'] += t.get('price', 0)
            return r
        
        par_results = {}
        for threads in [2, 4]:
            start = time.time()
            chunk_size = max(1, len(trades) // threads)
            chunks = [trades[i:i+chunk_size] for i in range(0, len(trades), chunk_size)]
            
            with ThreadPoolExecutor(max_workers=threads) as ex:
                futures = [ex.submit(worker, chunk) for chunk in chunks]
                all_results = []
                for f in futures:
                    try:
                        all_results.append(f.result(timeout=5))
                    except:
                        pass
            
            par_time = (time.time() - start) * 1000
            par_results[f"{threads}_threads"] = {
                "time_ms": round(par_time, 2),
                "threads": threads,
                "success_rate": f"{len(all_results)}/{threads}",
                "success_count": len(all_results)
            }
        
        best_par = min([v['time_ms'] for v in par_results.values()])
        speedup = round(seq_time / best_par, 2) if best_par > 0 else 0
        
        return jsonify({
            "sequential": {"time_ms": round(seq_time, 2), "records": len(trades)},
            "parallel": par_results,
            "speedup": speedup,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/window_history")
def window_history():
    """Get window history for real-time graphs"""
    try:
        product = request.args.get('product', 'BTC-USD')
        windows = get_windows(product)
        
        # Format for Chart.js
        timestamps = []
        trade_counts = []
        avg_prices = []
        volumes = []
        
        for w in windows[:20]:  # Last 20 windows
            try:
                ts = datetime.fromisoformat(w['window_start'].replace('Z', '+00:00'))
                timestamps.append(ts.strftime('%H:%M:%S'))
                trade_counts.append(w['trade_count'])
                avg_prices.append(w['avg_price'])
                volumes.append(w['total_volume'])
            except:
                pass
        
        return jsonify({
            "timestamps": timestamps,
            "trade_counts": trade_counts,
            "avg_prices": avg_prices,
            "volumes": volumes
        })
    except Exception as e:
        logger.error(f"Window history error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/speed_history")
def speed_history():
    """Get speed history for real-time graphs"""
    try:
        product = request.args.get('product', 'BTC-USD')
        
        # Get all speed data and aggregate by minute
        trades = get_speed_data(product, 500)
        
        if not trades:
            return jsonify({"timestamps": [], "trades": [], "prices": [], "volumes": []})
        
        # Group by minute
        minute_data = {}
        for t in trades:
            try:
                ts_str = t.get('processed_at', '').replace('Z', '+00:00')
                ts = datetime.fromisoformat(ts_str)
                minute_key = ts.strftime('%H:%M')
                
                if minute_key not in minute_data:
                    minute_data[minute_key] = {'count': 0, 'price_sum': 0, 'volume_sum': 0, 'price_count': 0}
                
                minute_data[minute_key]['count'] += 1
                minute_data[minute_key]['price_sum'] += t.get('price', 0)
                minute_data[minute_key]['volume_sum'] += t.get('size', 0)
                minute_data[minute_key]['price_count'] += 1
            except:
                pass
        
        # Sort and get last 20 minutes
        sorted_minutes = sorted(minute_data.items())[-20:]
        
        timestamps = []
        trades = []
        prices = []
        volumes = []
        
        for minute, data in sorted_minutes:
            timestamps.append(minute)
            trades.append(data['count'])
            avg_price = data['price_sum'] / data['price_count'] if data['price_count'] > 0 else 0
            prices.append(round(avg_price, 2))
            volumes.append(round(data['volume_sum'], 4))
        
        return jsonify({
            "timestamps": timestamps,
            "trades": trades,
            "prices": prices,
            "volumes": volumes
        })
    except Exception as e:
        logger.error(f"Speed history error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

