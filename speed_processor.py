
# speed_processor.py

import json
import boto3
import time
from datetime import datetime, timezone
from collections import defaultdict, deque
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

S3_BUCKET = "x24315851-scalable-s3"
STREAM_NAME = "x24315851-kinesis-stream"

WINDOW_SIZE = 60      # 1 min
SLIDE_SIZE = 10        # 10 sec


class SpeedProcessor:

    def __init__(self):
        self.s3 = boto3.client("s3")
        self.kinesis = boto3.client("kinesis", region_name="us-east-1")

        self.windows = defaultdict(lambda: deque(maxlen=10000))
        self.last_output = {}
        self.running = True

    def process_trade(self, trade):

        product = trade.get("product")

        if not product:
            return

        trade["processed_at"] = datetime.now(timezone.utc).isoformat()

        speed_key = f"speed/{trade['trade_id']}.json"

        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=speed_key,
            Body=json.dumps(trade),
            ContentType="application/json"
        )

        self.windows[product].append(trade)

        current_time = time.time()
        window_key = int(current_time / SLIDE_SIZE) * SLIDE_SIZE

        if window_key not in self.last_output:
            self.last_output[window_key] = {}

        last = self.last_output[window_key].get(product, 0)

        if current_time - last >= SLIDE_SIZE:
            self.output_window(product, window_key)
            self.last_output[window_key][product] = current_time

    def output_window(self, product, window_key):

        window_data = list(self.windows[product])

        if not window_data:
            return

        prices = [x["price"] for x in window_data]
        sizes = [x["size"] for x in window_data]

        summary = {
            "window_start": datetime.fromtimestamp(
                window_key,
                timezone.utc
            ).isoformat(),

            "window_end": datetime.fromtimestamp(
                window_key + WINDOW_SIZE,
                timezone.utc
            ).isoformat(),

            "product": product,

            "trade_count": len(window_data),

            "avg_price": round(sum(prices) / len(prices), 2),

            "max_price": round(max(prices), 2),

            "min_price": round(min(prices), 2),

            "total_volume": round(sum(sizes), 4),

            "buy_count": sum(
                1 for x in window_data if x["side"] == "buy"
            ),

            "sell_count": sum(
                1 for x in window_data if x["side"] == "sell"
            ),

            "top_5_trades": sorted(
                window_data,
                key=lambda x: x["size"],
                reverse=True
            )[:5],

            "generated_at": datetime.now(timezone.utc).isoformat()
        }

        key = f"window/{product}/{window_key}.json"

        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(summary),
            ContentType="application/json"
        )

        logger.info(
            f"Window written: {product} ({len(window_data)} trades)"
        )

    def consume_shard(self, shard_id):

        logger.info(f"Started consumer for {shard_id}")

        iterator = self.kinesis.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType="LATEST"
            # Change to TRIM_HORIZON once if you want to replay old records
        )["ShardIterator"]

        while self.running:

            response = self.kinesis.get_records(
                ShardIterator=iterator,
                Limit=100
            )

            records = response["Records"]

            logger.info(
                f"{shard_id}: received {len(records)} records"
            )

            for record in records:

                try:
                    trade = json.loads(record["Data"])
                    self.process_trade(trade)

                except Exception as e:
                    logger.exception(e)

            iterator = response["NextShardIterator"]

            time.sleep(1)

    def consume_kinesis(self):

        response = self.kinesis.list_shards(
            StreamName=STREAM_NAME
        )

        shards = response["Shards"]

        logger.info(f"Found {len(shards)} shards")

        threads = []

        for shard in shards:

            shard_id = shard["ShardId"]

            t = threading.Thread(
                target=self.consume_shard,
                args=(shard_id,),
                daemon=True
            )

            t.start()

            threads.append(t)

        while self.running:
            time.sleep(5)

    def run(self):

        try:
            self.consume_kinesis()

        except KeyboardInterrupt:
            logger.info("Stopping...")
            self.running = False


if __name__ == "__main__":
    SpeedProcessor().run()
