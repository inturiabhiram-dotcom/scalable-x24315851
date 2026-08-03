Project Flow
Data Ingestion
Coinbase WebSocket feed streams live trade data

Python producer connects to WebSocket and pushes trades to AWS Kinesis

Kinesis acts as the streaming buffer for all incoming data

Speed Layer (Real-Time Processing)
AWS Lambda function triggered by Kinesis records

Processes each trade individually with low latency

Calculates processing latency and enriches trade data

Stores processed trades in S3 (speed layer)

Batch Layer (Historical Processing)
PySpark job runs on Cloud9 environment

Reads historical trade data from S3 speed folder

Computes comprehensive aggregates (averages, volume, buy/sell distribution)

Stores batch summaries in S3 (batch layer)

Runs every 5 minutes for up-to-date historical views

Serving Layer (Query & Visualization)
AWS Athena enables SQL querying on S3 data

Flask web application serves as the dashboard

Plotly powers interactive charts and graphs

Real-time data updates every 3 seconds

Windowed Stream Processing
Separate window processor runs alongside Lambda

Maintains sliding windows (5-minute windows, 1-minute slides)

Computes windowed aggregates for real-time trend analysis

Auto-Scaling
Cloud9 auto-scaler monitors CPU utilization

Scales EC2 instances up when CPU exceeds threshold 

Scales down when CPU drops below threshold

Lambda auto-scales automatically based on incoming request volume

Data Storage
Raw trades stored in S3 (raw layer)

Processed trades in S3 (speed layer)

Batch summaries in S3 (batch layer)

Window metrics in S3 (window layer)

Athena query results in S3

Monitoring & Comparison
Performance benchmark compares sequential vs parallel execution

Metrics comparison tab shows speed vs batch layer differences

Scaling advisor recommends scaling actions based on CPU/Memory usage

10-minute data collection script generates reports and graphs


How to Run
Setup
Clone the repository

Ensure AWS credentials are configured with appropriate permissions (Kinesis, Lambda, S3, Athena, EC2, CloudWatch)

Make the startup script executable

Start Services
Run the main startup script to start all services:

Producer (Coinbase WebSocket consumer)

Flask Dashboard

Auto-Scaler

Batch Processor

Window Processor

Access Dashboard
Open web browser and navigate to localhost port 5000

Login credentials are not required

Monitor Data
Dashboard auto-refreshes with live data

Price charts update in real-time

Trades table shows latest transactions

Scaling status shows Lambda and Cloud9 metrics

Batch vs Speed comparison available in metrics tab

Collect Performance Data
Run the 10-minute collection script

Generates time-series graphs and comparison charts

Creates CSV files for report tables

Saves all results in organized folders

Stop Services
Use the stop command to gracefully terminate all running services