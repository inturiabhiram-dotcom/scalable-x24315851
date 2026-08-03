#!/bin/bash
# ============================================
# User Data Script for Auto-Scaling EC2 Instances
# Runs on every new instance launched by ASG
# ============================================

set -e
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo " Auto-Scaling EC2 Instance Setup"
echo "=========================================="

# Step 1: Install dependencies
echo " Installing system dependencies..."
yum update -y
yum install -y python3 python3-pip git java-11-amazon-corretto-devel

# Step 2: Install Python packages
echo " Installing Python packages..."
pip3 install boto3 websocket-client flask pandas pyspark schedule

# Step 3: Setup Java for PySpark
echo " Setting up Java environment..."
alternatives --set java /usr/lib/jvm/java-11-openjdk/bin/java
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> /home/ec2-user/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /home/ec2-user/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> /home/ec2-user/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON=python3' >> /home/ec2-user/.bashrc
source /home/ec2-user/.bashrc

# Step 4: Clone code from GitHub
echo " Cloning code from GitHub..."
mkdir -p /home/ec2-user/project
cd /home/ec2-user/project

# YOUR GITHUB REPO URL
REPO_URL="https://github.com/inturiabhiram-dotcom/scalable-x24315851.git"

if [ -d ".git" ]; then
<<<<<<< HEAD
    echo "  Git repo exists, pulling latest..."
=======
    echo " Git repo exists, pulling latest..."
>>>>>>> 6cc3086 (changes)
    git pull
else
    echo "Cloning repository..."
    git clone $REPO_URL .
fi

# Step 5: Make scripts executable
<<<<<<< HEAD
echo "  Making scripts executable..."
=======
echo " Making scripts executable..."
>>>>>>> 6cc3086 (changes)
chmod +x *.sh *.py

# Step 6: Create logs directory
mkdir -p logs

# Step 7: Start the FULL pipeline (with PySpark)
echo " Starting pipeline..."
nohup ./run_all.sh > /var/log/pipeline.log 2>&1 &

<<<<<<< HEAD
echo "  EC2 Setup Complete!"
echo "  Pipeline logs: tail -f /var/log/pipeline.log"
echo "  User data logs: tail -f /var/log/user-data.log"
=======
echo " EC2 Setup Complete!"
echo " Pipeline logs: tail -f /var/log/pipeline.log"
echo " User data logs: tail -f /var/log/user-data.log"
>>>>>>> 6cc3086 (changes)
