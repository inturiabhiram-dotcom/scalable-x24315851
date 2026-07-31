
 #user_data_github.sh

#!/bin/bash
# ============================================
# Scalable Cloud Analytics - EC2 Launch Script
# Pulls code directly from GitHub
# ============================================

set -e
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo "  Starting EC2 Setup from GitHub"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Step 1: Update system
echo -e "${YELLOW}  Updating system packages...${NC}"
yum update -y

# Step 2: Install dependencies
echo -e "${YELLOW}  Installing dependencies...${NC}"
yum install -y python3 python3-pip git

# Step 3: Install Python packages (lightweight version)
echo -e "${YELLOW}  Installing Python packages...${NC}"
pip3 install boto3 websocket-client flask pandas schedule

# Step 4: Clone code from GitHub
echo -e "${YELLOW} Cloning code from GitHub...${NC}"
mkdir -p /home/ec2-user/project
cd /home/ec2-user/project

# Clone your repository (replace with your actual repo URL)
REPO_URL="https://github.com/yourusername/scalable-cloud-analytics.git"

if [ -d ".git" ]; then
    echo " Git repo exists, pulling latest..."
    git pull
else
    echo "Cloning repository..."
    git clone $REPO_URL .
fi

# Step 5: Make scripts executable
echo -e "${YELLOW} Making scripts executable...${NC}"
chmod +x *.sh
chmod +x *.py

# Step 6: Create logs directory
mkdir -p logs

# Step 7: Start the pipeline (lightweight version)
echo -e "${GREEN}  Starting the pipeline...${NC}"
if [ -f "run_all_light.sh" ]; then
    nohup ./run_all_light.sh > /var/log/pipeline.log 2>&1 &
else
    # Fallback to run_all.sh if lightweight doesn't exist
    nohup ./run_all.sh > /var/log/pipeline.log 2>&1 &
fi

echo -e "${GREEN} EC2 Setup Complete!${NC}"
echo " Pipeline logs: tail -f /var/log/pipeline.log"
echo " User data logs: tail -f /var/log/user-data.log"
