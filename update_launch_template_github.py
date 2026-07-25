
# update_launch_template_github.py - Update launch template to t3.small with PySpark
import boto3
import base64
import sys

REGION = "us-east-1"
LAUNCH_TEMPLATE_NAME = "scalable-ag-template"
ASG_NAME = "scalable-autoscaling"

def get_security_group_id():
    ec2 = boto3.client('ec2', region_name=REGION)
    try:
        response = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Name', 'Values': ['speed-layer-worker']},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ],
            MaxResults=1
        )
        if response['Reservations']:
            instance = response['Reservations'][0]['Instances'][0]
            return instance['SecurityGroups'][0]['GroupId'], instance['ImageId']
    except:
        pass
    return "sg-038eb0d4c394b341f", "ami-0c02fb55956c7d316"

def create_user_data_script():
    return '''#!/bin/bash
# ============================================
# Scalable Cloud Analytics - EC2 Launch Script
# Full PySpark version for t3.small
# ============================================

set -e
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo "🚀 Starting EC2 Setup (Full PySpark)"
echo "=========================================="

# Install dependencies (including Java and PySpark)
yum update -y
yum install -y python3 python3-pip git java-11-amazon-corretto-devel

# Install Python packages (including PySpark)
pip3 install boto3 websocket-client flask pandas pyspark schedule

# Set Java environment for PySpark
alternatives --set java /usr/lib/jvm/java-11-openjdk/bin/java
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> /home/ec2-user/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /home/ec2-user/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> /home/ec2-user/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON=python3' >> /home/ec2-user/.bashrc
source /home/ec2-user/.bashrc

# Clone code from GitHub
mkdir -p /home/ec2-user/project
cd /home/ec2-user/project

# Replace with YOUR GitHub repo URL
REPO_URL="https://github.com/inturiabhiram-dotcom/scalable-x24315851.git"

if [ -d ".git" ]; then
    git pull
else
    git clone $REPO_URL .
fi

# Make scripts executable
chmod +x *.sh *.py

# Start the FULL pipeline (with PySpark)
nohup ./run_all.sh > /var/log/pipeline.log 2>&1 &

echo "✅ EC2 Setup Complete!"
echo "📝 Pipeline logs: tail -f /var/log/pipeline.log"
'''

def update_launch_template():
    ec2 = boto3.client('ec2', region_name=REGION)
    sg_id, ami_id = get_security_group_id()
    
    # Read user data script
    user_data = create_user_data_script()
    user_data_encoded = base64.b64encode(user_data.encode('utf-8')).decode('utf-8')
    
    try:
        # Create new version with t3.small
        response = ec2.create_launch_template_version(
            LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
            SourceVersion='$Default',
            LaunchTemplateData={
                'ImageId': ami_id,
                'InstanceType': 't3.small',  # t3.small for PySpark
                'SecurityGroupIds': [sg_id],
                'UserData': user_data_encoded,
                'TagSpecifications': [
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {'Key': 'Name', 'Value': 'spark-worker'},
                            {'Key': 'project', 'Value': 'scalable-instance'},
                            {'Key': 'source', 'Value': 'github'}
                        ]
                    }
                ]
            }
        )
        version = response['LaunchTemplateVersion']['VersionNumber']
        ec2.modify_launch_template(
            LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
            DefaultVersion=str(version)
        )
        print(f"✅ Launch template version {version} created (t3.small)")
        return version
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def update_asg():
    asg = boto3.client('autoscaling', region_name=REGION)
    try:
        asg.update_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            LaunchTemplate={
                'LaunchTemplateName': LAUNCH_TEMPLATE_NAME,
                'Version': '$Default'
            },
            MinSize=1,
            MaxSize=3,
            DesiredCapacity=1
        )
        print(f"✅ ASG '{ASG_NAME}' updated to t3.small")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("="*60)
    print("🚀 Updating Launch Template to t3.small (with PySpark)")
    print("="*60)
    
    version = update_launch_template()
    if version:
        update_asg()
        print(f"\n✅ Complete! Template v{version} with t3.small")
        print("\n📋 Instance Type: t3.small (2GB RAM, 2 vCPUs)")
        print("📋 Includes: PySpark + Java + Full Pipeline")
        print("\n🔄 Test: aws autoscaling set-desired-capacity --auto-scaling-group-name scalable-autoscaling --desired-capacity 2")
