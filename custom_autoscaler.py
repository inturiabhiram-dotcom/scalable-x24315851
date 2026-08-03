
# custom_autoscaler.py - Custom Auto-Scaling (Excludes ASG instances)
import boto3
import time
import os
import signal
import sys
from datetime import datetime, timedelta
import logging
import json
import requests

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# === Configuration ===
REGION = "us-east-1"
INSTANCE_ID = "i-07479a77ba5a4396a"  # Your main EC2 instance
AMI_ID = None
INSTANCE_TYPE = 't3.small'

# Security Group and Subnet
SECURITY_GROUP_ID = 'sg-085d5d39c9a78ec78'
SUBNET_ID = 'subnet-0e33f33159203d474'

# Scaling thresholds
CPU_HIGH_THRESHOLD = 20
CPU_LOW_THRESHOLD = 10
INSTANCE_MIN = 1
INSTANCE_MAX = 3
SCALING_COOLDOWN = 30
CHECK_INTERVAL = 15
SCALE_DOWN_WAIT = 60

# Tags for YOUR instances (CUSTOM auto-scaling)
PROJECT_TAG = {'Key': 'project', 'Value': 'scalable-instance'}
CUSTOM_TAG = {'Key': 'AutoScaling', 'Value': 'custom'}  # Only custom instances

USER_DATA_SCRIPT = '''#!/bin/bash
# Auto-scaled instance startup script
cd /home/ec2-user/project
source venv/bin/activate
nohup ./run_all.sh > /var/log/pipeline.log 2>&1 &
'''

# AWS clients
ec2 = boto3.client('ec2', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

class CustomAutoScaler:
    def __init__(self):
        self.current_capacity = 1
        self.last_scaling_time = 0
        self.scaling_in_progress = False
        self.scaling_history = []
        self.low_traffic_start_time = None
        self.worker_instances = []
        
        # Get AMI from your instance
        global AMI_ID
        try:
            response = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
            if response['Reservations']:
                AMI_ID = response['Reservations'][0]['Instances'][0]['ImageId']
                logger.info(f"Using AMI: {AMI_ID}")
            else:
                logger.error(f"Instance {INSTANCE_ID} not found!")
        except Exception as e:
            logger.error(f"Failed to get AMI: {e}")
    
    def get_worker_instances(self):
        """Get ONLY custom auto-scaling instances (exclude ASG instances)"""
        try:
            # Look for instances with BOTH tags: project AND AutoScaling=custom
            response = ec2.describe_instances(
                Filters=[
                    {'Name': 'tag:project', 'Values': [PROJECT_TAG['Value']]},
                    {'Name': 'tag:AutoScaling', 'Values': ['custom']},
                    {'Name': 'instance-state-name', 'Values': ['running']}
                ]
            )
            instances = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instances.append(instance['InstanceId'])
            
            # Always include the main instance (it has the custom tag)
            try:
                state = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
                if state['Reservations']:
                    if state['Reservations'][0]['Instances'][0]['State']['Name'] == 'running':
                        if INSTANCE_ID not in instances:
                            instances.append(INSTANCE_ID)
            except:
                pass
            
            self.worker_instances = instances
            logger.debug(f"Found {len(instances)} custom instances: {instances}")
            return instances
        except Exception as e:
            logger.error(f"Error getting instances: {e}")
            return [INSTANCE_ID]
    
    def get_average_cpu(self, instance_ids):
        """Get average CPU across all instances"""
        if not instance_ids:
            return 0

        end = datetime.utcnow()
        start = end - timedelta(minutes=1)
        total_cpu = 0
        valid_count = 0

        for instance_id in instance_ids:
            try:
                response = cw.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName='CPUUtilization',
                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                    StartTime=start,
                    EndTime=end,
                    Period=60,
                    Statistics=['Average']
                )
                datapoints = response.get('Datapoints', [])
                if datapoints:
                    avg = datapoints[0]['Average']
                    total_cpu += avg
                    valid_count += 1
            except Exception as e:
                logger.warning(f"Could not get CPU for {instance_id}: {e}")
        
        avg_cpu = total_cpu / valid_count if valid_count else 0
        return avg_cpu
    
    def launch_instance(self):
        """Launch a new EC2 instance with CUSTOM tag"""
        logger.info(" Launching new EC2 instance...")
        try:
            response = ec2.run_instances(
                ImageId=AMI_ID,
                InstanceType=INSTANCE_TYPE,
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[SECURITY_GROUP_ID],
                SubnetId=SUBNET_ID,
                UserData=USER_DATA_SCRIPT,
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            PROJECT_TAG,
                            CUSTOM_TAG,  # ← CRITICAL: Only custom instances get this tag
                            {'Key': 'Name', 'Value': 'custom-worker'}
                        ]
                    }
                ]
            )
            instance_id = response['Instances'][0]['InstanceId']
            logger.info(f" Instance {instance_id} launched successfully!")
            
            # Wait for instance to be running
            waiter = ec2.get_waiter('instance_running')
            waiter.wait(InstanceIds=[instance_id])
            logger.info(f" Instance {instance_id} is now running")
            
            return instance_id
        except Exception as e:
            logger.error(f"Failed to launch EC2 instance: {e}")
            return None
    
    def terminate_instance(self, instance_id):
        """Terminate a worker instance (never terminate main)"""
        if instance_id == INSTANCE_ID:
            logger.warning(f" Skipping termination of main instance: {instance_id}")
            return False
        
        try:
            ec2.terminate_instances(InstanceIds=[instance_id])
            logger.info(f" Terminated EC2 instance: {instance_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to terminate instance {instance_id}: {e}")
            return False
    
    def execute_scaling(self, action, reason):
        """Execute scaling action (up or down)"""
        old_capacity = self.current_capacity
        self.scaling_in_progress = True
        
        try:
            if action == "scale_up":
                if self.current_capacity < INSTANCE_MAX:
                    instance_id = self.launch_instance()
                    if instance_id:
                        self.current_capacity += 1
                        logger.info(f" SCALED UP: {old_capacity} → {self.current_capacity} | Reason: {reason}")
                        self.log_scaling_event(action, old_capacity, self.current_capacity, reason)
                else:
                    logger.info(f" At max capacity ({INSTANCE_MAX})")
                    self.scaling_in_progress = False
                    return False
                    
            elif action == "scale_down":
                if self.current_capacity > INSTANCE_MIN:
                    instances = self.get_worker_instances()
                    terminated = False
                    for instance_id in instances:
                        if instance_id != INSTANCE_ID:
                            if self.terminate_instance(instance_id):
                                self.current_capacity -= 1
                                terminated = True
                                logger.info(f"SCALED DOWN: {old_capacity} → {self.current_capacity} | Reason: {reason}")
                                self.log_scaling_event(action, old_capacity, self.current_capacity, reason)
                                break
                    
                    if not terminated:
                        logger.warning(" No instances available to terminate")
                        self.scaling_in_progress = False
                        return False
                else:
                    logger.info(f" At min capacity ({INSTANCE_MIN})")
                    self.scaling_in_progress = False
                    return False
            
            self.last_scaling_time = time.time()
            self.scaling_in_progress = False
            return True
            
        except Exception as e:
            logger.error(f"Scaling failed: {e}")
            self.scaling_in_progress = False
            return False
    
    def log_scaling_event(self, action, old_capacity, new_capacity, reason):
        """Log scaling events to file"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'old_capacity': old_capacity,
            'new_capacity': new_capacity,
            'reason': reason
        }
        self.scaling_history.append(event)
        
        try:
            with open('scaling_history.json', 'w') as f:
                json.dump(self.scaling_history, f, indent=2)
        except:
            pass
    
    def get_dashboard_status(self):
        """Check if dashboard is running"""
        try:
            response = requests.get('http://localhost:5000/api/status', timeout=2)
            return response.status_code == 200
        except:
            return False
    
    def run(self):
        """Main auto-scaling loop"""
        print("\n" + "="*60)
        print(" CUSTOM AUTO-SCALER STARTED")
        print("="*60)
        print(f" Configuration:")
        print(f"   Region: {REGION}")
        print(f"   Main Instance (protected): {INSTANCE_ID}")
        print(f"   Scale UP: CPU > {CPU_HIGH_THRESHOLD}%")
        print(f"   Scale DOWN: CPU < {CPU_LOW_THRESHOLD}% (after {SCALE_DOWN_WAIT}s)")
        print(f"   Check Interval: {CHECK_INTERVAL}s")
        print(f"   Cooldown: {SCALING_COOLDOWN}s")
        print(f"   Instances: {INSTANCE_MIN}-{INSTANCE_MAX}")
        print("="*60 + "\n")
        
        # Tag main instance with custom tag
        try:
            ec2.create_tags(
                Resources=[INSTANCE_ID],
                Tags=[PROJECT_TAG, CUSTOM_TAG]
            )
            logger.info(f" Tagged main instance: {INSTANCE_ID}")
        except Exception as e:
            logger.warning(f"Could not tag instance: {e}")
        
        # Get initial capacity
        initial_instances = self.get_worker_instances()
        self.current_capacity = len(initial_instances)
        logger.info(f" Initial capacity: {self.current_capacity}")
        
        loop_count = 0
        
        try:
            while True:
                loop_count += 1
                instances = self.get_worker_instances()
                current_count = len(instances)
                self.current_capacity = current_count
                
                # Get CPU
                cpu = self.get_average_cpu(instances)
                
                # Status update
                status_msg = f"\n[{loop_count}]  Status: {current_count} custom instances | CPU: {cpu:.1f}%"
                
                # Check if dashboard is running
                dashboard_status = " Running" if self.get_dashboard_status() else " Not responding"
                logger.info(f"{status_msg} | Dashboard: {dashboard_status}")
                
                # Determine scaling actions
                should_scale_up = False
                should_scale_down = False
                reason = ""
                
                # Scale UP conditions
                if current_count < INSTANCE_MAX and cpu > CPU_HIGH_THRESHOLD:
                    should_scale_up = True
                    reason = f"CPU {cpu:.1f}% > {CPU_HIGH_THRESHOLD}%"
                
                # Scale DOWN conditions (with delay)
                if current_count > INSTANCE_MIN and cpu < CPU_LOW_THRESHOLD:
                    if self.low_traffic_start_time is None:
                        self.low_traffic_start_time = time.time()
                        logger.info(f" Low traffic detected ({cpu:.1f}%), waiting {SCALE_DOWN_WAIT}s...")
                    elif time.time() - self.low_traffic_start_time > SCALE_DOWN_WAIT:
                        should_scale_down = True
                        reason = f"CPU {cpu:.1f}% < {CPU_LOW_THRESHOLD}% for {SCALE_DOWN_WAIT}s"
                else:
                    self.low_traffic_start_time = None
                
                # Execute scaling
                if should_scale_up:
                    if time.time() - self.last_scaling_time > SCALING_COOLDOWN:
                        self.execute_scaling("scale_up", reason)
                    else:
                        remaining = int(SCALING_COOLDOWN - (time.time() - self.last_scaling_time))
                        logger.info(f" Cooldown: {remaining}s remaining")
                
                elif should_scale_down:
                    if time.time() - self.last_scaling_time > SCALING_COOLDOWN:
                        self.execute_scaling("scale_down", reason)
                    else:
                        remaining = int(SCALING_COOLDOWN - (time.time() - self.last_scaling_time))
                        logger.info(f" Cooldown: {remaining}s remaining")
                else:
                    if loop_count % 4 == 0:
                        logger.info(" No scaling needed")
                
                time.sleep(CHECK_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("\n Auto-Scaler stopped by user")
        except Exception as e:
            logger.error(f" Error in main loop: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    scaler = CustomAutoScaler()
    scaler.run()
