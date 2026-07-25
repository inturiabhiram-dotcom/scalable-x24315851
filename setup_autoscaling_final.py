
# setup_autoscaling_light.py - LIGHTWEIGHT Auto-Scaling Setup (No PySpark)
import boto3
import json
import time
import os
import base64

REGION = "us-east-1"
S3_BUCKET = "x24315851-scalable-s3"
ASG_NAME = "scalable-autoscaling"
LAUNCH_TEMPLATE_NAME = "scalable-ag-template"

def get_current_instance_details():
    """Get details from current instance in ASG"""
    ec2 = boto3.client('ec2', region_name=REGION)
    asg = boto3.client('autoscaling', region_name=REGION)
    
    try:
        response = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[ASG_NAME])
        if response['AutoScalingGroups']:
            instances = response['AutoScalingGroups'][0].get('Instances', [])
            if instances:
                instance_id = instances[0]['InstanceId']
                print(f"Found instance: {instance_id}")
                
                resp = ec2.describe_instances(InstanceIds=[instance_id])
                instance = resp['Reservations'][0]['Instances'][0]
                return {
                    'ami_id': instance['ImageId'],
                    'instance_type': instance['InstanceType'],
                    'key_name': instance.get('KeyName', ''),
                    'security_group_ids': [sg['GroupId'] for sg in instance['SecurityGroups']]
                }
        
        print("No instances found in ASG, using default values")
        return None
    except Exception as e:
        print(f"Error getting instance details: {e}")
        return None

def update_launch_template():
    """Update launch template with LIGHTWEIGHT configuration (No PySpark)"""
    ec2 = boto3.client('ec2', region_name=REGION)
    
    details = get_current_instance_details()
    if not details:
        details = {
            'ami_id': 'ami-0c02fb55956c7d316',
            'instance_type': 't2.micro',
            'key_name': '',
            'security_group_ids': ['sg-038eb0d4c394b341f']
        }
    
    # LIGHTWEIGHT user data - NO PySpark installation
    user_data_script = '''#!/bin/bash
# Install ONLY essential packages (NO PySpark)
yum update -y
yum install -y python3 python3-pip
pip3 install boto3 websocket-client flask pandas

# Create project directory
mkdir -p /home/ec2-user/project
cd /home/ec2-user/project

# Download code from S3 (use lightweight version)
aws s3 cp s3://x24315851-scalable-s3/code/producer.py .
aws s3 cp s3://x24315851-scalable-s3/code/speed_processor.py .
aws s3 cp s3://x24315851-scalable-s3/code/app.py .
aws s3 cp s3://x24315851-scalable-s3/code/mapreduce_complete.py .
aws s3 cp s3://x24315851-scalable-s3/code/run_all.sh .
chmod +x run_all.sh

# Start the pipeline (without PySpark)
nohup ./run_all.sh > /var/log/pipeline.log 2>&1 &
'''
    
    user_data_encoded = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')
    
    try:
        response = ec2.create_launch_template_version(
            LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
            SourceVersion='$Default',
            LaunchTemplateData={
                'ImageId': details['ami_id'],
                'InstanceType': details['instance_type'],
                'SecurityGroupIds': details['security_group_ids'],
                'KeyName': details.get('key_name', ''),
                'UserData': user_data_encoded,
                'TagSpecifications': [
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {'Key': 'Name', 'Value': 'speed-layer-worker'},
                            {'Key': 'project', 'Value': 'scalable-instance'}
                        ]
                    }
                ]
            }
        )
        version_number = response['LaunchTemplateVersion']['VersionNumber']
        print(f"✅ Launch template version {version_number} created (LIGHTWEIGHT)")
        
        ec2.modify_launch_template(
            LaunchTemplateName=LAUNCH_TEMPLATE_NAME,
            DefaultVersion=str(version_number)
        )
        print(f"✅ Version {version_number} set as default")
        
        return True
    except Exception as e:
        print(f"❌ Error updating launch template: {e}")
        return False

def configure_scaling_policies():
    """Configure scaling policies for ASG"""
    asg = boto3.client('autoscaling', region_name=REGION)
    
    try:
        asg.update_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            LaunchTemplate={
                'LaunchTemplateName': LAUNCH_TEMPLATE_NAME,
                'Version': '$Default'
            },
            MinSize=1,
            MaxSize=2,  # Reduce max instances for t2.nano
            DesiredCapacity=1,
            DefaultCooldown=60
        )
        print("✅ ASG updated")
        
        # Delete existing policies
        try:
            asg.delete_policy(AutoScalingGroupName=ASG_NAME, PolicyName='cpu-target-tracking')
        except:
            pass
        
        try:
            asg.delete_policy(AutoScalingGroupName=ASG_NAME, PolicyName='cpu-step-scaling')
        except:
            pass
        
        # Create target tracking scaling policy
        response = asg.put_scaling_policy(
            AutoScalingGroupName=ASG_NAME,
            PolicyName='cpu-target-tracking',
            PolicyType='TargetTrackingScaling',
            TargetTrackingConfiguration={
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'ASGAverageCPUUtilization'
                },
                'TargetValue': 70.0,  # Higher threshold for small instances
                'DisableScaleIn': False
            },
            EstimatedInstanceWarmup=60
        )
        print(f"✅ CPU target tracking policy created")
        
        return True
    except Exception as e:
        print(f"❌ Error configuring policies: {e}")
        return False

def upload_code_to_s3():
    """Upload lightweight code to S3"""
    s3 = boto3.client('s3', region_name=REGION)
    
    files = [
        'producer.py',
        'speed_processor.py',
        'app.py',
        'mapreduce_complete.py',  # Use simple batch instead of PySpark
        'run_all.sh'
    ]
    
    print("\n📤 Uploading lightweight code to S3...")
    for file in files:
        try:
            if os.path.exists(file):
                s3.upload_file(file, S3_BUCKET, f'code/{file}')
                print(f"  ✅ Uploaded {file}")
            else:
                print(f"  ⚠️ {file} not found")
        except Exception as e:
            print(f"  ❌ Error uploading {file}: {e}")

def main():
    print("="*60)
    print(f"🚀 Configuring LIGHTWEIGHT Auto-Scaling for '{ASG_NAME}'")
    print("   (NO PySpark - Suitable for t2.nano)")
    print("="*60)
    
    upload_code_to_s3()
    
    print("\n📋 Updating launch template...")
    if not update_launch_template():
        print("❌ Failed to update launch template")
        return
    
    print("\n📋 Configuring scaling policies...")
    configure_scaling_policies()
    
    print("\n" + "="*60)
    print("✅ Lightweight Auto-Scaling Configuration Complete!")
    print("="*60)
    print(f"\n📊 Configuration Summary:")
    print(f"  • ASG Name: {ASG_NAME}")
    print("  • Min Instances: 1")
    print("  • Max Instances: 2 (reduced for t2.nano)")
    print("  • Scaling Policy: Target Tracking (70% CPU)")
    print("  • NO PySpark installed (lightweight)")

if __name__ == "__main__":
    main()
