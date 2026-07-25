# create_asg.py - Create Auto Scaling Group
import boto3
import json

ec2 = boto3.client('ec2')
asg = boto3.client('autoscaling')

# Get your instance details
INSTANCE_ID = 'i-0db05e897e3286e8c'
SECURITY_GROUP_ID = 'sg-038eb0d4c394b341f'
SUBNET_ID = 'subnet-0cc6e432477cb6765'

def create_launch_template():
    """Create launch template for auto-scaling"""
    try:
        response = ec2.create_launch_template(
            LaunchTemplateName='speed-layer-template',
            LaunchTemplateData={
                'ImageId': 'ami-0c02fb55956c7d316',  # Amazon Linux 2
                'InstanceType': 't3.medium',
                'SecurityGroupIds': [SECURITY_GROUP_ID],
                'UserData': '''#!/bin/bash
                # Install dependencies
                yum install -y python3-pip hadoop
                pip3 install boto3 websocket-client
                
                # Download and run speed processor
                aws s3 cp s3://x24315851-scalable-s3/speed_processor.py /home/ec2-user/
                cd /home/ec2-user
                python3 speed_processor.py &
                ''',
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
        print("✅ Launch template created")
        return True
    except Exception as e:
        print(f"Error creating launch template: {e}")
        return False

def create_auto_scaling_group():
    """Create Auto Scaling Group"""
    try:
        response = asg.create_auto_scaling_group(
            AutoScalingGroupName='speed-layer-asg',
            LaunchTemplate={
                'LaunchTemplateName': 'speed-layer-template',
                'Version': '$Latest'
            },
            MinSize=1,
            MaxSize=5,
            DesiredCapacity=1,
            VPCZoneIdentifier=SUBNET_ID,
            Tags=[
                {
                    'Key': 'project',
                    'Value': 'scalable-instance',
                    'PropagateAtLaunch': True
                }
            ]
        )
        print("✅ Auto Scaling Group created")
        return True
    except Exception as e:
        print(f"Error creating ASG: {e}")
        return False

def setup_target_tracking():
    """Set up target tracking scaling policy"""
    try:
        response = asg.put_scaling_policy(
            AutoScalingGroupName='speed-layer-asg',
            PolicyName='cpu-target-tracking',
            PolicyType='TargetTrackingScaling',
            TargetTrackingConfiguration={
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'ASGAverageCPUUtilization'
                },
                'TargetValue': 60.0,  # 60% target
                'DisableScaleIn': False
            },
            EstimatedInstanceWarmup=60
        )
        print("✅ Target tracking policy created")
        print(f"   Policy ARN: {response['PolicyARN']}")
        return True
    except Exception as e:
        print(f"Error creating policy: {e}")
        return False

if __name__ == "__main__":
    print("🔹 Setting up Auto Scaling Group...")
    create_launch_template()
    create_auto_scaling_group()
    setup_target_tracking()
    print("✅ Auto Scaling setup complete!")