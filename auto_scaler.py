
# auto_scaling.py - Complete Auto-Scaling Setup
import boto3
import time

REGION = "us-east-1"
INSTANCE_ID = "i-0db05e897e3286e8c"  # Your EC2 instance ID
SECURITY_GROUP_ID = "sg-038eb0d4c394b341f"
SUBNET_ID = "subnet-0cc6e432477cb6765"

def setup_auto_scaling():
    """Complete auto-scaling setup"""
    ec2 = boto3.client('ec2', region_name=REGION)
    asg = boto3.client('autoscaling', region_name=REGION)
    
    print("🔹 Setting up Auto Scaling...")
    
    # Get instance details
    try:
        response = ec2.describe_instances(InstanceIds=[INSTANCE_ID])
        instance = response['Reservations'][0]['Instances'][0]
        ami_id = instance['ImageId']
        instance_type = instance['InstanceType']
        print(f"✅ Using AMI: {ami_id}, Type: {instance_type}")
    except Exception as e:
        print(f"Error getting instance details: {e}")
        return
    
    # Create Launch Template
    try:
        ec2.create_launch_template(
            LaunchTemplateName='speed-layer-template',
            LaunchTemplateData={
                'ImageId': ami_id,
                'InstanceType': instance_type,
                'SecurityGroupIds': [SECURITY_GROUP_ID],
                'UserData': '''#!/bin/bash
                # Install Python and dependencies
                yum update -y
                yum install -y python3 python3-pip
                pip3 install boto3 websocket-client
                
                # Download speed processor from S3
                aws s3 cp s3://x24315851-scalable-s3/speed_processor.py /home/ec2-user/
                aws s3 cp s3://x24315851-scalable-s3/producer.py /home/ec2-user/
                cd /home/ec2-user
                nohup python3 speed_processor.py > /var/log/speed_processor.log 2>&1 &
                ''',
                'TagSpecifications': [{
                    'ResourceType': 'instance',
                    'Tags': [
                        {'Key': 'Name', 'Value': 'speed-layer-worker'},
                        {'Key': 'project', 'Value': 'scalable-instance'}
                    ]
                }]
            }
        )
        print("✅ Launch template created")
    except Exception as e:
        print(f"Launch template error: {e}")
    
    # Create Auto Scaling Group
    try:
        asg.create_auto_scaling_group(
            AutoScalingGroupName='speed-layer-asg',
            LaunchTemplate={
                'LaunchTemplateName': 'speed-layer-template',
                'Version': '$Latest'
            },
            MinSize=1,
            MaxSize=5,
            DesiredCapacity=1,
            VPCZoneIdentifier=SUBNET_ID,
            Tags=[{
                'Key': 'project',
                'Value': 'scalable-instance',
                'PropagateAtLaunch': True
            }]
        )
        print("✅ Auto Scaling Group created")
    except Exception as e:
        print(f"ASG creation error: {e}")
    
    # Create Target Tracking Policy
    try:
        asg.put_scaling_policy(
            AutoScalingGroupName='speed-layer-asg',
            PolicyName='cpu-target-tracking',
            PolicyType='TargetTrackingScaling',
            TargetTrackingConfiguration={
                'PredefinedMetricSpecification': {
                    'PredefinedMetricType': 'ASGAverageCPUUtilization'
                },
                'TargetValue': 60.0,
                'DisableScaleIn': False
            },
            EstimatedInstanceWarmup=60
        )
        print("✅ Target tracking policy created")
    except Exception as e:
        print(f"Policy creation error: {e}")
    
    # Create CloudWatch Alarm for monitoring
    try:
        cw = boto3.client('cloudwatch', region_name=REGION)
        cw.put_metric_alarm(
            AlarmName='speed-layer-high-cpu',
            ComparisonOperator='GreaterThanThreshold',
            EvaluationPeriods=2,
            MetricName='CPUUtilization',
            Namespace='AWS/EC2',
            Period=60,
            Statistic='Average',
            Threshold=60.0,
            ActionsEnabled=True,
            AlarmDescription='Alert when CPU exceeds 60%',
            Dimensions=[{'Name': 'AutoScalingGroupName', 'Value': 'speed-layer-asg'}],
            Unit='Percent'
        )
        print("✅ CloudWatch alarm created")
    except Exception as e:
        print(f"Alarm creation error: {e}")
    
    print("✅ Auto Scaling setup complete!")

if __name__ == "__main__":
    setup_auto_scaling()