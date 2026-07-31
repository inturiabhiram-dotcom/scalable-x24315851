
#!/bin/bash
# test_scaling.sh - Test auto-scaling

echo "Testing Auto-Scaling..."
echo "Current instances:"
aws autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names scalable-autoscaling \
    --query "AutoScalingGroups[0].Instances[].InstanceId" \
    --output text | wc -w

echo "Scaling up to 2 instances..."
aws autoscaling set-desired-capacity \
    --auto-scaling-group-name scalable-autoscaling \
    --desired-capacity 1

sleep 30

echo "Current instances after scale up:"
aws autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names scalable-autoscaling \
    --query "AutoScalingGroups[0].Instances[].InstanceId" \
    --output text | wc -w

echo "Scaling back down to 1..."
aws autoscaling set-desired-capacity \
    --auto-scaling-group-name scalable-autoscaling \
    --desired-capacity 1

echo "Done!"
