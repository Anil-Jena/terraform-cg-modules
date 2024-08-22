import boto3

ec2 = boto3.client('ec2')

def lambda_handler(event, context):
    # Replace these with your specific instance and volume details
    old_instance_id = 'i-xxxxxxxxxxxxxxx'  # The ID of the old EC2 instance
    new_instance_id = 'i-yyyyyyyyyyyyyyy'  # The ID of the new EC2 instance
    device_name = '/dev/sdf'               # The device name for the new volume (e.g., /dev/sdf)
    
    # Describe old instance volumes
    old_instance = ec2.describe_instances(InstanceIds=[old_instance_id])
    
    # Get the volume ID of the root volume of the old instance
    root_volume_id = old_instance['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][0]['Ebs']['VolumeId']
    
    # Create a snapshot of the old instance's volume
    snapshot = ec2.create_snapshot(VolumeId=root_volume_id, Description="Snapshot from old EC2")
    snapshot_id = snapshot['SnapshotId']
    
    print(f"Snapshot created: {snapshot_id}")
    
    # Wait for the snapshot to complete
    waiter = ec2.get_waiter('snapshot_completed')
    waiter.wait(SnapshotIds=[snapshot_id])
    
    # Create a new volume from the snapshot
    new_volume = ec2.create_volume(
        SnapshotId=snapshot_id,
        AvailabilityZone=old_instance['Reservations'][0]['Instances'][0]['Placement']['AvailabilityZone'],
        VolumeType='gp2',  # You can change this to your preferred volume type
    )
    new_volume_id = new_volume['VolumeId']
    
    print(f"New volume created: {new_volume_id}")
    
    # Wait for the volume to become available
    waiter = ec2.get_waiter('volume_available')
    waiter.wait(VolumeIds=[new_volume_id])
    
    # Attach the new volume to the new EC2 instance
    ec2.attach_volume(
        VolumeId=new_volume_id,
        InstanceId=new_instance_id,
        Device=device_name
    )
    
    print(f"Volume {new_volume_id} attached to instance {new_instance_id} as {device_name}")
    
    return {
        'statusCode': 200,
        'body': f"Volume {new_volume_id} attached to instance {new_instance_id}"
    }
