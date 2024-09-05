import boto3

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # Define your custom AMI ID, instance type, key name, security group, and subnet
    custom_ami_id = 'ami-xxxxxxxxxxxxxxxxx'  # Replace with your AMI ID
    instance_type = 't2.micro'  # Replace with your desired instance type
    key_name = 'your-key-pair-name'  # Replace with your EC2 key pair name
    security_group_ids = ['sg-xxxxxxxxxxxxxxxxx']  # Replace with your security group ID
    subnet_id = 'subnet-xxxxxxxxxxxxxxxxx'  # Replace with your subnet ID

    # Launch the EC2 instance
    response = ec2.run_instances(
        ImageId=custom_ami_id,
        InstanceType=instance_type,
        KeyName=key_name,
        MaxCount=1,
        MinCount=1,
        SecurityGroupIds=security_group_ids,
        SubnetId=subnet_id,
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': 'MyNewEC2Instance'  # Replace with your desired instance name
                    }
                ]
            }
        ]
    )

    # Retrieve the instance ID of the newly created EC2 instance
    instance_id = response['Instances'][0]['InstanceId']

    return {
        'statusCode': 200,
        'body': f'EC2 instance {instance_id} has been created successfully.'
    }
