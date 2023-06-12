import boto3
import time

def create_cluster():
    client = boto3.client('emr', region_name='us-east-2') 
    instances = {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.large',
                'InstanceCount': 1,
            },
            {
                'Name': "Worker nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 5,
            }
        ],
        'Ec2KeyName': 'labs',  
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    }
    response = client.run_job_flow(
        Name='EMR_Lab',
        ReleaseLabel='emr-5.36.0',
        Instances=instances,
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Spark'}
        ]
    )
    return response['JobFlowId']

def add_step(cluster_id):
    client = boto3.client('emr', region_name='us-east-2') 
    step = {
        'Name': 'EMR_Lab_Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://emr-demo-2023/updated_pyspark_emr_script.py", "--output_path","s3://emr-demo-2023/output/emrlab_output.parquet"]
        }
    }
    response = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return response['StepIds']

def wait_for_cluster_ready(cluster_id):
    emr = boto3.client('emr', region_name='us-east-2') 
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        status = response['Cluster']['Status']['State']
        if status in ['WAITING', 'RUNNING']:
            print(f"Cluster {cluster_id} is ready")
            break
        elif status in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
            raise Exception(f"Cluster {cluster_id} entered state {status}")
        else:
            print(f"Waiting for cluster {cluster_id} to be ready (current state: {status})")
        time.sleep(30) 

def main():
    cluster_id = create_cluster()
    print(f"Created cluster {cluster_id}")
    wait_for_cluster_ready(cluster_id)
    step_id = add_step(cluster_id)[0]
    print(f"Added step {step_id} to cluster {cluster_id}")


if __name__ == "__main__":
    main()
