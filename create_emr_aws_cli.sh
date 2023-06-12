aws emr create-cluster \
 --service-role EMR_DefaultRole \
 --release-label emr-5.36.0 \
 --name EMR_Lab \
 --applications Name=Spark \
 --ec2-attributes KeyName=labs,InstanceProfile=EMR_EC2_DefaultRole \
 --instance-groups InstanceType=m5.xlarge,InstanceGroupType=MASTER,InstanceCount=1 InstanceType=m5.xlarge,InstanceGroupType=CORE,InstanceCount=2 \
 --region us-east-2 \
 --managed-scaling-policy ComputeLimits='{MinimumCapacityUnits=2,MaximumCapacityUnits=4,UnitType=Instances}' \
 --steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://emr-demo-2023/updated_pyspark_emr_script.py","--output_path","s3://emr-demo-2023/output/emrlab_output.parquet"],"Type":"CUSTOM_JAR","ActionOnFailure":"CANCEL_AND_WAIT","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]'