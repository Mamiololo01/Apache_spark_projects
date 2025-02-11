## Spark Structured Streaming Data Pipeline from AWS Kinesis to AWS Redshift Python Terraform Apache Spark AWS

### Overview
Spark Structured Streaming data pipeline. Pushes data from Kinesis Stream to a Redshift Cluster. The pipeline keeps data backup in S3 per day basis for easy replaying of data in case required.

Kinesis Stream  ------>  Spark Streaming Application  ------>  Redshift Cluster Table
                                                       |
                                                        ---->  S3 (Backup)
                                                        

Note currently the teraform automation supports EC2 based deployment but in case the in future the scale up required EMR can be choosen to deploy the pipeline.

### Scalability

This pipeline is powered by Apache Spark which has the ability to scale out as per requirement both vertically and horizontally. In this sample we have choosen an Micro EC2 instance with single core but in an deployment we can scale up the core to increase solutions ability to precess messages. Also if this is not meeting requirement we can look into more scalable solution like EMR and even custom deployment in Kubernetes, e.g. Spark Standalone on Kubernetes, Kubernetes as Cluster Manager. 

Now how much parallelism Kinesis (with increased shards) and Redshift support need to be tested.

### Fault Tolerence

This set presented doesnot consifer this aspect but it can easily be incorporated into the solution:

Process recovery can be added with a monitoring solution for single node EC2, for EMR solution YARN handles the same and for Kubernetes we have multiple container recovery options.

For checkpoint data recovery, can be handled either with a external EBS volume attached to the EC2, Distributed storage for EMR or Kubernetes Volume for Kubernetes deployment.

### Deliverable details

Pyspark Structured Streaming script to execute the pipeline.
Bootstrap script to setup an EC2 host to execute the pipeline.
Teraform directory to host teraform scripts to setup and configure EC2 host for pipeline.
Preexecution setup
Before you can launch your pipeline you should complete the following:

Kinesis Stream - You should create a Kinesis Stream, note your stream name and Kinesis url. Note currently tested with stream on same AWS account.

Redshift Cluster - You should create a Redshift Cluster in the same AWS account and note cluster jdbc url, admin user and password. You can alse create a custom database or use the default (i.e. dev). 

Note You should create a Security Group with In-Bound rule from EC2 instance to allow TCP connection to 5439 (Redshift Service) and attach it to the Cluster.

S3 Bucket - You should create a bucket and also a directory dedicated for data backup. Note the bucket and directory name.

IAM User - You should provide read access to Kinesis stream and write access to Redshift cluster. Create access key and note the id and secret for it. This we will use for programatic access to AWS services.

Terraform - Install terraform to use automated pipeline launch.

You should be ready now to launch your pipeline.

### Execution

Once ready to launch move to teraform directory and launch teraform.

terraform init

terraform apply

Note on launching you will be asked to provide below detail.

| Input | Description |
| --- | --- |
| account_key | Key name of the Key Pair to use for the instance |
| access_key | Your AWS account Access Key |
| secret_key | Your AWS account Secret Key |
| aws_access_id | IAM user access key id with access to read Kinesis and write to  |
| aws_access_secret | IAM user access key secret with access to read Kinesis and write to Redshift |
| kinesis_url | Kinesis_url |
| kinesis_stream | Kinesis stream name |
| redshift_table | Redshift table as sink |
| redshift_cluster | Redshift cluster JDBC url |
| redshift_db | Redshift cluster database |
| redshift_user | Redshift cluster user |
| redshift_password | Redshift cluster password |
| aws_s3_endpoint | S3 endpoint url |
| s3_bucket | S3 bucket name |
| s3_backup_home | S3 directory for data backup |

With good setup your pipeline should be up and running.

### Troubleshooting

You should be able to see logs in root users home, inside logs directory.

Cross Account Kinesis as a source (Not tested)

To access Kinesis source in different AWS account then the one our Redshift Cluster and pipeline is placed we can follow the delegation access process, described in https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html.

NOTE When using cross account Kinesis your read stream you should use one additional parameter shown below:

    input_df = spark.readStream \
        .format("kinesis") \
        .option("streamName", os.getenv('KINESIS_STREAM')) \
        .option("endpointUrl", os.getenv('KINESIS_URL')) \
        .option("awsAccessKeyId", os.getenv('AWS_ACCESS_ID')) \
        .option("awsSecretKey", os.getenv('AWS_ACCESS_SECRET')) \
        .option("awsSTSRoleARN", os.getenv('AWS_ACCESS_ARN')) \
        .option("startingposition", "latest") \
        .load()

You need to set the environment variable AWS_ACCESS_ARN with value arn:aws:iam::<External Account Id>:role/<External Account Role>.

### Todo

Better monitoring and logging setup.
This solution uses Spark checkpointing feature to preserve the Stream State. Now the implementation provided uses local volume to preserve the state. This may not be good practice for production. For production if you want to go with EC2 (rather than EMR) use a EBS volume and attach to the instance and set your checkpointig directory inside the EBS volume.