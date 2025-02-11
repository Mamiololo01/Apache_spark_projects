#!/bin/bash
set -x

echo "export AWS_ACCESS_ID=${aws_access_id}" >> ~/.bashrc
echo "export AWS_ACCESS_SECRET=${aws_access_secret}" >> ~/.bashrc
echo "export KINESIS_URL=${kinesis_url}" >> ~/.bashrc
echo "export KINESIS_STREAM=${kinesis_stream}" >> ~/.bashrc
echo "export REDSHIFT_TABLE=${redshift_table}" >> ~/.bashrc
echo "export REDSHIFT_CLUSTER=${redshift_cluster}" >> ~/.bashrc
echo "export REDSHIFT_DB=${redshift_db}" >> ~/.bashrc
echo "export REDSHIFT_USER=${redshift_user}" >> ~/.bashrc
echo "export REDSHIFT_PASSWORD=${redshift_password}" >> ~/.bashrc
echo "export AWS_S3_ENDPOINT=${aws_s3_endpoint}" >> ~/.bashrc
echo "export S3_BUCKET=${s3_bucket}" >> ~/.bashrc
echo "export S3_BACKUP_HOME=${s3_backup_home}" >> ~/.bashrc
wget https://raw.githubusercontent.com/sankamuk/aws-kinesis-redshift-sparkstream/main/bootstrap.sh
/usr/bin/chmod 755 bootstrap.sh
./bootstrap.sh > /tmp/bootstrap.sh.log 2> /tmp/bootstrap.sh.log