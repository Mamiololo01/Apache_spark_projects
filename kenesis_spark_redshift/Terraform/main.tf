provider "aws" {
    region = "${var.region}"
    access_key = "${var.access_key}"
    secret_key = "${var.secret_key}"
}

resource "aws_instance" "ks-rs-pipeline" {
    ami                    = "ami-0xxxxxxxxxxxxxx"
    instance_type          = "t2.micro"
    key_name = "${var.account_key}"
    vpc_security_group_ids = [ "${aws_security_group.ks-rs-pl-sg.name}" ]
    iam_instance_profile = "${aws_iam_instance_profile.ks-rs-pipeline-profile.name}"
    user_data = "${data.template_file.user_data.rendered}"

}

data "template_file" "user_data" {
  template = "${file("templates/user_data.tpl")}"

  vars = {
    aws_access_id = "${var.aws_access_id}"
    aws_access_secret = "${var.aws_access_secret}"
    kinesis_url = "${var.kinesis_url}"
    kinesis_stream = "${var.kinesis_stream}"
    redshift_table = "${var.redshift_table}"
    redshift_cluster = "${var.redshift_cluster}"
    redshift_db = "${var.redshift_db}"
    redshift_user = "${var.redshift_user}"
    redshift_password = "${var.redshift_password}"
    aws_s3_endpoint = "${var.aws_s3_endpoint}"
    s3_bucket = "${var.s3_bucket}"
    s3_backup_home = "${var.s3_backup_home}"
  }
}

resource "aws_iam_role" "ks-rs-pipeline-role" {
  name               = "ks-rs-pipeline-role"
  assume_role_policy = <<EOF
{
 "Version": "2012-10-17",
 "Statement": [
   {
     "Action": "sts:AssumeRole",
     "Principal": {
       "Service": "ec2.amazonaws.com"
     },
     "Effect": "Allow",
     "Sid": ""
   }
 ]
}
EOF
}

resource "aws_iam_policy" "ks-rs-pipeline-policy" {
  name        = "ks-rs-pipeline-policy"
  description = "Kinesis Redshift Pipeline Policy"
  policy      = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "kinesis:*",
                "redshift:*"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}

resource "aws_iam_policy_attachment" "ks-rs-pipeline-attach" {
  name       = "ks-rs-pipeline-attachment"
  roles      = ["${aws_iam_role.ks-rs-pipeline-role.name}"]
  policy_arn = "${aws_iam_policy.ks-rs-pipeline-policy.arn}"
}

resource "aws_iam_instance_profile" "ks-rs-pipeline-profile" {
  name  = "ks-rs-pipeline-profile"
  role = "${aws_iam_role.ks-rs-pipeline-role.name}"
}

resource "aws_security_group" "ks-rs-pl-sg" {
    name = "ks-rs-pl-sg"
    description = "Kinesis Redshift Pipeline Security Group"
    ingress {
        from_port = 22
        to_port = 22
        protocol = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }    
    egress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}