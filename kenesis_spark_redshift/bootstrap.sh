#!/usr/bin/env bash
# Usage: Script to setup and initiate a data pipeline from Kinesis to Redshift
# Env: Unix Bash
# Version: 1.0
# Commit History:
# 15/07/21 - Initial version

cd ~/
export HOME=$(pwd)
source ~/.bashrc
sudo yum update -y

MVN_DOWNLOAD_LOCATION="https://mirrors.estointernet.in/apache/maven/maven-3/3.8.1/binaries/apache-maven-3.8.1-bin.tar.gz"
SPARK_VERSION="2.4.8"
HADOOP_VERSION="2.7.3"
JDK_VERSION="1.8.0"
RS_DRIVER_VERSION="1.2.20.1043"
KS_REPO_LOCATION="https://github.com/qubole/kinesis-sql.git"
KS_REPO_NAME="kinesis-sql"
KS_REPO_BRANCH="2.4"
KS_CONNECTOR_NAME="spark-sql-kinesis_2.11-1.2.1_spark-2.4-SNAPSHOT.jar"
GIT_REPO_LOCATION="https://github.com/sankamuk/aws-kinesis-redshift-sparkstream.git"
GIT_REPO_NAME="aws-kinesis-redshift-sparkstream"
PIPELINE_SCRIPT="stream_data.py"

print(){
  msg_t="$(echo $1 | tr [a-z] [A-Z])"
  msg="$2"
  if [ $(echo ${msg_t} | grep -qi "error" ; echo $?) -eq 0 ]
  then
    printf "[%-20s] [%-5s] %-100s\n" "$(date)" "${msg_t}" "$msg"
  else
    printf "[%-20s] [%-5s] %-100s\n" "$(date)" "${msg_t}" "$msg"
  fi

}

#__main__
print "info" "Bootstraping pipeline."

print "info" "Installing JDK."
sudo yum install -y -q java-${JDK_VERSION}-openjdk.x86_64 && sleep 10s && \
sudo yum install -y -q java-${JDK_VERSION}-openjdk-devel.x86_64 && sleep 30s
if [ $? -ne 0 ]
then
  print "error" "Cannot install JDK."
  exit 1
fi

print "info" "Installing Git."
sudo yum install -y -q git
if [ $? -ne 0 ]
then
  print "error" "Cannot install Git."
  exit 1
fi

print "info" "Installing Maven."
[ -d ${HOME}/mvn ] && rm -rf ${HOME}/mvn
DWN_SUFIX=$(echo ${MVN_DOWNLOAD_LOCATION} | awk -F. '{ print $NF }')
wget --quiet ${MVN_DOWNLOAD_LOCATION} && tar -zxvf apache-maven*.${DWN_SUFIX} && \
rm -f apache-maven*.${DWN_SUFIX} && mv ${HOME}/apache-maven* ${HOME}/mvn >/dev/null
if [ $? -ne 0 ]
then
  print "error" "Cannot install Maven."
  exit 1
fi

print "info" "Installing Spark."
[ -d ${HOME}/spark ] && rm -rf ${HOME}/spark
wget --quiet https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && \
tar -zxvf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz && mv spark-${SPARK_VERSION}-bin-hadoop2.7 spark && \
rm -f spark-${SPARK_VERSION}-bin-hadoop2.7.tgz >/dev/null
if [ $? -ne 0 ]
then
  print "error" "Cannot install Spark."
  exit 1
fi

print "info" "Installing Hadoop."
[ -d ${HOME}/hadoop ] && rm -rf ${HOME}/hadoop
wget --quiet https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
tar -zxvf hadoop-${HADOOP_VERSION}.tar.gz && mv hadoop-${HADOOP_VERSION} hadoop && \
rm -f hadoop-${HADOOP_VERSION}.tar.gz >/dev/null
if [ $? -ne 0 ]
then
  print "error" "Cannot install Hadoop."
  exit 1
fi

print "info" "Installing Redshift Driver."
[ -f ${HOME}/RedshiftJDBC4-no-awssdk-${RS_DRIVER_VERSION}.jar ] && \
rm -rf ${HOME}/RedshiftJDBC4-no-awssdk-${RS_DRIVER_VERSION}.jar
wget --quiet \
https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/${RS_DRIVER_VERSION}/RedshiftJDBC4-no-awssdk-${RS_DRIVER_VERSION}.jar \
>/dev/null 
if [ $? -ne 0 ]
then
  print "error" "Cannot install Driver."
  exit 1
fi

print "info" "Installing Git Repo for Kinesis driver."
[ -d ${HOME}/${KS_REPO_NAME} ] && rm -rf ${HOME}/${KS_REPO_NAME}
[ -d ${HOME}/${KS_CONNECTOR_NAME} ] && rm -rf ${HOME}/${KS_CONNECTOR_NAME}
git clone ${KS_REPO_LOCATION} >/dev/null 
if [ $? -ne 0 ]
then
  print "error" "Cannot install Git Repo."
  exit 1
fi
cd ${HOME}/${KS_REPO_NAME}
git checkout ${KS_REPO_BRANCH}
if [ $? -ne 0 ]
then
  print "error" "Cannot switch to required branch."
  exit 1
fi
${HOME}/mvn/bin/mvn install -DskipTests >/dev/null
if [ $? -ne 0 ]
then
  print "error" "Cannot build connector jar."
  exit 1
fi
mv ${HOME}/${KS_REPO_NAME}/target/${KS_CONNECTOR_NAME} ${HOME}/
cd ${HOME}
rm -rf ${HOME}/${KS_REPO_NAME}

print "info" "Installing Application Git Repo."
[ -d ${HOME}/${GIT_REPO_NAME} ] && rm -rf ${HOME}/${GIT_REPO_NAME}
git clone ${GIT_REPO_LOCATION} >/dev/null
if [ $? -ne 0 ]
then
  print "error" "Cannot install Git Repo."
  exit 1
fi

print "info" "Validating pipeline script."
if [ ! -f ${HOME}/${GIT_REPO_NAME}/${PIPELINE_SCRIPT} ]
then
  print "error" "Cannot identify pipeline script in repo."
  exit 1
fi

print "info" "Setting up logging."
[ -d ${HOME}/logs ] && rm -rf ${HOME}/logs
mkdir -p ${HOME}/logs
if [ ! -d ${HOME}/logs ]
then
  print "error" "Cannot set log directory."
  exit 1
fi

APP_CLASSPATH=$(find ${HOME}/hadoop/share/hadoop/tools/lib/ -name '*.jar' | xargs echo | tr ' ' ',')
APP_CLASSPATH="${APP_CLASSPATH},${HOME}/RedshiftJDBC4-no-awssdk-${RS_DRIVER_VERSION}.jar,${HOME}/${KS_CONNECTOR_NAME}"
print "info" "APP_CLASSPATH : ${APP_CLASSPATH}"

print "info" "Starting pipeline."
echo "${HOME}/spark/bin/spark-submit --jars ${APP_CLASSPATH} \
--conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' \
--conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' \
${HOME}/${GIT_REPO_NAME}/${PIPELINE_SCRIPT}" > ${HOME}/logs/comandline

${HOME}/spark/bin/spark-submit --jars ${APP_CLASSPATH} \
--conf spark.driver.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' \
--conf spark.executor.extraJavaOptions='-Dcom.amazonaws.services.s3.enableV4' \
${HOME}/${GIT_REPO_NAME}/${PIPELINE_SCRIPT} > ${HOME}/logs/stdout 2> ${HOME}/logs/stderr &

print "info" "Waiting for pipeline to start."
sleep 15s

print "info" "Validating pipeline status."
if [ $(jps | grep -vi jps | wc -l) -lt 1 ]
then
  print "error" "Cannot start pipeline."
  exit 1
fi

print "info" "Pipeline succesfully started."