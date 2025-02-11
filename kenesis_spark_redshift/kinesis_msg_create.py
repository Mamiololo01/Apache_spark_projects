#!/usr/bin/env python
# Usage: Push test message to Kinesis Stream
#        Need boto3 installed
#        You should give the host Kinesis access before executing stream
# Env: Python 3 on EC2(with Kinesis Role Added)
# Version: 1.0
# Commit History:
# 15/07/21 - Initial version

import boto3
import sys

def create_client(region_name='us-east-1'):
    print("Creating client")
    return boto3.client('kinesis', region_name)

def send_data(client, stream_name, data, shard_num=1):
    print(f"Sending data {data} to Kinesis Stream {stream_name}")
    en_data = bytes(data, 'utf-8')
    payload = {
        "Data": en_data,
        "PartitionKey": str(shard_num)
    }
    try:
        client.put_records(
                    Records=[payload],
                    StreamName=stream_name
                )
    except Exception as e:
        print("Error: Could not send your message. Detail provided below.")
        print(e)
        sys.exit(1)

if __name__ == '__main__':
    c = create_client(sys.argv[1])
    if len(sys.argv) != 4 :
        print("Usage: Pass two argument. 1. AWS Region, 2. Kinesis Stream name, 3. Message.")
        sys.exit(1)
    send_data(c, sys.argv[2], sys.argv[3])