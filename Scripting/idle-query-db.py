import os
import requests
import psycopg2
import pandas as pd
import boto3
import logging
from kubernetes import client as k8s_client, config as k8s_config
from psycopg2 import extras
from botocore.exceptions import ClientError

# ------------------- Logging Setup -------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger()

# ------------------- Database Connection -------------------
con = psycopg2.connect(
    host='your-db-hostname',  # Replace with your DB host
    user='your-db-username',  # Replace with your DB username
    password='your-db-password',  # Replace with your DB password
    port='5432',  # Replace with your DB port if different
    database='your-db-name'  # Replace with your DB name
)
cur_ = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ------------------- Get Idle Connections -------------------
idle_query = '''
SELECT * FROM pg_stat_activity WHERE query NOT ILIKE '%pg_stat_activity%' ORDER BY query_start DESC;
'''
cur_.execute(idle_query)
idle_rows = cur_.fetchall()
idle_df = pd.DataFrame(idle_rows)
idle_count = len(idle_df)
idle_df.drop(columns=['client_hostname'], inplace=True, errors='ignore')

# Convert datetime columns to string
for col in ['backend_start', 'query_start', 'state_change', 'xact_start']:
    if col in idle_df.columns:
        idle_df[col] = idle_df[col].astype(str)

# ------------------- Get EC2 IP to Name Mapping -------------------
def get_ec2_ip_name_mapping(region='your-region'):  # Replace your-region, e.g. 'ap-south-1'
    ec2 = boto3.client('ec2', region_name=region)
    ip_name_map = {}
    try:
        paginator = ec2.get_paginator('describe_instances')
        for page in paginator.paginate():
            for reservation in page['Reservations']:
                for instance in reservation['Instances']:
                    if instance.get('PrivateIpAddress'):
                        ip = instance['PrivateIpAddress'].strip()
                        name = "Unknown"
                        for tag in instance.get('Tags', []):
                            if tag['Key'] == 'Name':
                                name = tag['Value']
                        ip_name_map[ip] = name
    except ClientError as e:
        log.error(f"Could not fetch EC2 instance names: {e}")
    return ip_name_map

# ------------------- Get EKS Pod IP to Pod Name Mapping -------------------
def get_eks_pod_ip_name_mapping():
    try:
        k8s_config.load_kube_config()
        v1 = k8s_client.CoreV1Api()
        pod_ip_map = {}
        pods = v1.list_pod_for_all_namespaces(watch=False)
        for pod in pods.items:
            pod_ip = pod.status.pod_ip
            if pod_ip:
                pod_ip_map[pod_ip.strip()] = pod.metadata.name
        return pod_ip_map
    except Exception as e:
        log.error(f"Could not fetch pod IP mappings from EKS: {e}")
        return {}

# ------------------- Combine EC2 + EKS Mapping -------------------
ip_to_name = get_ec2_ip_name_mapping()
ip_to_name.update({
    "10.0.1.100": "Manual-App-Server",
    "10.0.1.101": "Manual-ETL-Job"
})
pod_ip_to_name = get_eks_pod_ip_name_mapping()

# ------------------- Create DataFrames -------------------
ip_name_df = pd.DataFrame(list(ip_to_name.items()), columns=['client_addr', 'instance_name'])
pod_ip_df = pd.DataFrame(list(pod_ip_to_name.items()), columns=['client_addr', 'pod_name'])

def clean_ip(df, column):
    return df[column].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

idle_df['client_addr'] = clean_ip(idle_df, 'client_addr')
ip_name_df['client_addr'] = clean_ip(ip_name_df, 'client_addr')
pod_ip_df['client_addr'] = clean_ip(pod_ip_df, 'client_addr')

# ------------------- Save Raw Excel Files -------------------
idle_file_path = 'idle_connections_prod_db.xlsx'
final_file_path = 'idle_connections_with_hostname.xlsx'

if idle_count > 0:
    idle_df.to_excel(idle_file_path, index=False)
    # Merge EC2 instance names
    merged_df = pd.merge(idle_df, ip_name_df, on='client_addr', how='left')
    # Merge pod names
    merged_df = pd.merge(merged_df, pod_ip_df, on='client_addr', how='left')
    # Resolve hostname
    merged_df['clienthostname'] = merged_df.apply(
        lambda row: row['instance_name'] if pd.notna(row['instance_name']) and row['instance_name'] != 'Unknown'
        else row['pod_name'] if pd.notna(row['pod_name'])
        else 'Unmapped IP',
        axis=1
    )
    # Save final file
    merged_df.to_excel(final_file_path, index=False)
    matched = merged_df['clienthostname'].ne('Unmapped IP').sum()
    log.info(f"Final merged Excel saved: {final_file_path}")
    log.info(f"{matched} of {len(merged_df)} IPs successfully mapped.")
else:
    log.info("No idle connections found. Skipping file save and notification.")

# ------------------- Slack Integration -------------------
# Slack token and channel removed for security reasons
# You can add your own Slack bot token and channel if needed

# def send_message_to_slack(channel_id, token, message):
#     ...

# def upload_file_to_slack(file_path, channel_id, token):
#     ...

# ------------------- Send Slack Notification -------------------
# if idle_count > 0:
#     send_message_to_slack(CHANNEL_ID, SLACK_BOT_TOKEN, message_text)
#     upload_file_to_slack(final_file_path, CHANNEL_ID, SLACK_BOT_TOKEN)
# else:
#     log.info("No idle connections found.")
