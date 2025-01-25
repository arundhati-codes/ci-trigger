import boto3
import json
import pandas as pd

def get_aws_credentials():
    secret_name = "my-aws-credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(response["SecretString"])

    return secrets["AWS_ACCESS_KEY_ID"], secrets["AWS_SECRET_ACCESS_KEY"], secrets["AWS_REGION"]

# Use fetched credentials
# aws_access_key_id, aws_secret_access_key, aws_region = get_aws_credentials()



# AWS Batch Configuration
AWS_REGION = "us-east-1"
JOB_QUEUE = "kallisto-job-queue"
JOB_DEFINITION = "kallisto-sra-job"

# Initialize AWS Batch client
batch = boto3.client("batch", region_name=AWS_REGION)

def submit_batch_job(srr_id):
    """
    Submits an AWS Batch job for the given SRR ID.
    """
    response = batch.submit_job(
        jobName=f"kallisto-job-{srr_id}",
        jobQueue=JOB_QUEUE,
        jobDefinition=JOB_DEFINITION,
        containerOverrides={
            "environment": [
                {"name": "SRA_ID", "value": srr_id}
            ]
        }
    )
    print(f"Submitted AWS Batch job for {srr_id}. Job ID: {response['jobId']}")
    return response['jobId']

def main():
    # Load queue.csv
    queue_file = "queue.csv"
    df = pd.read_csv(queue_file)

    # Process only pending jobs
    for index, row in df.iterrows():
        if row['status'] == 'pending':
            srr_id = row['srr_id']
            print(f"Submitting job for {srr_id}...")

            # Submit job and update status
            job_id = submit_batch_job(srr_id)
            df.at[index, 'status'] = 'submitted'

    # Save updated queue.csv
    df.to_csv(queue_file, index=False)
    print("Updated queue.csv with submitted statuses.")

if __name__ == "__main__":
    main()





