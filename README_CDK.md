# AWS CDK Data Pipeline Infrastructure

This directory contains the AWS CDK infrastructure code for automating the data pipeline that combines Parts 1, 2, and 3 of the project.

## Architecture

The infrastructure includes:

1. **S3 Bucket**: Stores BLS files and population JSON data
2. **Lambda Function (sync_and_fetch)**: Combines Part 1 (BLS file sync) and Part 2 (API data fetch)
   - Scheduled to run daily via EventBridge
3. **SQS Queue**: Receives notifications when JSON files are uploaded to S3
4. **Lambda Function (report_processor)**: Executes Part 3 (data analytics and reporting)
   - Triggered by SQS messages
   - Logs report results to CloudWatch

## Prerequisites

1. **AWS CLI** configured with appropriate credentials
2. **Python 3.12** or later
3. **AWS CDK CLI** installed:
   ```bash
   npm install -g aws-cdk
   ```
4. **Docker** (optional but recommended for bundling Lambda dependencies)

**Note**: If the S3 bucket `bls-dataset-sync2` already exists, the stack will use it. If you want to create a new bucket, change the bucket name in `data_pipeline_stack.py`.

## Setup Instructions

### 1. Install Prerequisites

**Install AWS CDK CLI globally:**
```bash
npm install -g aws-cdk
```

**Install Python Dependencies:**
```bash
# Install CDK dependencies
pip install -r requirements-cdk.txt

# Note: Lambda dependencies will be automatically bundled during deployment
# But you can install them locally for testing:
pip install -r lambda_functions/requirements.txt
```

### 2. Configure AWS Credentials

Ensure your AWS CLI is configured:
```bash
aws configure
```

### 3. Bootstrap CDK (First Time Only)

If this is your first time using CDK in this AWS account/region:

```bash
cdk bootstrap
```

This creates the necessary S3 bucket and IAM roles for CDK deployments.

### 4. Deploy the Stack

```bash
# Synthesize CloudFormation template (optional, to verify)
cdk synth

# Deploy the stack
cdk deploy
```

**Note**: The first deployment may take 10-15 minutes as it:
- Creates the S3 bucket
- Builds and packages Lambda functions with dependencies (pandas, numpy, etc.)
- Creates IAM roles and policies
- Sets up EventBridge rules and S3 notifications

### 5. Verify Deployment

After deployment, you should see output with:
- BucketName
- QueueUrl
- SyncLambdaName
- ReportLambdaName

You can also verify in the AWS Console:
- **S3**: Check for bucket `bls-dataset-sync2`
- **Lambda**: Two functions should be created
- **SQS**: Queue `bls-data-s3-events` should exist
- **EventBridge**: Rule `DailySyncRule` should be active

### 4. Verify Deployment

After deployment, check:
- S3 bucket: `bls-dataset-sync2`
- SQS queue: `bls-data-s3-events`
- Lambda functions: `SyncDataLambda` and `ReportLambda`
- EventBridge rule: `DailySyncRule`

## How It Works

1. **Daily Sync (Part 1 & 2)**:
   - EventBridge triggers the `sync_and_fetch` Lambda function daily at midnight UTC
   - The function syncs BLS files from the remote source to S3
   - The function fetches population data from the API and uploads it as JSON to S3

2. **S3 Event Notification**:
   - When a JSON file is uploaded to S3, an event notification sends a message to the SQS queue

3. **Report Processing (Part 3)**:
   - The SQS queue triggers the `report_processor` Lambda function
   - The function loads BLS and population data from S3
   - It generates three reports:
     - Population statistics (2013-2018): mean and standard deviation
     - Best year report: best year for each series_id
     - Combined report: PRS30006032, Q01 with population data
   - All results are logged to CloudWatch Logs

## Monitoring

View logs in CloudWatch:
- **Sync Lambda Logs**: `/aws/lambda/SyncDataLambda`
- **Report Lambda Logs**: `/aws/lambda/ReportLambda`

## Cleanup

To remove all resources:

```bash
cdk destroy
```

**Note**: The S3 bucket is set to `RETAIN` policy, so it will not be deleted automatically. You may need to manually empty and delete it if desired.

## Configuration

- Bucket name (currently: `bls-dataset-sync2`)
- Schedule time for daily sync (currently: midnight UTC)
- Lambda memory and timeout settings
- SQS queue retention period

