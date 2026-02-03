# Quick Start Deployment Guide

## One-Time Setup

1. **Install CDK CLI:**
   ```bash
   npm install -g aws-cdk
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements-cdk.txt
   ```

3. **Bootstrap CDK (first time only):**
   ```bash
   cdk bootstrap
   ```

## Deploy

```bash
cdk deploy
```

## Verify

After deployment, check CloudWatch Logs:
- `/aws/lambda/SyncDataLambda` - Daily sync logs
- `/aws/lambda/ReportLambda` - Report processing logs

## Test the Pipeline

1. **Manually trigger sync Lambda** (optional):
   - Go to AWS Console → Lambda → SyncDataLambda
   - Click "Test" to run immediately

2. **Check S3 bucket** for uploaded files

3. **Check SQS queue** for messages when JSON files are uploaded

4. **Check Report Lambda logs** for analytics results

## Schedule

- **Daily sync**: Runs at midnight UTC (00:00)
- **Report processing**: Triggers automatically when JSON files are uploaded to S3

## Cleanup

```bash
cdk destroy
```

**Warning**: The S3 bucket is retained by default. Manually delete it if needed.
