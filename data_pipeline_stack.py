from aws_cdk import (
    Duration,
    Stack,
    CfnOutput,
    BundlingOptions,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_s3_notifications as s3n,
    RemovalPolicy,
)
from constructs import Construct


class DataPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 Bucket for storing data files
        bucket = s3.Bucket(
            self, "BlsDataBucket",
            bucket_name="bls-dataset-sync2",
            removal_policy=RemovalPolicy.RETAIN,  # Keep bucket on stack deletion
            auto_delete_objects=False,
        )

        # SQS Queue for S3 event notifications
        queue = sqs.Queue(
            self, "S3EventQueue",
            queue_name="bls-data-s3-events",
            visibility_timeout=Duration.minutes(15),  # Allow time for Lambda processing
            retention_period=Duration.days(14),
        )

        # Lambda function for Part 1 & 2: Sync BLS files and fetch API data
        sync_lambda = _lambda.Function(
            self, "SyncDataLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="sync_and_fetch.handler",
            code=_lambda.Code.from_asset(
                "lambda_functions",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                )
            ),
            timeout=Duration.minutes(15),
            memory_size=512,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
            }
        )

        # Grant permissions to sync Lambda
        bucket.grant_read_write(sync_lambda)

        # Lambda function for Part 3: Data analytics and reporting
        report_lambda = _lambda.Function(
            self, "ReportLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="report_processor.handler",
            code=_lambda.Code.from_asset(
                "lambda_functions",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                )
            ),
            timeout=Duration.minutes(15),
            memory_size=1024,  # More memory for pandas operations
            environment={
                "BUCKET_NAME": bucket.bucket_name,
            }
        )

        # Grant permissions to report Lambda
        bucket.grant_read(report_lambda)

        # Grant SQS permissions to report Lambda
        queue.grant_consume_messages(report_lambda)

        # Add SQS event source to report Lambda
        report_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                queue,
                batch_size=1,  # Process one message at a time
            )
        )

        # EventBridge Rule to trigger sync Lambda daily
        rule = events.Rule(
            self, "DailySyncRule",
            schedule=events.Schedule.cron(
                hour="0",  # Run at midnight UTC
                minute="0"
            ),
        )
        rule.add_target(targets.LambdaFunction(sync_lambda))

        # S3 Event Notification: Send message to SQS when JSON files are uploaded
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(
                suffix=".json"
            )
        )

        # Output important resource names
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "SyncLambdaName", value=sync_lambda.function_name)
        CfnOutput(self, "ReportLambdaName", value=report_lambda.function_name)
