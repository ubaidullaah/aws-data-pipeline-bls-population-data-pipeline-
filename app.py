#!/usr/bin/env python3
import aws_cdk as cdk
from data_pipeline_stack import DataPipelineStack

app = cdk.App()
DataPipelineStack(app, "DataPipelineStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    )
)

app.synth()
