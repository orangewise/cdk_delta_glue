#!/usr/bin/env python

from aws_cdk.core import App
from cdk_delta_glue.cdk_stack import CdkDeltaGlueStack

tags = {"project": "cdk-delta-glue"}

app = App()
cdk_delta_glue_stack = CdkDeltaGlueStack(app, f"cdk-delta-glue", tags=tags)

app.synth()
