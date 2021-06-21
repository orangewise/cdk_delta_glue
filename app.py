#!/usr/bin/env python
from os.path import basename
from aws_cdk.core import App
from cdk_delta_glue.cdk_stack import CdkDeltaGlueStack
from zipfile import ZipFile

tags = {"project": "cdk-delta-glue"}


def zip_lib(files):
    """Zip shared library files that are used in the Glue jobs."""
    with ZipFile("assets/scripts/lib.zip", "w") as libzip:
        for file in files:
            libzip.write(file, basename(file))


zip_lib(["assets/scripts/__init__.py", "assets/scripts/s3.py"])

app = App()
cdk_delta_glue_stack = CdkDeltaGlueStack(app, f"cdk-delta-glue", tags=tags)

app.synth()
