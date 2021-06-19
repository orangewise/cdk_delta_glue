from aws_cdk import core, aws_s3 as s3
from aws_cdk.core import Aws


class CdkDeltaGlueStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create buckets
        delta_glue_bucket = s3.Bucket(
            self,
            "cdk_delta_glue",
            bucket_name=f"{Aws.ACCOUNT_ID}-cdk-delta-glue",
        )
        core.CfnOutput(self, "cdk_delta_glue_bucket", value=delta_glue_bucket.bucket_name)
