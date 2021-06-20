from aws_cdk import (
    core,
    aws_s3 as s3,
    aws_s3_deployment as s3_deploy,
    aws_glue as glue,
    aws_iam as iam,
)
from aws_cdk.core import Aws


class CdkDeltaGlueStack(core.Stack):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.add_buckets()
        self.add_role()
        self.add_scripts()
        self.add_jars()
        self.add_jobs()

    def add_jobs(self):
        glue_full_load = glue.CfnJob(
            self,
            f"delta-full-load",
            role=self.role.role_arn,
            allocated_capacity=2,
            command=glue.CfnJob.JobCommandProperty(
                name=f"glueetl",  # Apache Spark ETL job
                python_version="3",
                script_location=f"s3://{self.delta_glue_bucket.bucket_name}/glue-scripts/delta_full_load.py",
            ),
            # https://docs.aws.amazon.com/glue/latest/dg/add-job.html
            glue_version="2.0",
            # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
            default_arguments={
                "--extra-py-files": f"s3://{self.delta_glue_bucket.bucket_name}/jars/delta-core_2.11-0.6.1.jar",
                "--extra-jars": f"s3://{self.delta_glue_bucket.bucket_name}/jars/delta-core_2.11-0.6.1.jar",
                "--conf": "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            },
        )

    def add_jars(self):
        s3_deploy.BucketDeployment(
            self,
            "jar-deployment",
            sources=[s3_deploy.Source.asset("./assets/jars")],
            destination_bucket=self.delta_glue_bucket,
            destination_key_prefix="jars",
        )

    def add_scripts(self):
        """Deploy scripts to s3"""
        s3_deploy.BucketDeployment(
            self,
            "script-deployment",
            sources=[s3_deploy.Source.asset("./assets/scripts")],
            destination_bucket=self.delta_glue_bucket,
            destination_key_prefix="glue-scripts",
        )

    def add_role(self):
        policy_statement = iam.PolicyStatement(
            actions=[
                "logs:*",
                "s3:*",
                "ec2:*",
                "iam:*",
                "cloudwatch:*",
                "dynamodb:*",
                "glue:*",
            ]
        )
        policy_statement.add_all_resources()

        self.role = iam.Role(
            self,
            f"delta-glue-job-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )
        self.role.add_to_policy(policy_statement)

    def add_buckets(self):
        self.delta_glue_bucket = s3.Bucket(
            self,
            "cdk_delta_glue",
            bucket_name=f"{Aws.ACCOUNT_ID}-cdk-delta-glue",
        )
        core.CfnOutput(
            self, "cdk_delta_glue_bucket", value=self.delta_glue_bucket.bucket_name
        )
