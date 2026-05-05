"""
Step 1: Create AWS resources for the OpenLineage e2e pipeline.

Creates in us-west-2:
  - S3 bucket: ol-lineage-{account_id}
  - Glue database: ol_lineage
  - IAM role: ol-lineage-confluent-role  (trust policy updated in step 2)

Run:
    source ~/aws-session.env && python3 scripts/e2e_pipeline/01_aws_setup.py
"""

from __future__ import annotations

import json

import boto3

REGION       = "us-west-2"
GLUE_DB      = "ol_lineage"
IAM_ROLE     = "ol-lineage-confluent-role"


def get_account_id() -> str:
    return boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]


def create_s3_bucket(s3, account_id: str) -> str:
    bucket = f"ol-lineage-{account_id}"
    try:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        print(f"[S3] Created bucket: s3://{bucket}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"[S3] Bucket already exists: s3://{bucket}")
    except Exception as e:
        if "BucketAlreadyExists" in str(e):
            print(f"[S3] Bucket already exists: s3://{bucket}")
        else:
            raise
    # Block all public access
    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True,
        },
    )
    return bucket


def create_glue_database(glue) -> None:
    try:
        glue.create_database(DatabaseInput={"Name": GLUE_DB})
        print(f"[Glue] Created database: {GLUE_DB}")
    except glue.exceptions.AlreadyExistsException:
        print(f"[Glue] Database already exists: {GLUE_DB}")


def create_iam_role(iam, account_id: str, bucket: str) -> str:
    # Placeholder trust — step 2 will replace with Confluent's actual IAM ARN
    placeholder_trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    try:
        resp = iam.create_role(
            RoleName=IAM_ROLE,
            AssumeRolePolicyDocument=json.dumps(placeholder_trust),
            Description="Allows Confluent Tableflow to write to S3 and register Glue tables",
        )
        role_arn = resp["Role"]["Arn"]
        print(f"[IAM] Created role: {role_arn}")
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=IAM_ROLE)["Role"]["Arn"]
        print(f"[IAM] Role already exists: {role_arn}")

    # Inline policy: S3 (bucket + objects) + Glue read/write
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3Bucket",
                "Effect": "Allow",
                "Action": ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"],
                "Resource": f"arn:aws:s3:::{bucket}",
            },
            {
                "Sid": "S3Objects",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                    "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
                ],
                "Resource": f"arn:aws:s3:::{bucket}/*",
            },
            {
                "Sid": "Glue",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase", "glue:GetDatabases",
                    "glue:GetTable", "glue:GetTables",
                    "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
                    "glue:GetPartition", "glue:GetPartitions",
                    "glue:CreatePartition", "glue:UpdatePartition", "glue:DeletePartition",
                    "glue:BatchCreatePartition", "glue:BatchGetPartition",
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{account_id}:catalog",
                    f"arn:aws:glue:{REGION}:{account_id}:database/{GLUE_DB}",
                    f"arn:aws:glue:{REGION}:{account_id}:table/{GLUE_DB}/*",
                ],
            },
        ],
    }
    iam.put_role_policy(
        RoleName=IAM_ROLE,
        PolicyName="ol-lineage-tableflow-policy",
        PolicyDocument=json.dumps(policy),
    )
    print("[IAM] Attached inline policy (S3 + Glue)")
    return role_arn


def main() -> None:
    account_id = get_account_id()
    print(f"AWS account: {account_id} | region: {REGION}\n")

    s3   = boto3.client("s3",   region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)
    iam  = boto3.client("iam",  region_name=REGION)

    print("[1/3] S3 bucket...")
    bucket = create_s3_bucket(s3, account_id)

    print("\n[2/3] Glue database...")
    create_glue_database(glue)

    print("\n[3/3] IAM role + policy...")
    role_arn = create_iam_role(iam, account_id, bucket)

    print(f"""
AWS setup complete. Add to ~/aws-session.env:

  export AWS_ACCOUNT_ID="{account_id}"
  export AWS_S3_BUCKET="{bucket}"
  export AWS_IAM_ROLE_ARN="{role_arn}"
  export AWS_GLUE_DATABASE="{GLUE_DB}"
  export AWS_REGION="{REGION}"

Then run:  python3 scripts/e2e_pipeline/02_tableflow_setup.py
""")


if __name__ == "__main__":
    main()
