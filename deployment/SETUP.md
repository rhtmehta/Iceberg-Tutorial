# Apache Iceberg with AWS Glue and Spark — Local Setup Guide

> [!WARNING]
> **Tutorial Setup Only**
>
> The IAM permissions used in this guide are intentionally broad to keep the tutorial simple. Do **not** use these permissions in production environments. Production environments should follow the principle of least privilege.

---

## Architecture

This tutorial uses:

| Component | Version |
|-----------|---------|
| AWS S3 | — |
| AWS Glue Catalog | — |
| Apache Iceberg | — |
| Apache Spark | 3.5.6 |
| Java | 17 |
| Python | 3.11 |

---

## AWS Setup

### Step 1: Create IAM Policy

Create a file named `iceberg-tutorial-policy.json` with the following content:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3IcebergAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts",
        "s3:GetEncryptionConfiguration",
        "s3:PutEncryptionConfiguration",
        "s3:PutBucketPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock"
      ],
      "Resource": [
        "arn:aws:s3:::rmehta-iceberg-tutorial",
        "arn:aws:s3:::rmehta-iceberg-tutorial/*"
      ]
    },
    {
      "Sid": "GlueIcebergAccess",
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase",
        "glue:DeleteDatabase",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:UpdateDatabase",
        "glue:CreateTable",
        "glue:DeleteTable",
        "glue:BatchDeleteTable",
        "glue:UpdateTable",
        "glue:GetTable",
        "glue:GetTables",
        "glue:BatchCreatePartition",
        "glue:CreatePartition",
        "glue:DeletePartition",
        "glue:BatchDeletePartition",
        "glue:UpdatePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/iceberg_tutorial",
        "arn:aws:glue:*:*:table/iceberg_tutorial/*"
      ]
    }
  ]
}
```

> [!CAUTION]
> Never grant broad permissions like this in production. Scope the `Resource` ARNs to your specific region and account ID.

Then create the policy:

```bash
aws iam create-policy \
  --policy-name iceberg-tutorial-policy \
  --policy-document file://iceberg-tutorial-policy.json \
  --description "Policy for Apache Iceberg tutorial — S3 and Glue access"
```

Save the `Policy.Arn` from the output — you will need it in the next step.

---

### Step 2: Create IAM Group

```bash
aws iam create-group \
  --group-name iceberg-tutorial-group
```

Attach the policy to the group (replace `<ACCOUNT_ID>` with your AWS account ID):

```bash
aws iam attach-group-policy \
  --group-name iceberg-tutorial-group \
  --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/iceberg-tutorial-policy
```

---

### Step 3: Create IAM User

```bash
aws iam create-user \
  --user-name iceberg-tutorial-user
```

Add the user to the group:

```bash
aws iam add-user-to-group \
  --user-name iceberg-tutorial-user \
  --group-name iceberg-tutorial-group
```

---

### Step 4: Create Access Keys

```bash
aws iam create-access-key \
  --user-name iceberg-tutorial-user
```

The response contains both `AccessKeyId` and `SecretAccessKey`. Save them immediately — the secret is shown only once.

> [!CAUTION]
> Do not commit these values to Git or share them with anyone.

Configure your local CLI with these credentials:

```bash
aws configure --profile iceberg-tutorial
# AWS Access Key ID: <AccessKeyId from above>
# AWS Secret Access Key: <SecretAccessKey from above>
# Default region name: us-east-1
# Default output format: json
```

---

### Step 5: Create Glue IAM Role

Create a trust policy file named `glue-trust-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Create the role:

```bash
aws iam create-role \
  --role-name iceberg-tutorial-glue-role \
  --assume-role-policy-document file://glue-trust-policy.json \
  --description "Glue service role for Iceberg tutorial"
```

Attach the tutorial policy to the role (replace `<ACCOUNT_ID>`):

```bash
aws iam attach-role-policy \
  --role-name iceberg-tutorial-glue-role \
  --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/iceberg-tutorial-policy
```

Also attach the AWS managed Glue service role policy:

```bash
aws iam attach-role-policy \
  --role-name iceberg-tutorial-glue-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

---

## S3 Setup

Create the S3 bucket (replace `us-east-1` with your preferred region):

```bash
aws s3api create-bucket \
  --bucket rmehta-iceberg-tutorial \
  --region us-east-1
```

> **Note:** For regions other than `us-east-1`, add `--create-bucket-configuration LocationConstraint=<region>`.

---

### Block Public Access

New buckets do not have public access blocked by default via the CLI. Set all four block public access flags explicitly:

```bash
aws s3api put-public-access-block \
  --bucket rmehta-iceberg-tutorial \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

Verify:

```bash
aws s3api get-public-access-block \
  --bucket rmehta-iceberg-tutorial
```

Expected output:

```json
{
    "PublicAccessBlockConfiguration": {
        "BlockPublicAcls": true,
        "IgnorePublicAcls": true,
        "BlockPublicPolicy": true,
        "RestrictPublicBuckets": true
    }
}
```

---

### Enable Default Encryption

Enable server-side encryption with SSE-S3 (AES-256) so all objects written to the bucket are encrypted at rest by default:

```bash
aws s3api put-bucket-encryption \
  --bucket rmehta-iceberg-tutorial \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        },
        "BucketKeyEnabled": true
      }
    ]
  }'
```

Verify:

```bash
aws s3api get-bucket-encryption \
  --bucket rmehta-iceberg-tutorial
```

Expected output:

```json
{
    "ServerSideEncryptionConfiguration": {
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                },
                "BucketKeyEnabled": true
            }
        ]
    }
}
```

> **Note:** SSE-S3 (AES-256) is sufficient for this tutorial. For production, consider SSE-KMS with a customer-managed key for tighter access control and audit logging via AWS CloudTrail.

---

Verify the bucket was created:

```bash
aws s3 ls | grep rmehta-iceberg-tutorial
```

---

## AWS Glue Setup

Create the Glue database with the S3 warehouse location:

```bash
aws glue create-database \
  --database-input '{
    "Name": "iceberg_tutorial",
    "LocationUri": "s3://rmehta-iceberg-tutorial/warehouse",
    "Description": "Iceberg tutorial database"
  }'
```

Verify the database was created:

```bash
aws glue get-database --name iceberg_tutorial
```

---

## Local Development Setup

---

## Mac (Apple Silicon M-Series)

> Tested on: MacBook Air M4

---

### Install Homebrew

Install from [https://brew.sh](https://brew.sh), then verify:

```bash
brew --version
# Expected: Homebrew x.x.x
```

---

### Java Installation

#### Install Java 17

```bash
brew install openjdk@17
```

Create a JDK symlink:

```bash
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
  /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

Verify installed JDKs:

```bash
/usr/libexec/java_home -V
# Expected: 17.x.x
```

#### Configure JAVA_HOME

Edit `~/.zshrc`:

```bash
vi ~/.zshrc
```

Add:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

Reload and verify:

```bash
source ~/.zshrc
java -version
# Expected: openjdk version "17.x.x"
```

---

### Python Installation

#### Install Python 3.11

```bash
brew install python@3.11
```

Verify:

```bash
which python3.11
python3.11 --version
# Expected:
#   /opt/homebrew/bin/python3.11
#   Python 3.11.x
```

#### Create Local Symlinks

```bash
mkdir -p ~/.local/bin
ln -sf /opt/homebrew/bin/python3.11 ~/.local/bin/python3
ln -sf /opt/homebrew/bin/pip3.11    ~/.local/bin/pip3
```

Verify:

```bash
ls -l ~/.local/bin
# Expected:
#   python3 -> /opt/homebrew/bin/python3.11
#   pip3    -> /opt/homebrew/bin/pip3.11
```

#### Update `.zshrc`

```bash
vi ~/.zshrc
```

Set contents to:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$HOME/.local/bin:$JAVA_HOME/bin:/opt/homebrew/bin:$PATH"
```

Reload and verify:

```bash
source ~/.zshrc

which python3
which python3.11
python3 --version
python3.11 --version

# Expected:
#   /Users/<username>/.local/bin/python3
#   /opt/homebrew/bin/python3.11
#   Python 3.11.x
#   Python 3.11.x
```

---

### Spark Installation

> **Expected environment before proceeding:**
> - Java → 17
> - Python → 3.11.x
> - python3 → 3.11.x

#### Create Tools Directory

```bash
mkdir -p ~/tools
mkdir -p ~/tools/scripts
```

#### Download and Extract Spark 3.5.6

```bash
cd ~/tools
curl -O https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
mv spark-3.5.6-bin-hadoop3 spark-3.5.6
rm spark-3.5.6-bin-hadoop3.tgz
```

Verify:

```bash
ls ~/tools
# Expected: scripts  spark-3.5.6
```

#### Verify Spark

```bash
~/tools/spark-3.5.6/bin/spark-submit --version
# Expected: version 3.5.6
```

#### Verify PySpark

```bash
PYSPARK_PYTHON=$(which python3) ~/tools/spark-3.5.6/bin/pyspark
```

Inside PySpark:

```python
import sys
sys.version
# Expected: Python 3.11.x

exit()
```

#### Create Spark Activation Script

```bash
vi ~/tools/scripts/use-spark-3.5.6.sh
```

Contents:

```bash
#!/bin/bash

export SPARK_HOME=$HOME/tools/spark-3.5.6
export PYSPARK_PYTHON=$(which python3)
export PATH="$SPARK_HOME/bin:$PATH"

echo "Spark Environment Loaded"
echo "SPARK_HOME=$SPARK_HOME"

spark-submit --version
```

Make executable:

```bash
chmod +x ~/tools/scripts/use-spark-3.5.6.sh
```

#### Load Spark

```bash
source ~/tools/scripts/use-spark-3.5.6.sh
```

Verify:

```bash
which spark-submit
which pyspark
# Expected:
#   ~/tools/spark-3.5.6/bin/spark-submit
#   ~/tools/spark-3.5.6/bin/pyspark
```

#### Iceberg Readiness Check

```bash
spark-shell
# Expected:
#   Spark context available as 'sc'
#   Spark session available as 'spark'
```

Exit:

```scala
:quit
```

---

## Windows Setup

### Install Java 17

Download **Temurin JDK 17** from [https://adoptium.net](https://adoptium.net) and install it.

Verify:

```powershell
java -version
# Expected: openjdk version "17.x.x"
```

---

### Install Python 3.11

Download from [https://www.python.org/downloads/release/python-3110/](https://www.python.org/downloads/release/python-3110/).

During installation, check ✔ **Add Python to PATH**.

Verify:

```powershell
python --version
# Expected: Python 3.11.x
```

---

### Install Spark 3.5.6

Download from [https://archive.apache.org/dist/spark/spark-3.5.6/](https://archive.apache.org/dist/spark/spark-3.5.6/) and extract to `C:\tools\spark-3.5.6`.

Create the following environment variables:

| Variable | Value |
|----------|-------|
| `JAVA_HOME` | `C:\Program Files\Java\jdk-17` |
| `SPARK_HOME` | `C:\tools\spark-3.5.6` |

Update `PATH` to include:

```
%JAVA_HOME%\bin
%SPARK_HOME%\bin
```

Verify:

```powershell
spark-submit --version
# Expected: version 3.5.6
```

---

## Creating Iceberg Tables

There are two ways to create Iceberg tables.

---

### Option 1: AWS CLI

Create a file named `iceberg-console-user-table.json`:

```json
{
  "Name": "iceberg_tutorial_console_user",
  "StorageDescriptor": {
    "Columns": [
      { "Name": "user_id",    "Type": "bigint",    "Comment": "Unique user identifier" },
      { "Name": "username",   "Type": "string",    "Comment": "Login username" },
      { "Name": "email",      "Type": "string",    "Comment": "Email address" },
      { "Name": "first_name", "Type": "string",    "Comment": "" },
      { "Name": "last_name",  "Type": "string",    "Comment": "" },
      { "Name": "phone",      "Type": "string",    "Comment": "" },
      { "Name": "status",     "Type": "string",    "Comment": "active | inactive | suspended" },
      { "Name": "created_at", "Type": "timestamp", "Comment": "Partition column" },
      { "Name": "updated_at", "Type": "timestamp", "Comment": "" }
    ],
    "Location": "s3://rmehta-iceberg-tutorial/warehouse/iceberg-tutorial-console-user/",
    "InputFormat":  "org.apache.hadoop.mapred.FileInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    }
  },
  "PartitionKeys": [],
  "TableType": "EXTERNAL_TABLE",
  "Parameters": {
    "table_type":               "ICEBERG",
    "metadata_location":        "s3://rmehta-iceberg-tutorial/warehouse/iceberg-tutorial-console-user/",
    "compaction_enabled":       "true",
    "snapshot_retention":       "true",
    "orphan_file_deletion":     "true"
  }
}
```

Create the table:

```bash
aws glue create-table \
  --database-name iceberg_tutorial \
  --table-input file://iceberg-console-user-table.json
```

> **Note:** Glue creates Iceberg V2 tables by default.

Verify the table was created:

```bash
aws glue get-table \
  --database-name iceberg_tutorial \
  --name iceberg_tutorial_console_user
```

The table should now be accessible via:

- AWS CLI
- AWS Console
- Spark SQL

---

### Option 2: Create Table Using Spark SQL

```python
spark.sql("""
  CREATE TABLE IF NOT EXISTS glue_catalog.rmehta_iceberg.iceberg_tutorial_spark_user (
    user_id    BIGINT      NOT NULL COMMENT 'Unique user identifier',
    username   STRING      NOT NULL COMMENT 'Login username',
    email      STRING      NOT NULL COMMENT 'Email address',
    first_name STRING               COMMENT '',
    last_name  STRING               COMMENT '',
    phone      STRING               COMMENT '',
    status     STRING      NOT NULL COMMENT 'active | inactive | suspended',
    created_at TIMESTAMP   NOT NULL COMMENT 'Partition column',
    updated_at TIMESTAMP   NOT NULL COMMENT ''
  )
  USING iceberg
  PARTITIONED BY (months(created_at))
  LOCATION 's3://rmehta-iceberg-tutorial/warehouse/rmehta_iceberg/iceberg_tutorial_spark_user'
  TBLPROPERTIES (
    'table_type'                                 = 'ICEBERG',
    'format'                                     = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10'
  )
""")
```