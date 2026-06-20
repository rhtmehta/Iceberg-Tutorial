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

```
Local Machine
└── Spark 3.5.6 (PySpark / spark-shell)
    ├── Iceberg Runtime JAR       → Iceberg table format support
    ├── Iceberg AWS Bundle JAR    → Glue + S3 AWS integrations
    └── AWS Credentials (~/.aws)  → Auth to Glue API + S3
            │
            ▼
    AWS Glue Data Catalog         → Metadata store (databases, tables, schemas)
            │
            ▼
    Amazon S3                     → Actual data files (Parquet + Iceberg metadata)
```

**Platforms covered:** macOS (Apple Silicon), Windows, and Linux (Ubuntu/Debian). Wherever a step differs by platform, all three are shown side by side.

---

## AWS Setup

> Platform-independent — the AWS CLI commands below are identical on Mac, Windows, and Linux. Only the CLI **installation** step (further down) differs by platform.

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

Verify the profile was saved:

**Mac / Linux:**
```bash
cat ~/.aws/credentials
# Expected:
# [iceberg-tutorial]
# aws_access_key_id = AKIA...
# aws_secret_access_key = ...

cat ~/.aws/config
# Expected:
# [profile iceberg-tutorial]
# region = us-east-1
# output = json
```

**Windows (PowerShell):**
```powershell
Get-Content $env:USERPROFILE\.aws\credentials
Get-Content $env:USERPROFILE\.aws\config
```

Verify the credentials actually work against AWS:

```bash
AWS_PROFILE=iceberg-tutorial aws glue get-databases
# Expected: JSON list including your iceberg_tutorial database (created later in this guide)
```

> On Windows PowerShell, set the profile for the current session instead: `$env:AWS_PROFILE="iceberg-tutorial"` then run the same command.

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

Each subsection below covers **Mac, Windows, and Linux** side by side. Skip to whichever platform applies to you, but note that later steps (Spark config, JARs, activation script) assume the directory layout established here, so stay consistent within your platform's column.

---

### Install Homebrew / Chocolatey / apt

| Platform | Action |
|---|---|
| **Mac** | Install Homebrew from [https://brew.sh](https://brew.sh) |
| **Windows** | No package manager required for this guide — installers are downloaded directly. (Optional: [Chocolatey](https://chocolatey.org) if you prefer `choco install`.) |
| **Linux (Ubuntu/Debian)** | `apt` ships with the OS — just run `sudo apt update` first |

**Mac verify:**
```bash
brew --version
# Expected: Homebrew x.x.x
```

**Linux verify:**
```bash
sudo apt update
apt --version
```

---

### Java Installation

#### Install Java 17

**Mac:**
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

**Windows:**
Download **Temurin JDK 17** from [https://adoptium.net](https://adoptium.net) and run the installer.

Verify:
```powershell
java -version
# Expected: openjdk version "17.x.x"
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt install -y openjdk-17-jdk
```

Verify:
```bash
java -version
# Expected: openjdk version "17.x.x"

readlink -f $(which java)
# Confirms install path, typically:
# /usr/lib/jvm/java-17-openjdk-amd64/bin/java
```

---

#### Configure JAVA_HOME

**Mac** — Edit `~/.zshrc`:
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

**Windows** — Create the following environment variables (System Properties → Environment Variables, or `setx`):

| Variable | Value |
|----------|-------|
| `JAVA_HOME` | `C:\Program Files\Java\jdk-17` |

Add to `PATH`:
```
%JAVA_HOME%\bin
```

Verify in a new terminal:
```powershell
java -version
```

**Linux (Ubuntu/Debian)** — Edit `~/.bashrc` (or `~/.zshrc` if using zsh):
```bash
vi ~/.bashrc
```
Add:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```
Reload and verify:
```bash
source ~/.bashrc
java -version
# Expected: openjdk version "17.x.x"
```

> If your `JAVA_HOME` path differs, confirm it with `update-alternatives --list java` or `readlink -f $(which java)`.

---

### Python Installation

#### Install Python 3.11

**Mac:**
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

**Windows:**
Download from [https://www.python.org/downloads/release/python-3110/](https://www.python.org/downloads/release/python-3110/).

During installation, check ✔ **Add Python to PATH**.

Verify:
```powershell
python --version
# Expected: Python 3.11.x
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt install -y python3.11 python3.11-venv python3-pip
```
Verify:
```bash
which python3.11
python3.11 --version
# Expected:
#   /usr/bin/python3.11
#   Python 3.11.x
```

> If `python3.11` isn't available via `apt` on your Ubuntu version, add the deadsnakes PPA first: `sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt update`.

---

#### Create Local Symlinks (Mac & Linux)

This ensures `python3` and `pip3` resolve to 3.11 specifically, regardless of what else is installed system-wide.

**Mac:**
```bash
mkdir -p ~/.local/bin
ln -sf /opt/homebrew/bin/python3.11 ~/.local/bin/python3
ln -sf /opt/homebrew/bin/pip3.11    ~/.local/bin/pip3
```

**Linux (Ubuntu/Debian):**
```bash
mkdir -p ~/.local/bin
ln -sf /usr/bin/python3.11 ~/.local/bin/python3
ln -sf /usr/bin/pip3.11    ~/.local/bin/pip3
```

> If `pip3.11` doesn't exist yet on Linux: `python3.11 -m ensurepip --upgrade` first.

Verify (both platforms):
```bash
ls -l ~/.local/bin
# Expected:
#   python3 -> .../python3.11
#   pip3    -> .../pip3.11
```

**Windows** has no equivalent symlink step — the installer's "Add Python to PATH" option already makes `python` and `pip` resolve correctly, since only one Python version is installed.

---

#### Update Shell Profile (Mac & Linux)

**Mac** — `vi ~/.zshrc`, set:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$HOME/.local/bin:$JAVA_HOME/bin:/opt/homebrew/bin:$PATH"
```

**Linux (Ubuntu/Debian)** — `vi ~/.bashrc`, set:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$HOME/.local/bin:$JAVA_HOME/bin:/usr/bin:$PATH"
```

`~/.local/bin` is listed first on both platforms so the `python3`/`pip3` symlinks take priority over any system Python.

Reload and verify (Mac: `source ~/.zshrc`; Linux: `source ~/.bashrc`):
```bash
which python3
which python3.11
python3 --version
python3.11 --version

# Expected on both platforms:
#   .../.local/bin/python3
#   .../python3.11   (system path)
#   Python 3.11.x
#   Python 3.11.x
```

**Windows** — no shell profile edit needed; `PATH` was already configured via the installer and system environment variables.

---

### Spark Installation

> **Expected environment before proceeding:**
> - Java → 17
> - Python → 3.11.x
> - python3 → 3.11.x (Mac/Linux)

#### Create Tools Directory

**Mac / Linux:**
```bash
mkdir -p ~/tools
mkdir -p ~/tools/scripts
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType Directory -Force -Path C:\tools
```

---

#### Download and Extract Spark 3.5.6

**Mac / Linux:**
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

**Windows:**
Download from [https://archive.apache.org/dist/spark/spark-3.5.6/](https://archive.apache.org/dist/spark/spark-3.5.6/) and extract to `C:\tools\spark-3.5.6`.

Create environment variables:

| Variable | Value |
|----------|-------|
| `SPARK_HOME` | `C:\tools\spark-3.5.6` |

Update `PATH` to include:
```
%SPARK_HOME%\bin
```

---

#### Verify Spark

**Mac / Linux:**
```bash
~/tools/spark-3.5.6/bin/spark-submit --version
# Expected: version 3.5.6
```

**Windows:**
```powershell
spark-submit --version
# Expected: version 3.5.6
```

---

#### Verify PySpark (Mac & Linux)

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

> **Windows:** run `pyspark` directly — since only one Python is on `PATH`, no `PYSPARK_PYTHON` override is needed.

---

#### Create Spark Activation Script

**Mac / Linux:**
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

Load:
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

**Windows:** No activation script needed — `SPARK_HOME` and `PATH` are already set system-wide via environment variables, so `spark-submit` and `pyspark` work in any new terminal.

---

#### Iceberg Readiness Check (all platforms)

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

## Connecting Local Spark to AWS Glue Catalog (Iceberg)

This section picks up once Spark is installed and walks through connecting it to the AWS Glue Data Catalog to read and write Iceberg tables backed by S3.

---

### Step 1: Install and Configure the AWS CLI

If you haven't installed the AWS CLI yet:

**Mac:**
```bash
brew install awscli
```

**Windows:**
Download and run the MSI installer from [https://aws.amazon.com/cli/](https://aws.amazon.com/cli/).

**Linux (Ubuntu/Debian):**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install -y unzip
unzip awscliv2.zip
sudo ./aws/install
```

Verify (all platforms):
```bash
aws --version
# Expected: aws-cli/2.x.x
```

If you already configured the `iceberg-tutorial` profile in the AWS Setup section above, skip ahead to Step 2 — credentials are shared across all tools that read `~/.aws` (or `%USERPROFILE%\.aws` on Windows).

---

### Step 2: Download Required JARs

Spark needs two JARs to connect to Glue and S3 via Iceberg:

| JAR | Purpose |
|-----|---------|
| `iceberg-spark-runtime-3.5_2.12-1.7.1.jar` | Iceberg core runtime for Spark 3.5 |
| `iceberg-aws-bundle-1.7.1.jar` | AWS SDK + Glue catalog + S3FileIO support |

**Mac / Linux:**
```bash
mkdir -p ~/tools/jars
cd ~/tools/jars

curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
```

Verify:
```bash
ls -lh ~/tools/jars
# Expected:
#   iceberg-aws-bundle-1.7.1.jar              ~100MB
#   iceberg-spark-runtime-3.5_2.12-1.7.1.jar  ~40MB
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType Directory -Force -Path C:\tools\jars
cd C:\tools\jars

Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" -OutFile "iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar" -OutFile "iceberg-aws-bundle-1.7.1.jar"
```

Verify:
```powershell
Get-ChildItem C:\tools\jars
```

---

### Step 3: Create `spark-defaults.conf`

Rather than passing `--conf` flags on every command, configure Spark defaults once.

**Mac / Linux:**
```bash
cp ~/tools/spark-3.5.6/conf/spark-defaults.conf.template \
   ~/tools/spark-3.5.6/conf/spark-defaults.conf

vi ~/tools/spark-3.5.6/conf/spark-defaults.conf
```

**Windows (PowerShell):**
```powershell
Copy-Item "C:\tools\spark-3.5.6\conf\spark-defaults.conf.template" `
          "C:\tools\spark-3.5.6\conf\spark-defaults.conf"

notepad C:\tools\spark-3.5.6\conf\spark-defaults.conf
```

Add the following at the bottom (replace placeholder values with your own):

**Mac / Linux:**
```properties
# ── Iceberg Extension ────────────────────────────────────────────────────────
spark.sql.extensions                          org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ── Catalog: glue_catalog ────────────────────────────────────────────────────
spark.sql.catalog.glue_catalog                org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl   org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl        org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.glue_catalog.warehouse      s3://rmehta-iceberg-tutorial/warehouse

# ── Default Catalog ───────────────────────────────────────────────────────────
spark.sql.defaultCatalog                      glue_catalog

# ── AWS Region ────────────────────────────────────────────────────────────────
spark.hadoop.aws.region                       us-east-1

# ── JARs ──────────────────────────────────────────────────────────────────────
spark.jars                                    /Users/<your-username>/tools/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar,/Users/<your-username>/tools/jars/iceberg-aws-bundle-1.7.1.jar
```

> On Linux, the JAR paths will be `/home/<your-username>/tools/jars/...` instead of `/Users/<your-username>/...`.

**Windows:**
```properties
# ── Iceberg Extension ────────────────────────────────────────────────────────
spark.sql.extensions                          org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ── Catalog: glue_catalog ────────────────────────────────────────────────────
spark.sql.catalog.glue_catalog                org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl   org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl        org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.glue_catalog.warehouse      s3://rmehta-iceberg-tutorial/warehouse

# ── Default Catalog ───────────────────────────────────────────────────────────
spark.sql.defaultCatalog                      glue_catalog

# ── AWS Region ────────────────────────────────────────────────────────────────
spark.hadoop.aws.region                       us-east-1

# ── JARs (use forward slashes even on Windows) ───────────────────────────────
spark.jars                                    C:/tools/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar,C:/tools/jars/iceberg-aws-bundle-1.7.1.jar
```

> **Replace on every platform:**
> - `rmehta-iceberg-tutorial` → your S3 bucket name
> - `us-east-1` → your AWS region
> - `<your-username>` → your username (Mac/Linux: `echo $HOME`; Windows: `echo %USERPROFILE%`)

---

### Step 4: Export AWS Credentials for Spark

Spark picks up AWS credentials from environment variables. The cleanest approach for local development is exporting them from your named profile before starting Spark.

**Mac / Linux** — add a helper function to your shell profile (`~/.zshrc` on Mac, `~/.bashrc` on Linux):

```bash
vi ~/.zshrc      # Mac
vi ~/.bashrc     # Linux
```

Add at the bottom:
```bash
# Load AWS credentials from a named profile into environment variables
function aws-export() {
  local profile="${1:-iceberg-tutorial}"
  export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile "$profile")
  export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile "$profile")
  export AWS_DEFAULT_REGION=$(aws configure get region --profile "$profile")
  export AWS_REGION=$AWS_DEFAULT_REGION
  echo "AWS credentials loaded from profile: $profile"
  echo "Region: $AWS_DEFAULT_REGION"
}
```

Reload and run:
```bash
source ~/.zshrc      # Mac
source ~/.bashrc     # Linux

aws-export iceberg-tutorial
```

Verify:
```bash
echo $AWS_ACCESS_KEY_ID
echo $AWS_DEFAULT_REGION
echo $AWS_REGION
# Expected: your key and region are printed
```

**Windows (PowerShell)** — define an equivalent function in your PowerShell profile:

```powershell
notepad $PROFILE
```

Add:
```powershell
function Set-AwsTutorialCreds {
    param([string]$Profile = "iceberg-tutorial")
    $env:AWS_ACCESS_KEY_ID     = aws configure get aws_access_key_id --profile $Profile
    $env:AWS_SECRET_ACCESS_KEY = aws configure get aws_secret_access_key --profile $Profile
    $env:AWS_DEFAULT_REGION    = aws configure get region --profile $Profile
    $env:AWS_REGION            = $env:AWS_DEFAULT_REGION
    Write-Host "AWS credentials loaded from profile: $Profile"
    Write-Host "Region: $env:AWS_DEFAULT_REGION"
}
```

Reload and run:
```powershell
. $PROFILE
Set-AwsTutorialCreds -Profile iceberg-tutorial
```

Verify:
```powershell
echo $env:AWS_ACCESS_KEY_ID
echo $env:AWS_DEFAULT_REGION
echo $env:AWS_REGION
```

---

### Step 5: Update the Spark Activation Script

**Mac / Linux** — update the activation script created earlier to also load the Iceberg/AWS environment:

```bash
vi ~/tools/scripts/use-spark-3.5.6.sh
```

Replace contents with:
```bash
#!/bin/bash

# ── Spark ─────────────────────────────────────────────────────────────────────
export SPARK_HOME=$HOME/tools/spark-3.5.6
export PYSPARK_PYTHON=$(which python3)
export PATH="$SPARK_HOME/bin:$PATH"

# ── AWS Credentials ───────────────────────────────────────────────────────────
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile iceberg-tutorial)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile iceberg-tutorial)
export AWS_DEFAULT_REGION=$(aws configure get region --profile iceberg-tutorial)
export AWS_REGION=$AWS_DEFAULT_REGION

echo "──────────────────────────────────────────"
echo " Spark Environment Loaded"
echo " SPARK_HOME    = $SPARK_HOME"
echo " AWS Region    = $AWS_DEFAULT_REGION"
echo "──────────────────────────────────────────"

spark-submit --version
```

Reload:
```bash
source ~/tools/scripts/use-spark-3.5.6.sh
```

**Windows** — since `SPARK_HOME`/`PATH` are already set system-wide, just load AWS credentials before each session using the `Set-AwsTutorialCreds` function from Step 4:

```powershell
Set-AwsTutorialCreds -Profile iceberg-tutorial
spark-submit --version
```

---

### Step 6: Verify the Connection (all platforms)

#### Option A: spark-shell (Scala)

```bash
spark-shell
```

Run inside the shell:
```scala
// List all databases in the Glue catalog
spark.sql("SHOW DATABASES").show()

// Switch to your tutorial database
spark.sql("USE iceberg_tutorial")

// List tables
spark.sql("SHOW TABLES").show()
```

Expected output:
```
+------------------+
|      namespace   |
+------------------+
| iceberg_tutorial |
+------------------+

+------------------+--------------------+-----------+
|         namespace|           tableName|isTemporary|
+------------------+--------------------+-----------+
| iceberg_tutorial | <your_table_name>  |      false|
+------------------+--------------------+-----------+
```

Exit:
```scala
:quit
```

---

#### Option B: PySpark

```bash
pyspark
```

Run inside the shell:
```python
# List databases
spark.sql("SHOW DATABASES").show()

# List tables in your database
spark.sql("SHOW TABLES IN iceberg_tutorial").show()

# Describe a table
spark.sql("DESCRIBE TABLE iceberg_tutorial.<your_table_name>").show(truncate=False)
```

Exit:
```python
exit()
```

---

### Configuration Reference

| Property | Value | Description |
|----------|-------|-------------|
| `spark.sql.extensions` | `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` | Enables Iceberg SQL extensions |
| `spark.sql.catalog.glue_catalog` | `org.apache.iceberg.spark.SparkCatalog` | Registers catalog named `glue_catalog` |
| `spark.sql.catalog.glue_catalog.catalog-impl` | `org.apache.iceberg.aws.glue.GlueCatalog` | Uses AWS Glue as the catalog backend |
| `spark.sql.catalog.glue_catalog.io-impl` | `org.apache.iceberg.aws.s3.S3FileIO` | Uses Iceberg's native S3 file I/O |
| `spark.sql.catalog.glue_catalog.warehouse` | `s3://your-bucket/warehouse` | Default S3 location for new tables |
| `spark.sql.defaultCatalog` | `glue_catalog` | Makes `glue_catalog` the default — no prefix needed in SQL |
| `spark.hadoop.aws.region` | `us-east-1` | AWS region for Glue and S3 calls |
| `spark.jars` | paths to both JARs | JARs loaded into every Spark session |

---

### Troubleshooting

#### `Unable to load credentials from any of the providers`

Spark cannot find your AWS credentials.

**Fix (Mac/Linux):**
```bash
source ~/tools/scripts/use-spark-3.5.6.sh
echo $AWS_ACCESS_KEY_ID  # must not be empty
```

**Fix (Windows):**
```powershell
Set-AwsTutorialCreds -Profile iceberg-tutorial
echo $env:AWS_ACCESS_KEY_ID
```

---

#### `ClassNotFoundException: org.apache.iceberg.aws.glue.GlueCatalog`

The `iceberg-aws-bundle` JAR is not on the classpath.

**Fix (Mac/Linux):**
```bash
ls -lh ~/tools/jars/iceberg-aws-bundle-1.7.1.jar
# File must exist at exactly this path
```

**Fix (Windows):**
```powershell
Get-Item C:\tools\jars\iceberg-aws-bundle-1.7.1.jar
```

Confirm the `spark.jars` paths in `spark-defaults.conf` are absolute and correct on your platform.

---

#### `Access Denied` on S3

Your IAM user lacks S3 permissions to the bucket.

**Fix (all platforms):** Confirm the IAM policy attached to the user includes the S3 actions listed in the AWS Setup section above, and that the bucket name in `spark.sql.catalog.glue_catalog.warehouse` matches exactly.

---

#### `EntityNotFoundException` (Glue database not found)

The region in your credentials doesn't match where the Glue database was created.

**Fix (Mac/Linux):**
```bash
echo $AWS_DEFAULT_REGION
aws glue get-database --name iceberg_tutorial --profile iceberg-tutorial
```

**Fix (Windows):**
```powershell
echo $env:AWS_DEFAULT_REGION
aws glue get-database --name iceberg_tutorial --profile iceberg-tutorial
```

---

## Creating Iceberg Tables

There are two ways to create Iceberg tables. Both are platform-independent — Option 1 runs through the AWS CLI (already installed above) and Option 2 runs inside any Spark shell (`spark-shell` or `pyspark`).

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
  CREATE TABLE IF NOT EXISTS glue_catalog.iceberg_tutorial.iceberg_tutorial_spark_user (
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
  LOCATION 's3://rmehta-iceberg-tutorial/warehouse/iceberg_tutorial/iceberg_tutorial_spark_user'
  TBLPROPERTIES (
    'table_type'                                 = 'ICEBERG',
    'format'                                     = 'parquet',
    'write.parquet.compression-codec'            = 'snappy',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '10'
  )
""")
```
