# Connecting Local Spark to AWS Glue Catalog (Iceberg)

This guide picks up from the local Spark setup and walks through connecting your local Spark 3.5.6 instance to the AWS Glue Data Catalog to read and write Iceberg tables backed by S3.

---

## Architecture Overview

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

---

## Step 1: Configure AWS Credentials Locally

Spark will authenticate to Glue and S3 using your local AWS credentials. The easiest way is via the AWS CLI.

### Install AWS CLI (if not already installed)

**Mac:**
```bash
brew install awscli
```

**Windows:**
Download from [https://aws.amazon.com/cli/](https://aws.amazon.com/cli/)

Verify:
```bash
aws --version
# Expected: aws-cli/2.x.x
```

---

### Configure a Named Profile

Use a named profile rather than the default profile to keep tutorial credentials isolated.

```bash
aws configure --profile iceberg-tutorial
```

Enter the values saved from the IAM Access Key step:

```
AWS Access Key ID:     <your-access-key>
AWS Secret Access Key: <your-secret-key>
AWS Default region:    <your-region>   # e.g. us-east-1
Default output format: json
```

Verify the profile was saved:

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

Verify the credentials work:

```bash
AWS_PROFILE=iceberg-tutorial aws glue get-databases
# Expected: JSON list including your iceberg_tutorial database
```

---

## Step 2: Download Required JARs

Spark needs two JARs to connect to Glue and S3 via Iceberg:

| JAR | Purpose |
|-----|---------|
| `iceberg-spark-runtime-3.5_2.12-1.7.1.jar` | Iceberg core runtime for Spark 3.5 |
| `iceberg-aws-bundle-1.7.1.jar` | AWS SDK + Glue catalog + S3FileIO support |

Create a directory for JARs:

```bash
mkdir -p ~/tools/jars
cd ~/tools/jars
```

Download both JARs from Maven Central:

```bash
# Iceberg Spark Runtime (Spark 3.5, Scala 2.12)
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar

# Iceberg AWS Bundle (includes Glue + S3 dependencies)
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
```

Verify both JARs are present:

```bash
ls -lh ~/tools/jars
# Expected:
#   iceberg-aws-bundle-1.7.1.jar           ~100MB
#   iceberg-spark-runtime-3.5_2.12-1.7.1.jar  ~40MB
```

---

## Step 3: Create `spark-defaults.conf`

Rather than passing `--conf` flags on every command, configure Spark defaults once in a config file.

```bash
cp ~/tools/spark-3.5.6/conf/spark-defaults.conf.template \
   ~/tools/spark-3.5.6/conf/spark-defaults.conf
```

Open the file:

```bash
vi ~/tools/spark-3.5.6/conf/spark-defaults.conf
```

Add the following at the bottom (replace placeholder values with your own):

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

> **Replace:**
> - `rmehta-iceberg-tutorial` → your S3 bucket name
> - `us-east-1` → your AWS region
> - `<your-username>` → your Mac username (`echo $HOME` to confirm)

---

## Step 4: Export AWS Credentials for Spark

Spark picks up AWS credentials from environment variables. The cleanest way for local development is to export them from your named profile before starting Spark.

Add a helper function to your `~/.zshrc`:

```bash
vi ~/.zshrc
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

Reload:

```bash
source ~/.zshrc
```

Before starting any Spark session, run:

```bash
aws-export iceberg-tutorial
```

Verify:

```bash
echo $AWS_ACCESS_KEY_ID
echo $AWS_DEFAULT_REGION
echo $AWS_REGION

# Expected: your key and region are printed
```

---

## Step 5: Update the Spark Activation Script

Update the script created in the previous guide to also load the Iceberg/AWS environment:

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

---

## Step 6: Verify the Connection

### Option A: spark-shell (Scala)

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
| rmehta_iceberg |
+------------------+

+------------------+--------------------+-----------+
|         namespace|           tableName|isTemporary|
+------------------+--------------------+-----------+
| rmehta_iceberg | <your_table_name>  |      false|
+------------------+--------------------+-----------+
```

Exit:
```scala
:quit
```

---

### Option B: PySpark

```bash
pyspark
```

Run inside the shell:

```python
# List databases
spark.sql("SHOW DATABASES").show()

# List tables in your database
spark.sql("SHOW TABLES IN rmehta_iceberg").show()

# Describe a table
spark.sql("DESCRIBE TABLE rmehta_iceberg.<your_table_name>").show(truncate=False)
```

Exit:
```python
exit()
```

---

## Configuration Reference

### `spark-defaults.conf` Key Properties

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

## Troubleshooting

### `Unable to load credentials from any of the providers`

Spark cannot find your AWS credentials.

**Fix:** Ensure you exported credentials before starting Spark:

```bash
source ~/tools/scripts/use-spark-3.5.6.sh
echo $AWS_ACCESS_KEY_ID  # must not be empty
```

---

### `ClassNotFoundException: org.apache.iceberg.aws.glue.GlueCatalog`

The `iceberg-aws-bundle` JAR is not on the classpath.

**Fix:** Confirm the `spark.jars` paths in `spark-defaults.conf` are absolute and correct:

```bash
ls -lh ~/tools/jars/iceberg-aws-bundle-1.7.1.jar
# File must exist at exactly this path
```

---

### `Access Denied` on S3

Your IAM user lacks S3 permissions to the bucket.

**Fix:** Confirm the IAM policy attached to the user includes `s3:*` on the tutorial bucket, and that the bucket name in `spark.sql.catalog.glue_catalog.warehouse` matches exactly.

---

### `EntityNotFoundException` (Glue database not found)

The region in your credentials doesn't match where the Glue database was created.

**Fix:** Confirm `AWS_DEFAULT_REGION` matches the region where you created the `iceberg_tutorial` database in Glue:

```bash
echo $AWS_DEFAULT_REGION
aws glue get-database --name iceberg_tutorial --profile iceberg-tutorial
```