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

Navigate to **AWS Console → IAM → Policies → Create Policy** and create a policy with:

- S3 Full Access
- Glue Full Access

> [!CAUTION]
> Never grant broad permissions like this in production.

---

### Step 2: Create IAM Group

Navigate to **IAM → User Groups → Create Group** and attach the policy created above.

---

### Step 3: Create IAM User

Navigate to **IAM → Users → Create User** and attach the user to the group created above.

---

### Step 4: Create Access Keys

Navigate to **IAM User → Security Credentials → Create Access Key → Command Line Interface (CLI)** and save:

- Access Key
- Secret Access Key

> [!CAUTION]
> Do not commit these values to Git or share them with anyone.

---

### Step 5: Create Glue IAM Role

Navigate to **IAM → Roles → Create Role** and select:

- **Trusted Entity Type:** AWS Service
- **Use Case:** Glue

Name the role (e.g., `iceberg-tutorial-glue-role`) and save the role name for later.

---

## S3 Setup

Navigate to **AWS Console → S3 → Create Bucket**.

Example bucket name:
```
rmehta-iceberg-tutorial
```

Use all recommended default settings and copy the bucket name — it will be required later.

---

## AWS Glue Setup

Navigate to **AWS Console → Glue → Databases → Add Database** and create a database with:

| Field | Value |
|-------|-------|
| Database Name | `iceberg_tutorial` |
| Location | `s3://rmehta-iceberg-tutorial/warehouse` |

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

### Option 1: AWS Console

Navigate to **Glue → Tables → Create Table** and choose **Apache Iceberg**.

Configure the table with:

| Field | Value |
|-------|-------|
| Database | `iceberg_tutorial` |
| Compaction | ✔ Enabled |
| Snapshot Retention | ✔ Enabled |
| Orphan File Deletion | ✔ Enabled |
| IAM Role | `iceberg-tutorial-glue-role` |
| Location | `s3://rmehta-iceberg-tutorial/warehouse/iceberg-tutorial-console-user/` |

Add columns and create the table.

> **Note:** Glue creates Iceberg V2 tables by default.

Verify using:

```sql
SHOW TABLES;
```

The table should now be visible through:

- AWS Console
- AWS CLI
- Spark SQL

---

### Option 2: Create Table Using Spark SQL

This section will be covered in the next step of the tutorial.