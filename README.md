# Hadoop MapReduce Word Count — Assignment 2
## Interactive Terminal-Based Word Count Program

---

## Project Structure

```
hadoop-wordcount/
├── src/
│   ├── run_wordcount.sh
│   ├── WordCountMapper.java     ← Tokenizes input text, emits (word, 1)
│   ├── WordCountReducer.java    ← Sums counts per word
│   └── WordCountDriver.java     ← Interactive file chooser + runs job
├── input/
│   ├── sample1.txt              ← First dataset
│   └── sample2.txt              ← Second dataset
├── pom.xml                      ← Maven build file
└── README.md                    ← This file
```

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Jashh18/CPC451_Ass2.git
cd hadoop-wordcount
```

---

### 2. Prerequisites

Make sure the following are installed before proceeding:

| Software | Version | Installation Command                                    |
|----------|---------|---------------------------------------------------------|
| Java JDK | 11+     | `sudo apt update && sudo apt install -y openjdk-11-jdk` |
| Maven    | 3.x     | `sudo apt install -y maven`                             |
| Hadoop   | 3.3.6   | See Hadoop Installation section below                   |

Verify installations:

```bash
java -version
mvn -version
hadoop version
```

---

## Full Setup Guide

### Step 1 — Install Hadoop (Pseudo-Distributed Mode)

```bash
# Download and extract Hadoop
cd ~
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /opt/hadoop

# Set environment variables
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc
```

---

### Step 2 — Configure Hadoop

```bash
# core-site.xml
sudo tee /opt/hadoop/etc/hadoop/core-site.xml > /dev/null << 'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

# hdfs-site.xml
sudo tee /opt/hadoop/etc/hadoop/hdfs-site.xml > /dev/null << 'EOF'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF

# mapred-site.xml
sudo tee /opt/hadoop/etc/hadoop/mapred-site.xml > /dev/null << 'EOF'
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

# yarn-site.xml
sudo tee /opt/hadoop/etc/hadoop/yarn-site.xml > /dev/null << 'EOF'
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF
```

---

### Step 3 — Setup SSH (Required for Hadoop)

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Test — should connect without a password prompt
ssh localhost
```

---

### Step 4 — Format and Start Hadoop

```bash
# Format HDFS (first time only — do NOT repeat this step)
hdfs namenode -format

# Start services
start-dfs.sh
start-yarn.sh

# Verify all 4 services are running
jps
```

Expected `jps` output:

```
NameNode
DataNode
ResourceManager
NodeManager
```

---

### Step 5 — Upload Input Files to HDFS

```bash
# Create input directory
hdfs dfs -mkdir -p /input

# Upload sample files
hdfs dfs -put input/sample1.txt /input/
hdfs dfs -put input/sample2.txt /input/

# Confirm upload
hdfs dfs -ls /input/
```

---

### Step 6 — Build the JAR

```bash
cd hadoop-wordcount
mvn clean package

# Verify the JAR was created
ls -la target/wordcount-1.0.jar
```

---

### Step 7 — Run the Program

**Option A — Using the runner script (recommended):**

```bash
chmod +x src/run_wordcount.sh
./src/run_wordcount.sh
```

**Option B — Running directly with Hadoop:**

```bash
hadoop jar target/wordcount-1.0.jar com.wordcount.WordCountDriver
```

---

## Expected Output

After running, the program walks through each MapReduce phase interactively:

```
╔══════════════════════════════════════════════════════════════════════════╗
║     📊  HADOOP MAPREDUCE WORD COUNT - FLOW VISUALIZATION  📊            ║
║         Assignment 2 - Distributed Word Counting                         ║
╚══════════════════════════════════════════════════════════════════════════╝

📁 CHOOSE INPUT METHOD:
────────────────────────────────────────────────────────────────────────────
  1) sample1.txt (2.45 KB)
  2) sample2.txt (1.12 KB)

👉 Enter choice (1-2): 1

📥 STEP 1: INPUT PHASE
🗺️  STEP 2: MAP PHASE
🔄 STEP 3: SHUFFLE & SORT PHASE
🔀 STEP 4: REDUCE PHASE

📊 WORD COUNT RESULTS
═══════════════════════════════════════════════════════════════════════════
WORD                           │ COUNT
───────────────────────────────────────────────────────────────────────────
the                            │ 14
hadoop                         │ 9
...

╔══════════════════════════════════════════════════════════════════════════╗
║                         JOB EXECUTION SUMMARY                            ║
╚══════════════════════════════════════════════════════════════════════════╝
  ⏱️  Total execution time: 20.27 seconds
  🗺️  Map tasks completed:    1
  🔀 Reduce tasks completed: 1
```

---

## Troubleshooting

**Hadoop services not starting:**
```bash
# Check if SSH works without a password
ssh localhost

# Re-format HDFS only if it has never been started before
hdfs namenode -format
start-dfs.sh && start-yarn.sh
```

**`Connection refused` on port 9000:**
```bash
# Make sure NameNode is running
jps | grep NameNode

# If missing, start HDFS again
start-dfs.sh
```

**`Output directory already exists` error:**
```bash
# The program uses timestamped temp directories automatically.
# If you see this on manual runs, delete the old output first:
hdfs dfs -rm -r /tmp/wordcount_output_*
```

**Maven build fails:**
```bash
# Ensure you are inside the project root
cd hadoop-wordcount
mvn clean package -e
```