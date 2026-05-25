# Hadoop MapReduce Word Count — Assignment 2
## Interactive Terminal-Based Word Count Program

---

## Project Structure

```
hadoop-wordcount/
├── src/
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

## Quick Start (One Command Setup)

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/hadoop-wordcount.git
cd hadoop-wordcount

# Run the complete setup script
chmod +x setup.sh
./setup.sh
```

---

## Step-by-Step Setup Guide

### Prerequisites

| Software  | Version | Installation Command                              |
|-----------|---------|---------------------------------------------------|
| Java JDK  | 11+     | `sudo apt update && sudo apt install -y openjdk-11-jdk` |
| Maven     | 3.x     | `sudo apt install -y maven`                       |
| Hadoop    | 3.3.6   | See Hadoop Installation section below             |

---

### 1. Clone the Repository

```bash
# Clone from GitHub
git clone https://github.com/YOUR_USERNAME/hadoop-wordcount.git
cd hadoop-wordcount

# Or if using local files
cd /home/tunteja/hadoop-wordcount
```

---

### 2. Install Hadoop (Pseudo-Distributed Mode)

```bash
# Download Hadoop
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

### 3. Configure Hadoop

Create the required configuration files:

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

### 4. Setup SSH (Required for Hadoop)

```bash
# Generate SSH key
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

# Test SSH connection
ssh localhost  # Should connect without password
```

---

### 5. Start Hadoop Services

```bash
# Format HDFS (only first time)
hdfs namenode -format

# Start HDFS and YARN
start-dfs.sh
start-yarn.sh

# Verify services are running
jps
# Expected output: NameNode, DataNode, ResourceManager, NodeManager
```

---

### 6. Prepare HDFS with Input Files

```bash
# Create input directory in HDFS
hdfs dfs -mkdir -p /input

# Upload sample files to HDFS
hdfs dfs -put input/sample1.txt /input/
hdfs dfs -put input/sample2.txt /input/

# Verify files are uploaded
hdfs dfs -ls /input/
```

---

### 7. Build the JAR File

```bash
cd /home/tunteja/hadoop-wordcount

# Clean and build using Maven
mvn clean package

# Verify JAR was created
ls -la target/wordcount-1.0.jar
```

---

### 8. Run the WordCount Program

```bash
# Run the interactive program
hadoop jar target/wordcount-1.0.jar com.wordcount.WordCountDriver
```

---

## How to Use the Interactive Program

Once you run the program, you'll see:

```
╔══════════════════════════════════════════════════════════════╗
║     📊  HADOOP WORD COUNT MAPREDUCE PROGRAM  📊             ║
║         Assignment 2 - Distributed Word Counting           ║
╚══════════════════════════════════════════════════════════════╝

📁 Available text files in HDFS (/input):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. sample1.txt                    [2.45 KB]
  2. sample2.txt                    [1.12 KB]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👉 Choose a file (1-2):
```

Follow these steps:

1. **Choose a file** — Type `1` or `2` and press Enter
2. **Watch MapReduce progress** — The job will show map/reduce progress (0% → 100%)
3. **View results** — Word counts are displayed in a formatted table