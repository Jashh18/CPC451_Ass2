# Hadoop MapReduce Word Count — Assignment 2
## Complete Setup Guide (Ubuntu / Linux)

---

## Project Structure

```
hadoop-wordcount/
├── src/main/java/com/wordcount/
│   ├── WordCountMapper.java    ← Tokenizes text, emits (word, 1)
│   ├── WordCountReducer.java   ← Sums counts per word
│   └── WordCountDriver.java    ← Configures and submits the job
├── input/
│   └── sample.txt              ← Dataset (edit this freely)
├── demo-website/
│   └── index.html              ← Open this in a browser for demo
└── pom.xml                     ← Maven build file
```

---

## Step-by-Step Setup

### 1. Install Prerequisites

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk maven ssh pdsh
java -version   # should show openjdk 11
mvn -version    # should show Apache Maven 3.x
```

### 2. Download & Install Hadoop

```bash
cd ~
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /opt/hadoop
```

### 3. Set Environment Variables

Add these to `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Then reload:
```bash
source ~/.bashrc
```

### 4. Configure Hadoop (Pseudo-Distributed Mode)

**Edit `/opt/hadoop/etc/hadoop/core-site.xml`:**
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

**Edit `/opt/hadoop/etc/hadoop/hdfs-site.xml`:**
```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

**Edit `/opt/hadoop/etc/hadoop/mapred-site.xml`:**
```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

**Edit `/opt/hadoop/etc/hadoop/yarn-site.xml`:**
```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
```

### 5. Setup SSH (required by Hadoop)

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost   # should connect without password
```

### 6. Format HDFS & Start Services

```bash
hdfs namenode -format
start-dfs.sh
start-yarn.sh
jps   # verify: NameNode, DataNode, ResourceManager, NodeManager
```

Web UIs (open in browser):
- HDFS: http://localhost:9870
- YARN: http://localhost:8088

### 7. Build the JAR

```bash
cd ~/hadoop-wordcount
mvn clean package -DskipTests
# Creates: target/wordcount-1.0.jar
```

### 8. Upload Input & Run

```bash
# Create HDFS directories
hdfs dfs -mkdir -p /user/$USER/input

# Upload the dataset
hdfs dfs -put input/sample.txt /user/$USER/input/

# Run the MapReduce job
hadoop jar target/wordcount-1.0.jar \
  com.wordcount.WordCountDriver \
  /user/$USER/input/sample.txt \
  /user/$USER/output

# View results
hdfs dfs -cat /user/$USER/output/part-r-00000
```

### 9. Re-running the job

Delete the output directory first (Hadoop won't overwrite):
```bash
hdfs dfs -rm -r /user/$USER/output
hadoop jar target/wordcount-1.0.jar com.wordcount.WordCountDriver /user/$USER/input/sample.txt /user/$USER/output
```

---

## Key Optimisations Implemented

| Optimisation | Where | Effect |
|---|---|---|
| **Combiner** | Driver.java | Reduces shuffle data dramatically |
| **Object reuse** | Mapper.java | Cuts GC pressure on large datasets |
| **Text normalisation** | Mapper.java | Accurate counts across case/punctuation |
| **Replication=1** | hdfs-site.xml | OK for single node (use 3 on real cluster) |

---

## Demo Website

Open `demo-website/index.html` in any browser — no server needed.
It simulates the full MapReduce pipeline interactively with custom text input.
