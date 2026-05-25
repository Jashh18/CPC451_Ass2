#!/bin/bash

# WordCount Runner Script
# This script automates the entire process

echo "═══════════════════════════════════════════════════════════════"
echo "  Hadoop WordCount - Automated Runner"
echo "═══════════════════════════════════════════════════════════════"

# Step 1: Check if Hadoop is running
echo ""
echo "🔍 Checking Hadoop services..."
if ! jps | grep -q "NameNode"; then
    echo "⚠️  Hadoop services not running. Starting them..."
    start-dfs.sh
    start-yarn.sh
    sleep 5
fi

# Step 2: Create input directory if needed
echo ""
echo "📁 Setting up HDFS directories..."
hdfs dfs -mkdir -p /input

# Step 3: Ask user for local file to upload
echo ""
echo "📂 Local text files found:"
ls -la *.txt 2>/dev/null || echo "   No .txt files in current directory"

echo ""
read -p "Enter path to your text file (or press Enter to use default sample.txt): " local_file

if [ -z "$local_file" ]; then
    # Create a sample file if none exists
    if [ ! -f "sample.txt" ]; then
        echo "Creating sample.txt with demo content..."
        cat > sample.txt << EOF
Hadoop is a powerful framework for distributed processing
MapReduce is the programming model for Hadoop
Hadoop can process massive amounts of data
MapReduce jobs run in parallel across clusters
Hadoop and MapReduce work together seamlessly
EOF
    fi
    local_file="sample.txt"
fi

# Step 4: Upload to HDFS
echo ""
echo "📤 Uploading $local_file to HDFS..."
hdfs dfs -put -f $local_file /input/

# Step 5: Build the JAR
echo ""
echo "🔨 Building JAR file..."
mvn clean package

# Step 6: Run the MapReduce job
echo ""
echo "🚀 Running WordCount MapReduce job..."
echo ""

hadoop jar target/wordcount-1.0.jar com.wordcount.WordCountDriver

echo ""
echo "✅ Script completed!"