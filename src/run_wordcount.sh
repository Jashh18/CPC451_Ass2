#!/bin/bash

# WordCount Runner Script with Enhanced UI

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${CYAN}${BOLD}"
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║     🚀 Hadoop WordCount - Automated Runner 🚀                ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Step 1: Check if Hadoop is running
echo -e "${YELLOW}🔍 Checking Hadoop services...${NC}"
if ! jps | grep -q "NameNode"; then
    echo -e "${RED}⚠️  Hadoop services not running. Starting them...${NC}"
    start-dfs.sh
    start-yarn.sh
    sleep 5
    echo -e "${GREEN}✓ Hadoop services started${NC}"
else
    echo -e "${GREEN}✓ Hadoop services are running${NC}"
fi

# Step 2: Create input directory if needed
echo -e "\n${YELLOW}📁 Setting up HDFS directories...${NC}"
hdfs dfs -mkdir -p /input
echo -e "${GREEN}✓ HDFS directories ready${NC}"

# Step 3: Upload all text files to HDFS
echo -e "\n${YELLOW}📤 Uploading text files to HDFS...${NC}"
for file in input/*.txt; do
    if [ -f "$file" ]; then
        hdfs dfs -put -f "$file" /input/ 2>/dev/null
        echo -e "${GREEN}  ✓ Uploaded: $file${NC}"
    fi
done

# Step 4: Build the JAR
echo -e "\n${YELLOW}🔨 Building JAR file...${NC}"
mvn clean package -q
echo -e "${GREEN}✓ Build complete${NC}"

# Step 5: Run the MapReduce job
echo -e "\n${PURPLE}${BOLD}════════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}🎯 Starting WordCount MapReduce Job${NC}"
echo -e "${PURPLE}${BOLD}════════════════════════════════════════════════════════════════${NC}\n"

hadoop jar target/wordcount-1.0.jar com.wordcount.WordCountDriver

echo -e "\n${GREEN}${BOLD}✅ Script completed!${NC}"