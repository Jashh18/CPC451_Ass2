package com.wordcount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {

    // ANSI color codes for beautiful terminal output
    private static final String RESET = "\u001B[0m";
    private static final String CYAN = "\u001B[36m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String PURPLE = "\u001B[35m";
    private static final String RED = "\u001B[31m";
    private static final String BOLD = "\u001B[1m";
    private static final String DIM = "\u001B[2m";

    public static void main(String[] args) throws Exception {
        
        printHeader();
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        String inputDir = "/input";
        Path inputPath = new Path(inputDir);
        
        if (!fs.exists(inputPath)) {
            System.err.println(RESET + "ERROR: Input directory " + inputDir + " does not exist in HDFS!" + RESET);
            System.exit(-1);
        }
        
        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        
        if (fileStatuses.length == 0) {
            System.err.println(RESET + "ERROR: No files found in " + inputDir + RESET);
            System.exit(-1);
        }
        
        // Display available files
        displayAvailableFiles(fileStatuses);
        
        // Get user choice
        String inputPathStr = getUserChoice(fileStatuses);
        
        // Display file details
        displayFileDetails(inputPathStr, fs);
        
        // Create output directory with timestamp
        String timestamp = String.valueOf(System.currentTimeMillis());
        String tempOutputPath = "/tmp/wordcount_output_" + timestamp;
        
        printSeparator();
        System.out.println(CYAN + BOLD + "MAPREDUCE PIPELINE EXECUTION" + RESET);
        printSeparator();
        
        // STEP 1: Input Phase
        showInputPhase(inputPathStr, fs);
        
        // STEP 2: Map Phase
        showMapPhaseStart();

        // Configure and run the job
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPathStr));
        FileOutputFormat.setOutputPath(job, new Path(tempOutputPath));

        // ── Suppress all Hadoop INFO/WARN logs during job execution ──
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(false); // false = suppress progress lines too
        long endTime = System.currentTimeMillis();

        // ── Restore logging ──
        org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.WARN);
                
        if (success) {
            showMapPhaseComplete(job);
            
            // STEP 3: Shuffle & Sort Phase
            showShufflePhase();
            
            // STEP 4: Reduce Phase
            showReducePhase();
            
            // Display Results
            displayResults(fs, new Path(tempOutputPath));
            
            // Job Summary
            showJobSummary(job, startTime, endTime);
            
            // Clean up
            cleanup(fs, new Path(tempOutputPath));
            
        } else {
            System.err.println(RESET + "ERROR: Job failed." + RESET);
            System.exit(1);
        }
    }
    
    private static void printHeader() {
        System.out.println();
        System.out.println(CYAN + BOLD + "===========================================================" + RESET);
        System.out.println(CYAN + BOLD + "     HADOOP MAPREDUCE WORD COUNT" + RESET);
        System.out.println(CYAN + BOLD + "===========================================================" + RESET);
        System.out.println();
    }
    
    private static void printSeparator() {
        System.out.println(DIM + "-----------------------------------------------------------" + RESET);
    }
    
    private static void displayAvailableFiles(FileStatus[] fileStatuses) {
        System.out.println(YELLOW + "Available input files:" + RESET);
        printSeparator();
        
        int counter = 1;
        for (FileStatus status : fileStatuses) {
            if (!status.isDirectory()) {
                String fileName = status.getPath().getName();
                long fileSize = status.getLen();
                String sizeStr = formatFileSize(fileSize);
                System.out.printf("  %d) %s " + DIM + "(%s)" + RESET + "\n", 
                    counter++, fileName, sizeStr);
            }
        }
        System.out.println();
    }
    
    private static String getUserChoice(FileStatus[] fileStatuses) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.print(GREEN + "Enter choice (1-" + (fileStatuses.length) + "): " + RESET);
        System.out.flush();
        int choice = scanner.nextInt();
        
        if (choice < 1 || choice > fileStatuses.length) {
            System.err.println(RESET + "ERROR: Invalid choice! Exiting..." + RESET);
            System.exit(-1);
        }
        
        String selectedPath = fileStatuses[choice - 1].getPath().toString();
        System.out.println(GREEN + "Selected: " + RESET + selectedPath.substring(selectedPath.lastIndexOf('/') + 1));
        System.out.println();
        return selectedPath;
    }
    
    private static void displayFileDetails(String filePath, FileSystem fs) throws Exception {
        Path path = new Path(filePath);
        FileStatus status = fs.getFileStatus(path);
        
        System.out.println(BLUE + "File Details:" + RESET);
        printSeparator();
        System.out.println("  Name: " + path.getName());
        System.out.println("  Size: " + formatFileSize(status.getLen()));
        System.out.println("  HDFS Path: " + filePath);
        System.out.println();
    }
    
    private static void showInputPhase(String inputPath, FileSystem fs) throws Exception {
        System.out.println(CYAN + "[STEP 1] INPUT PHASE" + RESET);
        printSeparator();
        
        Path path = new Path(inputPath);
        FileStatus status = fs.getFileStatus(path);
        
        // Read file and count lines
        int lineCount = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        while (reader.readLine() != null) lineCount++;
        reader.close();
        
        System.out.println("  Input file: " + path.getName());
        System.out.println("  Lines: " + lineCount);
        System.out.println("  Size: " + formatFileSize(status.getLen()));
        System.out.println("  Status: " + GREEN + "Uploaded to HDFS" + RESET);
        System.out.println();
    }
    
    private static void showMapPhaseStart() {
        System.out.println(CYAN + "[STEP 2] MAP PHASE" + RESET);
        printSeparator();
        System.out.println("  Splitting input into chunks...");
        System.out.println("  Tokenizing lines into words...");
        System.out.println("  Normalizing text (lowercase + punctuation removal)...");
        System.out.println("  Emitting (word, 1) pairs...");
    }
    
    private static void showMapPhaseComplete(Job job) throws Exception {
        Counters counters = job.getCounters();
        
        // Get actual counters from MapReduce job
        long mapInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
        long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
        long combineInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_INPUT_RECORDS").getValue();
        long combineOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_OUTPUT_RECORDS").getValue();
        
        System.out.println("  Map input records: " + YELLOW + mapInputRecords + RESET);
        System.out.println("  Map output records: " + YELLOW + mapOutputRecords + RESET);
        
        if (combineInputRecords > 0) {
            System.out.println("  Combine input records: " + combineInputRecords + " records");
            System.out.println("  Combine output records: " + GREEN + combineOutputRecords + RESET + " records (optimized!)");
        }
        System.out.println();
    }
    
    private static void showShufflePhase() {
        System.out.println(CYAN + "[STEP 3] SHUFFLE AND SORT PHASE" + RESET);
        printSeparator();
        System.out.println("  Transferring data from mappers to reducers...");
        System.out.println("  Grouping by key (word)...");
        System.out.println("  Sorting alphabetically...");
        System.out.println("  Status: " + GREEN + "Complete" + RESET);
        System.out.println();
    }
    
    private static void showReducePhase() {
        System.out.println(CYAN + "[STEP 4] REDUCE PHASE" + RESET);
        printSeparator();
        System.out.println("  Summing counts for each unique word...");
        System.out.println("  Writing final output to HDFS...");
        System.out.println("  Status: " + GREEN + "Complete" + RESET);
        System.out.println();
    }
    
    private static void displayResults(FileSystem fs, Path outputPath) throws Exception {
        System.out.println(PURPLE + BOLD + "WORD COUNT RESULTS" + RESET);
        System.out.println(CYAN + "═══════════════════════════════════════════════════════════════════════════" + RESET);
        System.out.printf(BOLD + "%-25s | %s\n" + RESET, "WORD", "COUNT");
        System.out.println(DIM + "-----------------------------------------------------------" + RESET);
        
        Path resultFile = findResultFile(fs, outputPath);
        
        if (fs.exists(resultFile)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(resultFile)));
            String line;
            int totalWords = 0;
            int wordCount = 0;
            Map<String, Integer> wordMap = new LinkedHashMap<>();
            List<Map.Entry<String, Integer>> sortedList = new ArrayList<>();
            
            // First, read all results
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    totalWords += count;
                    wordCount++;
                    wordMap.put(word, count);
                }
            }
            reader.close();
            
            // Sort by count descending
            sortedList = new ArrayList<>(wordMap.entrySet());
            sortedList.sort(Map.Entry.<String, Integer>comparingByValue().reversed());
            
            // Get top 10 words for highlighting
            Set<String> top10Words = new HashSet<>();
            for (int i = 0; i < Math.min(10, sortedList.size()); i++) {
                top10Words.add(sortedList.get(i).getKey());
            }
            
            // Display results with top 10 highlighted
            for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue();
                
                // Color code based on count frequency
                if (top10Words.contains(word)) {
                    // Highlight top 10 words in YELLOW + BOLD
                    System.out.printf(YELLOW + BOLD + "%-25s | %,d\n" + RESET, word, count);
                } else if (count > 10) {
                    System.out.printf(YELLOW + "%-25s | %,d\n" + RESET, word, count);
                } else if (count > 5) {
                    System.out.printf("%-25s | %,d\n", word, count);
                } else {
                    System.out.printf("%-25s | %,d\n", word, count);
                }
            }
            
            System.out.println(CYAN + "═══════════════════════════════════════════════════════════════════════════" + RESET);
            System.out.println("\n" + BOLD + "SUMMARY STATISTICS:" + RESET);
            printSeparator();
            System.out.println("  Total unique words: " + GREEN + wordCount + RESET);
            System.out.println("  Total word occurrences: " + GREEN + totalWords + RESET);
            
            // Show top 10 most frequent words
            System.out.println("\n" + BOLD + "TOP 10 MOST FREQUENT WORDS:" + RESET);
            printSeparator();
            for (int i = 0; i < Math.min(10, sortedList.size()); i++) {
                Map.Entry<String, Integer> entry = sortedList.get(i);
                System.out.printf("  %d. %-20s : " + YELLOW + "%,d\n" + RESET, 
                    i + 1, entry.getKey(), entry.getValue());
            }
            
        } else {
            System.err.println(RESET + "WARNING: Could not find output file." + RESET);
        }
        System.out.println();
    }
    
    private static Path findResultFile(FileSystem fs, Path outputPath) throws Exception {
        FileStatus[] files = fs.listStatus(outputPath);
        for (FileStatus file : files) {
            if (file.getPath().getName().startsWith("part-") && !file.getPath().getName().contains("SUCCESS")) {
                return file.getPath();
            }
        }
        return new Path(outputPath, "part-r-00000");
    }
    
    private static void showJobSummary(Job job, long startTime, long endTime) throws Exception {
        Counters counters = job.getCounters();

        // Use "Job Counters" group — these always exist
        long mapTasks = counters.findCounter("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS").getValue();
        long reduceTasks = counters.findCounter("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES").getValue();

        System.out.println(CYAN + BOLD + "JOB EXECUTION SUMMARY" + RESET);
        System.out.println(CYAN + "===========================================================" + RESET);
        System.out.println("  Total execution time: " + GREEN + String.format("%.2f", (endTime - startTime) / 1000.0) + RESET + " seconds");
        System.out.println("  Map tasks completed: " + GREEN + mapTasks + RESET);
        System.out.println("  Reduce tasks completed: " + GREEN + reduceTasks + RESET);
        System.out.println(CYAN + "===========================================================" + RESET);
        System.out.println();
    }
    
    private static void cleanup(FileSystem fs, Path outputPath) throws Exception {
        fs.delete(outputPath, true);
    }
    
    private static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        return String.format("%.2f MB", bytes / (1024.0 * 1024));
    }
}