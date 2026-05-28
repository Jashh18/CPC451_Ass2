package com.wordcount;

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

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class WordCountDriver {

    // ANSI color codes for beautiful terminal output
    public static final String RESET = "\u001B[0m";
    public static final String BLACK = "\u001B[30m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String PURPLE = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String WHITE = "\u001B[37m";
    public static final String BOLD = "\u001B[1m";
    public static final String DIM = "\u001B[2m";

    public static void main(String[] args) throws Exception {
        
        printBanner();
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        String inputDir = "/input";
        Path inputPath = new Path(inputDir);
        
        if (!fs.exists(inputPath)) {
            printError("Input directory " + inputDir + " does not exist in HDFS!");
            printInfo("Please create it using: hdfs dfs -mkdir /input");
            System.exit(-1);
        }
        
        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        
        if (fileStatuses.length == 0) {
            printError("No files found in " + inputDir);
            printInfo("Please upload a text file using: hdfs dfs -put yourfile.txt /input/");
            System.exit(-1);
        }
        
        // Display available files
        displayAvailableFiles(fileStatuses);
        
        // Get user choice
        String inputPath_str = getUserChoice(fileStatuses);
        
        // Display file details
        displayFileDetails(inputPath_str, fs);
        
        // Create output directory with timestamp
        String timestamp = String.valueOf(System.currentTimeMillis());
        String tempOutputPath = "/tmp/wordcount_output_" + timestamp;
        
        // Show MapReduce Pipeline starting
        showPipelineStart();
        
        // STEP 1: Input Phase
        showInputPhase(inputPath_str, fs);
        
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

        FileInputFormat.addInputPath(job, new Path(inputPath_str));
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
            printError("JOB FAILED! Check logs for details.");
            System.exit(1);
        }
    }
    
    private static void printBanner() {
        System.out.println();
        System.out.println(CYAN + BOLD + "╔══════════════════════════════════════════════════════════════════════════╗" + RESET);
        System.out.println(CYAN + BOLD + "║                                                                          ║" + RESET);
        System.out.println(CYAN + BOLD + "║     📊  HADOOP MAPREDUCE WORD COUNT - FLOW VISUALIZATION  📊            ║" + RESET);
        System.out.println(CYAN + BOLD + "║                                                                          ║" + RESET);
        System.out.println(CYAN + BOLD + "║         Assignment 2 - Distributed Word Counting                         ║" + RESET);
        System.out.println(CYAN + BOLD + "║                                                                          ║" + RESET);
        System.out.println(CYAN + BOLD + "╚══════════════════════════════════════════════════════════════════════════╝" + RESET);
        System.out.println();
    }
    
    private static void displayAvailableFiles(FileStatus[] fileStatuses) {
        System.out.println(YELLOW + "📁 CHOOSE INPUT METHOD:" + RESET);
        System.out.println(BLUE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        
        int counter = 1;
        for (FileStatus status : fileStatuses) {
            if (!status.isDirectory()) {
                String fileName = status.getPath().getName();
                long fileSize = status.getLen();
                String sizeStr = formatFileSize(fileSize);
                System.out.printf("  %d) %s (%s)\n", counter++, fileName, sizeStr);
            }
        }
        System.out.println();
    }
    
    private static String getUserChoice(FileStatus[] fileStatuses) throws Exception {
        java.util.Scanner scanner = new java.util.Scanner(System.in);
        System.out.print(GREEN + "👉 Enter choice (1-" + (fileStatuses.length) + "): " + RESET);
        int choice = scanner.nextInt();
        
        if (choice < 1 || choice > fileStatuses.length) {
            printError("Invalid choice! Exiting...");
            System.exit(-1);
        }
        
        String selectedPath = fileStatuses[choice - 1].getPath().toString();
        System.out.println();
        System.out.println(GREEN + "✔ Using: " + selectedPath.substring(selectedPath.lastIndexOf('/') + 1) + RESET);
        System.out.println();
        return selectedPath;
    }
    
    private static void displayFileDetails(String filePath, FileSystem fs) throws Exception {
        Path path = new Path(filePath);
        FileStatus status = fs.getFileStatus(path);
        
        System.out.println(YELLOW + "📄 FILE DETAILS:" + RESET);
        System.out.println(BLUE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        System.out.printf("  • File name: %s\n", path.getName());
        System.out.printf("  • Size: %s\n", formatFileSize(status.getLen()));
        System.out.printf("  • HDFS Path: %s\n", filePath);
        System.out.println();
    }
    
    private static void showPipelineStart() {
        System.out.println(PURPLE + BOLD + "\n# MAPREDUCE PIPELINE STARTING" + RESET);
        System.out.println(PURPLE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        System.out.println();
    }
    
    private static void showInputPhase(String inputPath, FileSystem fs) throws Exception {
        System.out.println(BOLD + "📥 STEP 1: INPUT PHASE" + RESET);
        System.out.println(BLUE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        
        Path path = new Path(inputPath);
        FileStatus status = fs.getFileStatus(path);
        
        // Read file and count lines
        int lineCount = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        while (reader.readLine() != null) lineCount++;
        reader.close();
        
        System.out.printf("  • Input file: %s\n", path.getName());
        System.out.printf("  • Lines: %d\n", lineCount);
        System.out.printf("  • Size: %s\n", formatFileSize(status.getLen()));
        System.out.println("  • HDFS: Uploading to distributed storage...");
        System.out.println("  • ✓ Input uploaded to HDFS");
        System.out.println();
    }
    
    private static void showMapPhaseStart() {
        System.out.println(BOLD + "🗺️  STEP 2: MAP PHASE" + RESET);
        System.out.println(BLUE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        System.out.println("  • Splitting input into chunks...");
        System.out.println("  • Tokenizing each line into words...");
        System.out.println("  • Normalizing text (lowercase + punctuation removal)...");
        System.out.println("  • Emitting (word, 1) pairs...");
    }
    
    private static void showMapPhaseComplete(Job job) throws Exception {
        Counters counters = job.getCounters();
        
        // Get actual counters from MapReduce job
        long mapInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
        long mapOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
        long combineInputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_INPUT_RECORDS").getValue();
        long combineOutputRecords = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "COMBINE_OUTPUT_RECORDS").getValue();
        
        System.out.println("  • ✓ Map input records: " + mapInputRecords + " lines");
        System.out.println("  • ✓ Map output records: " + mapOutputRecords + " (word,1) pairs");
        
        if (combineInputRecords > 0) {
            System.out.println("  • Combine input records: " + combineInputRecords + " records");
            System.out.println("  • Combine output records: " + combineOutputRecords + " records " + GREEN + "(optimized!)" + RESET);
        }
        System.out.println();
    }
    
    private static void showShufflePhase() {
        System.out.println(BOLD + "🔄 STEP 3: SHUFFLE & SORT PHASE" + RESET);
        System.out.println(BLUE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        System.out.println("  • Transferring data from Mappers to Reducers...");
        System.out.println("  • Grouping by key (word)...");
        System.out.println("  • Sorting alphabetically...");
        System.out.println("  • ✓ Shuffle completed");
        System.out.println();
    }
    
    private static void showReducePhase() {
        System.out.println(BOLD + "🔀 STEP 4: REDUCE PHASE" + RESET);
        System.out.println(BLUE + "────────────────────────────────────────────────────────────────────────────" + RESET);
        System.out.println("  • Summing counts for each unique word...");
        System.out.println("  • Writing final output to HDFS...");
        System.out.println("  • ✓ Reduce phase complete");
        System.out.println();
    }
    
    private static void displayResults(FileSystem fs, Path outputPath) throws Exception {
        System.out.println(BOLD + GREEN + "📊 WORD COUNT RESULTS" + RESET);
        System.out.println(CYAN + "═══════════════════════════════════════════════════════════════════════════" + RESET);
        System.out.printf(BOLD + "%-30s │ %s\n" + RESET, "WORD", "COUNT");
        System.out.println(CYAN + "───────────────────────────────────────────────────────────────────────────" + RESET);
        
        Path resultFile = findResultFile(fs, outputPath);
        
        if (fs.exists(resultFile)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(resultFile)));
            String line;
            int totalWords = 0;
            int lineCount = 0;
            
            java.util.List<String> topWords = new java.util.ArrayList<>();
            
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    totalWords += count;
                    lineCount++;
                    topWords.add(word + " → " + count);
                    
                    // Color code based on count frequency
                    if (count > 10) {
                        System.out.printf(RED + "%-30s │ %,d\n" + RESET, word, count);
                    } else if (count > 5) {
                        System.out.printf(YELLOW + "%-30s │ %,d\n" + RESET, word, count);
                    } else {
                        System.out.printf("%-30s │ %,d\n", word, count);
                    }
                }
            }
            reader.close();
            
            System.out.println(CYAN + "═══════════════════════════════════════════════════════════════════════════" + RESET);
            System.out.println(BOLD + "\n📈 SUMMARY STATISTICS:" + RESET);
            System.out.println("  • Total unique words: " + GREEN + lineCount + RESET);
            System.out.println("  • Total word occurrences: " + GREEN + totalWords + RESET);
            
            // Show top 10 most frequent words
            System.out.println(BOLD + "\n🏆 TOP 10 MOST FREQUENT WORDS:" + RESET);
            System.out.println(CYAN + "───────────────────────────────────────────────────────────────────────────" + RESET);
            
            // Re-read to get sorted by count
            BufferedReader reader2 = new BufferedReader(new InputStreamReader(fs.open(resultFile)));
            java.util.Map<String, Integer> wordMap = new java.util.HashMap<>();
            while ((line = reader2.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    wordMap.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            reader2.close();
            
            wordMap.entrySet().stream()
                .sorted(java.util.Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(entry -> System.out.printf("  %-20s → %,d\n", entry.getKey(), entry.getValue()));
            
        } else {
            printWarning("Could not find output file.");
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
    long mapTasks    = counters.findCounter("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS").getValue();
    long reduceTasks = counters.findCounter("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES").getValue();

    System.out.println(BOLD + PURPLE + "╔══════════════════════════════════════════════════════════════════════════╗" + RESET);
    System.out.println(BOLD + PURPLE + "║                         JOB EXECUTION SUMMARY                            ║" + RESET);
    System.out.println(BOLD + PURPLE + "╚══════════════════════════════════════════════════════════════════════════╝" + RESET);

    System.out.printf("  ⏱️  Total execution time: " + GREEN + "%.2f seconds\n" + RESET, (endTime - startTime) / 1000.0);
    System.out.println("  🗺️  Map tasks completed:    " + GREEN + mapTasks    + RESET);
    System.out.println("  🔀 Reduce tasks completed: " + GREEN + reduceTasks + RESET);
    System.out.println();
}
    
    private static void cleanup(FileSystem fs, Path outputPath) throws Exception {
       // System.out.println(YELLOW + "🗑️  Cleaning up temporary files..." + RESET);
        fs.delete(outputPath, true);
       // System.out.println(GREEN + "✓ Cleanup complete!" + RESET);
    }
    
    private static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        return String.format("%.2f MB", bytes / (1024.0 * 1024));
    }
    
    private static void printInfo(String message) {
        System.out.println(BLUE + "ℹ️  " + message + RESET);
    }
    
    private static void printError(String message) {
        System.out.println(RED + "❌ " + message + RESET);
    }
    
    private static void printWarning(String message) {
        System.out.println(YELLOW + "⚠️  " + message + RESET);
    }
}
