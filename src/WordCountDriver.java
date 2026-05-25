package com.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        
        Scanner scanner = new Scanner(System.in);
        
        printBanner();
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        String inputDir = "/input";
        Path inputPath = new Path(inputDir);
        
        if (!fs.exists(inputPath)) {
            System.err.println("Error: Input directory " + inputDir + " does not exist in HDFS!");
            System.err.println("Please create it first using: hdfs dfs -mkdir /input");
            System.err.println("Then upload files using: hdfs dfs -put <local_file> /input/");
            System.exit(-1);
        }
        
        FileStatus[] fileStatuses = fs.listStatus(inputPath);
        List<String> textFiles = new ArrayList<>();
        
        System.out.println("\n📁 Available text files in HDFS (" + inputDir + "):");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
        int counter = 1;
        for (FileStatus status : fileStatuses) {
            if (!status.isDirectory()) {
                String fileName = status.getPath().getName();
                long fileSize = status.getLen();
                String sizeStr = formatFileSize(fileSize);
                textFiles.add(status.getPath().toString());
                System.out.printf("  %d. %-30s [%s]\n", counter++, fileName, sizeStr);
            }
        }
        
        if (textFiles.isEmpty()) {
            System.err.println("No files found in " + inputDir);
            System.err.println("Please upload a text file using: hdfs dfs -put yourfile.txt /input/");
            System.exit(-1);
        }
        
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.print("\n👉 Choose a file (1-" + (counter-1) + "): ");
        int choice = scanner.nextInt();
        
        if (choice < 1 || choice > textFiles.size()) {
            System.err.println("Invalid choice! Exiting...");
            System.exit(-1);
        }
        
        String inputPath_str = textFiles.get(choice - 1);
        System.out.println("\n✅ Selected: " + inputPath_str);
        
        // Use a temporary output directory that will be deleted after showing results
        String tempOutputPath = "/tmp/wordcount_output_" + System.currentTimeMillis();
        
        System.out.println("\n🚀 Starting MapReduce Job...");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        
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
        
        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        
        if (success) {
            System.out.println("\n✅ JOB COMPLETED SUCCESSFULLY!");
            System.out.println("⏱️  Time taken: " + (endTime - startTime) / 1000.0 + " seconds");
            
            // Display results
            displayResults(fs, new Path(tempOutputPath));
            
            // Clean up - delete temporary output directory
            // System.out.println("\n🗑️  Cleaning up temporary files...");
            // fs.delete(new Path(tempOutputPath), true);
            // System.out.println("✅ Cleanup complete!");
            
        } else {
            System.err.println("\n❌ JOB FAILED! Check logs for details.");
            System.exit(1);
        }
        
        scanner.close();
    }
    
    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                              ║");
        System.out.println("║     📊  HADOOP WORD COUNT MAPREDUCE PROGRAM  📊             ║");
        System.out.println("║                                                              ║");
        System.out.println("║         Assignment 2 - Distributed Word Counting           ║");
        System.out.println("║                                                              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }
    
    private static String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        return String.format("%.2f MB", bytes / (1024.0 * 1024));
    }
    
    private static void displayResults(FileSystem fs, Path outputPath) throws Exception {
        System.out.println("\n📊 WORD COUNT RESULTS");
        System.out.println("═══════════════════════════════════════════════════════════════════");
        System.out.printf("%-30s │ %s\n", "WORD", "COUNT");
        System.out.println("─────────────────────────────────────────────────────────────────");
        
        Path resultFile = new Path(outputPath, "part-r-00000");
        if (!fs.exists(resultFile)) {
            FileStatus[] files = fs.listStatus(outputPath);
            for (FileStatus file : files) {
                if (file.getPath().getName().startsWith("part-")) {
                    resultFile = file.getPath();
                    break;
                }
            }
        }
        
        if (fs.exists(resultFile)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(resultFile)));
            String line;
            int totalWords = 0;
            int lineCount = 0;
            
            List<String> allWords = new ArrayList<>();
            
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    totalWords += count;
                    lineCount++;
                    allWords.add(word + " → " + count);
                    System.out.printf("%-30s │ %,d\n", word, count);
                }
            }
            reader.close();
            
            System.out.println("═══════════════════════════════════════════════════════════════════");
            System.out.printf("📈 SUMMARY:\n");
            System.out.printf("   • Total unique words: %,d\n", lineCount);
            System.out.printf("   • Total word occurrences: %,d\n", totalWords);
            
        } else {
            System.out.println("⚠️  Could not find output file.");
        }
        System.out.println();
    }
}