package com.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * WordCountDriver  --  the entry point for the Hadoop job.
 *
 * This class wires the Mapper, Reducer, and all job-level settings together,
 * then submits the job to the cluster (or local pseudo-distributed mode).
 *
 * Usage:
 *   hadoop jar wordcount.jar com.wordcount.WordCountDriver <input_path> <output_path>
 *
 * Example:
 *   hadoop jar wordcount.jar com.wordcount.WordCountDriver /input/sample.txt /output/wordcount
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {

        // ------------------------------------------------------------------ //
        //  1. Validate command-line arguments
        // ------------------------------------------------------------------ //
        if (args.length < 2) {
            System.err.println("Usage: WordCountDriver <input path> <output path>");
            System.exit(-1);
        }

        // ------------------------------------------------------------------ //
        //  2. Create a Hadoop Configuration object
        //     (reads core-site.xml, hdfs-site.xml, mapred-site.xml, etc.)
        // ------------------------------------------------------------------ //
        Configuration conf = new Configuration();

        // ------------------------------------------------------------------ //
        //  3. Create the Job and give it a human-readable name
        // ------------------------------------------------------------------ //
        Job job = Job.getInstance(conf, "Word Count");

        // Tell Hadoop where to find the Driver class when distributing the JAR
        job.setJarByClass(WordCountDriver.class);

        // ------------------------------------------------------------------ //
        //  4. Register the Mapper and Reducer
        // ------------------------------------------------------------------ //
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // ------------------------------------------------------------------ //
        //  5. OPTIMISATION: Add a Combiner
        //
        //  The Combiner runs the Reducer logic locally on each Mapper node
        //  BEFORE the shuffle phase.  This dramatically reduces the amount of
        //  data sent across the network when the cluster has many nodes.
        //
        //  For Word Count the Combiner is identical to the Reducer because
        //  addition is both commutative and associative: sum(sum(a,b), c) == sum(a,b,c)
        // ------------------------------------------------------------------ //
        job.setCombinerClass(WordCountReducer.class);

        // ------------------------------------------------------------------ //
        //  6. Declare output key / value types
        // ------------------------------------------------------------------ //
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // ------------------------------------------------------------------ //
        //  7. Set input and output formats
        //     TextInputFormat  = reads one line at a time (default)
        //     TextOutputFormat = writes "key\tvalue" text lines (default)
        // ------------------------------------------------------------------ //
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // ------------------------------------------------------------------ //
        //  8. Set HDFS input / output paths from command-line args
        // ------------------------------------------------------------------ //
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ------------------------------------------------------------------ //
        //  9. Submit and wait for completion
        //     waitForCompletion(true) prints progress to stdout
        //     returns true if the job succeeded, false otherwise
        // ------------------------------------------------------------------ //
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
