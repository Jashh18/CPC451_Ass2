package com.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WordCountMapper
 *
 * INPUT:
 *   key   = byte offset of the line (LongWritable) -- Hadoop provides this automatically
 *   value = one line of raw text (Text)
 *
 * OUTPUT:
 *   key   = individual word (Text)
 *   value = integer 1 (IntWritable)  -- meaning "I saw this word once"
 *
 * Hadoop will SORT and GROUP all identical keys from every Mapper
 * before sending them to the Reducer.
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reusing these objects avoids creating millions of short-lived objects
    // during a large job -- an easy but impactful optimisation.
    private final static IntWritable ONE  = new IntWritable(1);
    private final Text               word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Convert the Hadoop Text line to a plain Java String
        String line = value.toString();

        // Remove punctuation and convert to lowercase so
        // "Hello", "hello," and "HELLO" are counted as the same word.
        // This is a simple but effective normalisation step.
        line = line.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();

        // StringTokenizer splits on whitespace (spaces, tabs, newlines)
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().trim();

            // Skip empty tokens and very short words (optional filter)
            if (token.isEmpty()) continue;

            word.set(token);

            // Emit (word, 1) -- "I saw 'word' one time on this line"
            context.write(word, ONE);
        }
    }
}