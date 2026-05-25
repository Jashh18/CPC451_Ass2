package com.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCountReducer
 *
 * INPUT  (after Hadoop's sort & shuffle):
 *   key    = a word (Text)
 *   values = an Iterable of IntWritable 1's -- one per Mapper occurrence
 *            e.g.  "hadoop" -> [1, 1, 1, 1, 1]
 *
 * OUTPUT:
 *   key    = word  (Text)
 *   value  = total count  (IntWritable)
 *            e.g.  "hadoop" -> 5
 *
 * Because Hadoop guarantees all values for the SAME key reach the SAME
 * Reducer call, simply summing the list gives the exact global word count.
 */
public class WordCountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Reuse a single IntWritable for every output write -- avoids GC pressure
    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        // Sum every 1 emitted by the Mappers for this word
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);

        // Emit (word, totalCount)
        context.write(key, result);
    }
}
