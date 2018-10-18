package project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RepliesReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String allIds = " ";
        System.out.println("Key: " + key);
        for (Text value : values){
            System.out.println(value.toString());
            allIds = allIds.concat(value.toString()) + " ";
        }
        context.write(key, new Text(allIds));
    }
}
