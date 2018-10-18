package project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountUserMsgReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        String completeMsg = " ";

        for (Text value : values){
            completeMsg = completeMsg.concat(value.toString()) + " ";
            count = count + 1;
            // System.out.println(count);
        }

        String stringCount = Integer.toString(count);
        completeMsg = completeMsg.concat("" + stringCount);
        context.write(key, new Text(completeMsg));
    }
}
