package project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.io.IOException;

public class TwitterKeyWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();

            if (tweet.contains("TRUMP")){
                context.write(new Text("TRUMP"), new IntWritable(1));
            }
            if (tweet.contains("FLU")){
                context.write(new Text("FLU"), new IntWritable(1));
            }
            if (tweet.contains("ZIKA")){
                context.write(new Text("ZIKA"), new IntWritable(1));
            }
            if (tweet.contains("DIARRHEA")){
                context.write(new Text("DIARRHEA"), new IntWritable(1));
            }
            if (tweet.contains("EBOLA")){
                context.write(new Text("EBOLA"), new IntWritable(1));
            }
            if (tweet.contains("HEADACHE")){
                context.write(new Text("HEADACHE"), new IntWritable(1));
            }
            if (tweet.contains("MEASLES")){
                context.write(new Text("MEASLES"), new IntWritable(1));
            }
        }

        catch(TwitterException e){}

        // try {
        //     Status status = TwitterObjectFactory.createStatus(rawTweet);
        //     String tweet = status.getText().toUpperCase();
        //     if (tweet.contains("IMPEACH")) {
        //         context.write(new Text("IMPEACH"), new IntWritable(1));
        //     }
        //     else if (tweet.contains("DRAIN")){
        //         context.write(new Text("DRAIN"), new IntWritable(1));
        //     }
        //     else if (tweet.contains("SWAMP")){
        //         context.write(new Text("SWAMP"), new IntWritable(1));
        //     }
        //     else if (tweet.contains("CHANGE")){
        //         context.write(new Text("CHANGE"), new IntWritable(1));
        //     }
        // }
        //
        // catch(TwitterException e) {}
    }
}
