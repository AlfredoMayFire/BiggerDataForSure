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
            if (tweet.contains("Flu")){
                context.write(new Text("Flu"), new IntWritable(1));
            }
            if (tweet.contains("Zika")){
                context.write(new Text("Zika"), new IntWritable(1));
            }
            if (tweet.contains("Diarrhea")){
                context.write(new Text("Diarrhea"), new IntWritable(1));
            }
            if (tweet.contains("Ebola")){
                context.write(new Text("Ebola"), new IntWritable(1));
            }
            if (tweet.contains("Headache")){
                context.write(new Text("Headache"), new IntWritable(1));
            }
            if (tweet.contains("Measles")){
                context.write(new Text("Measles"), new IntWritable(1));
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
