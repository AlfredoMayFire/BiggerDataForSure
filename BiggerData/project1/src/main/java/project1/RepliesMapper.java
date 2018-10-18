package project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import java.io.IOException;

public class RepliesMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            long replyMsgId = status.getId();
            long msgReplied2Id = status.getInReplyToUserId();

            if(msgReplied2Id != -1){
                String replyingMsgIdString;
                replyingMsgIdString = Long.toString(replyMsgId);
                // System.out.println("Key: " + msgReplied2Id);
                context.write(new LongWritable(msgReplied2Id), new Text(replyingMsgIdString));
            }
        }

        catch(TwitterException e) {}
    }
}
