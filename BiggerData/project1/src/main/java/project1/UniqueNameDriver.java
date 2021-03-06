package project1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueNameDriver {
	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UniqueNames <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(project1.UniqueNameDriver.class);
        job.setJobName("TwitterUniqueName");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(project1.UniqueNameMapper.class);
        job.setReducerClass(project1.UniqueNameReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
