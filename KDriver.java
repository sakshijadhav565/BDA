import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "K Frequent Words");

        job.setJarByClass(KDriver.class);

        job.setMapperClass(KMapper.class);
        job.setReducerClass(KReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}