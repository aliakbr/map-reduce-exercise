import java.util.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopTenJob {

    //Mapper class
    public static class top_ten_map extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Double, Text> followers = new TreeMap<Double, Text>();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split on tab to get the cat name and the weight
            String v[] = value.toString().split("\t");
            Double total = Double.parseDouble(v[1]);
            followers.put(new Double(total), new Text(value));

            if (followers.size() > 10) {
                followers.remove(followers.firstKey());
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for ( Text a : followers.values() ) {
                context.write(NullWritable.get(), a);
            }
        }
    }


    //Reducer class
    public static class top_ten_reducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            TreeMap<Double, Text> followers = new TreeMap< Double, Text>();

            for (Text value : values) {
                String v[] = value.toString().split("\t");
                Double weight = Double.parseDouble(v[1]);
                followers.put(new Double(weight), new Text(value));

                if (followers.size() > 10) {
                    followers.remove(followers.firstKey());
                }
            }

            for (Text t : followers.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }


    //Main function
    public static void main(String args[])throws Exception
    {
        Configuration conf= new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "4096");

        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.2");
        conf.set("mapreduce.reduce.memory.mb", "4096");

        Job job = new Job(conf,"af_top_ten");
        job.setJarByClass(TopTenJob.class);

        //Providing the mapper and reducer class names
        job.setMapperClass(TopTenJob.top_ten_map.class);
        job.setReducerClass(TopTenJob.top_ten_reducer.class);

        job.setNumReduceTasks(1);

        //Setting
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(args[1]);


        //Configure input/output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Exiting the job if flag is false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
