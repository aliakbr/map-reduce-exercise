import java.util.*;

import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Job2 {
    //Mapper class
    public static class mapper2 extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable longWritable, Text value,
                        OutputCollector<Text, Text> outputCollector,
                        Reporter reporter) throws IOException {
            String[] token = value.toString().split("\t");
            outputCollector.collect(new Text(token[0]), new Text(token[1]));
        }
    }


    //Reducer class
    public static class reducer2 extends MapReduceBase implements
            Reducer<Text, Text, Text, IntWritable>
    {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, IntWritable> outputCollector,
                           Reporter reporter) throws IOException {
            Set<Text> follower_set = new HashSet();

            while (values.hasNext()){
                follower_set.add(values.next());
            }
            outputCollector.collect(new Text(key), new IntWritable(follower_set.size()));
        }
    }


    //Main function
    public static void main(String args[])throws Exception
    {

        JobConf conf = new JobConf(Job2.class);
        conf.set("mapred.child.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.java.opts", "-Xmx2048m");
        conf.set("mapreduce.map.memory.mb", "4096");

        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.2");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.setJobName("af_job_2");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(mapper2.class);
//        conf.setCombinerClass(reducer1.class);
        conf.setReducerClass(reducer2.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
