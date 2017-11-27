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
        Set<Text> follower_set = new HashSet();

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, IntWritable> outputCollector,
                           Reporter reporter) throws IOException {

            while (values.hasNext()){
                follower_set.add(values.next());
            }
            outputCollector.collect(new Text(key), new IntWritable(follower_set.size()));
        }
    }


    //Main function
    public static void main(String args[])throws Exception
    {
        JobConf conf = new JobConf(Job1.class);

        conf.setJobName("af_job_1");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
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
