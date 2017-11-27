import java.util.*;

import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TopTenJob {
    //Mapper class
    public static class top_ten_map extends MapReduceBase implements
            Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Double, Text> followers = new TreeMap<Double, Text>();

        public void map(Object o, Text value, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {
            // Split on tab to get the cat name and the weight
            String v[] = value.toString().split("\t");
            Double total = Double.parseDouble(v[1]);

            followers.put(total, value);

            if (followers.size() > 10) {
                followers.remove(followers.firstKey());
            }
        }

        protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {

            for ( Text id_user : followers.values() ) {
                context.write(NullWritable.get(),id_user);
            }
        }
    }


    //Reducer class
    public static class top_ten_reducer extends MapReduceBase implements
            Reducer<NullWritable, Text, NullWritable, Text>
    {

        public void reduce(NullWritable nullWritable, Iterator<Text> values, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {
            TreeMap<Double, Text> followers = new TreeMap< Double, Text>();

            while (values.hasNext()) {
                Text value = values.next();
                String v[] = value.toString().split("\t");
                Double weight = Double.parseDouble(v[1]);
                followers.put(weight, value);

                if (followers.size() > 10) {
                    followers.remove(followers.firstKey());
                }
            }

            for (Text t : followers.values()) {
                outputCollector.collect(NullWritable.get(), new Text(t));
            }
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
        conf.setJobName("af_job_3");
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(TopTenJob.top_ten_map.class);
//        conf.setCombinerClass(reducer1.class);
        conf.setReducerClass(TopTenJob.top_ten_reducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
