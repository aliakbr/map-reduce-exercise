import java.util.*;

import java.io.IOException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Job1 {
    public static String list_to_string(List<String> a){
        String result = "";
        for (int i = 0; i < a.size(); i++){
            if (result.isEmpty()){
                result += a.get(i);
            }
            else{
                result += ","+a.get(i);
            }
        }
        return result;
    }

    //Mapper class
    public static class mapper1 extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable longWritable, Text value,
                        OutputCollector<Text, Text> outputCollector,
                        Reporter reporter) throws IOException {
            String[] token = value.toString().split("\t");
            String s1 = token[1] + ",1"; //follower
            String s2 = token[0] + ",0"; //following
            outputCollector.collect(new Text(token[0]), new Text(s1));
            outputCollector.collect(new Text(token[1]), new Text(s2));
        }
    }


    //Reducer class
    public static class reducer1 extends MapReduceBase implements
            Reducer<Text, Text, Text, Text>
    {
        List<String> following_list = new ArrayList<String>();
        List<String> follower_list = new ArrayList<String>();

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> outputCollector,
                           Reporter reporter) throws IOException {
            follower_list.clear();
            following_list.clear();
            while (values.hasNext()){
                String[] token = values.next().toString().split(",");
                if (token[1].equals("1")){
                    follower_list.add(token[0]);
                }
                else{
                    following_list.add(token[1]);
                }
            }

            for (int i = 0; i < follower_list.size(); i++){
                for (int j = 0; j < following_list.size(); j++){
                    outputCollector.collect(new Text(following_list.get(j)),
                            new Text(following_list.get(i)));
                }

                outputCollector.collect(new Text(key), new Text(follower_list.get(i)));
            }

        }
    }


    //Main function
    public static void main(String args[])throws Exception
    {
        JobConf conf = new JobConf(Job1.class);

        conf.setJobName("af_job_1");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(mapper1.class);
//        conf.setCombinerClass(reducer1.class);
        conf.setReducerClass(reducer1.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
