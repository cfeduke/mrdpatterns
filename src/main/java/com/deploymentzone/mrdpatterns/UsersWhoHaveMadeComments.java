package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UsersWhoHaveMadeComments extends Configured implements Tool {
  public static class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable> {

    private Text outUserId = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String,String> parsed = transformXmlToMap(value.toString());
      String userId = parsed.get("UserId");

      if (userId == null) {
        return;
      }

      outUserId.set(userId);

      context.write(outUserId, NullWritable.get());
    }
  }

  public static class DistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new UsersWhoHaveMadeComments(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: UsersWhoHaveMadeComments <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "Users Who Have Made Comments");
    job.setJarByClass(UsersWhoHaveMadeComments.class);
    job.setMapperClass(DistinctUserMapper.class);
    job.setCombinerClass(DistinctUserReducer.class);
    job.setReducerClass(DistinctUserReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
