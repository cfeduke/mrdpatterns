package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isInteger;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isNullOrEmpty;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserIdsByReputation extends Configured implements Tool {
  public static final String REPUTATION_KEY = "com.deploymentzone.mrdpatterns.minimum_reputation";

  public static class UserIdsByReputationMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
    private int minimumReputation = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      minimumReputation = context.getConfiguration().getInt(REPUTATION_KEY, 0);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String,String> parsed = transformXmlToMap(value.toString());

      String reputation = parsed.get("Reputation");
      if (isNullOrEmpty(reputation) || !isInteger(reputation)) {
        return;
      }
      String userId = parsed.get("Id");
      if (isNullOrEmpty(userId) || !isInteger(userId)) {
        return;
      }

      if (Integer.parseInt(reputation) < minimumReputation) {
        return;
      }

      context.write(new IntWritable(Integer.parseInt(userId)), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new UserIdsByReputation(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: UserIdsByReputation <minimum_reputation> <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }
    Job job = new Job(conf, "User IDs by Reputation");
    job.setJarByClass(UserIdsByReputation.class);
    job.setMapperClass(UserIdsByReputationMapper.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(1);
    job.getConfiguration().setInt(REPUTATION_KEY, Integer.parseInt(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
