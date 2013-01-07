package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isNullOrEmpty;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NumberOfUsersByState extends Configured implements Tool {
  public static class CountUsersByStateMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
    public static final String STATE_COUNTER_GROUP = "State";
    public static final String UNKNOWN_COUNTER = "Unknown";
    public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";

    private static final Set<String> US_STATES = new HashSet<String>(Arrays.asList(new String[] { "AL", "AK", "AZ",
        "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
        "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
        "SC", "SF", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY" }));

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String,String> parsed = transformXmlToMap(value.toString());

      String location = parsed.get("Location");
      if (isNullOrEmpty(location)) {
        context.getCounter(STATE_COUNTER_GROUP, NULL_OR_EMPTY_COUNTER).increment(1);
        return;
      }

      String[] tokens = location.toUpperCase().split("\\s");

      boolean unknown = true;
      for (String token : tokens) {
        token = token.replaceAll(",", "");
        if (US_STATES.contains(token)) {
          context.getCounter(STATE_COUNTER_GROUP, token).increment(1);
          unknown = false;
          break;
        }
      }

      if (unknown) {
        context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
      }
    }

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new NumberOfUsersByState(), args);
    System.exit(res);
  }

  @SuppressWarnings("deprecation")
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: NumberOfUsersByState <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "StackOverflow Number of Users by State");
    job.setJarByClass(NumberOfUsersByState.class);
    job.setMapperClass(CountUsersByStateMapper.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    Path outputDir = new Path(otherArgs[1]);
    FileOutputFormat.setOutputPath(job, outputDir);
    boolean success = job.waitForCompletion(true);

    if (success) {
      for (Counter counter : job.getCounters().getGroup(CountUsersByStateMapper.STATE_COUNTER_GROUP)) {
        System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
      }
    }

    FileSystem.get(conf).delete(outputDir);

    return success ? 0 : 1;
  }
}
