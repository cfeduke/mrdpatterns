package com.deploymentzone.mrdpatterns;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.deploymentzone.mrdpatterns.io.MinMaxCountTupleDate;
import com.deploymentzone.mrdpatterns.utils.MRDPUtils;

public class CommentCountByUser extends Configured implements Tool {
  public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTupleDate> {

    private Text outUserId = new Text();
    private MinMaxCountTupleDate outTuple = new MinMaxCountTupleDate();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
      String strDate = parsed.get("CreationDate");
      String userId = parsed.get("UserId");
      Date creationDate;

      if (userId == null) {
        return;
      }

      try {
        creationDate = MinMaxCountTupleDate.FORMAT.parse(strDate);
      } catch (ParseException e) {
        e.printStackTrace();
        return;
      }

      outTuple.set(creationDate, creationDate, 1);

      outUserId.set(userId);

      context.write(outUserId, outTuple);
    }
  }

  public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTupleDate, Text, MinMaxCountTupleDate> {

    private MinMaxCountTupleDate result = new MinMaxCountTupleDate();

    public void reduce(Text key, Iterable<MinMaxCountTupleDate> values, Context context) throws IOException, InterruptedException {
      result.reset();

      int sum = 0;

      for (MinMaxCountTupleDate val : values) {
        result.deriveMinMax(val);
        sum += val.getCount();
      }

      result.setCount(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CommentCountByUser(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: CommentCountByUser <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "StackOverflow Comment Count by User");
    job.setJarByClass(CommentCountByUser.class);
    job.setMapperClass(MinMaxCountMapper.class);
    job.setCombinerClass(MinMaxCountReducer.class);
    job.setReducerClass(MinMaxCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MinMaxCountTupleDate.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
