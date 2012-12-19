package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isNullOrEmpty;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.deploymentzone.mrdpatterns.io.CountAverageTuple;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.*;

public class AverageCommentLengthByHour extends Configured implements Tool {
  public static class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {

    private IntWritable outHour = new IntWritable();
    private CountAverageTuple outTuple = new CountAverageTuple();

    @SuppressWarnings("deprecation")
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String,String> parsed = transformXmlToMap(value.toString());

      String strDate = parsed.get("CreationDate");
      String text = parsed.get("Text");

      if (isNullOrEmpty(strDate) || isNullOrEmpty(text)) {
        return;
      }

      Date creationDate;

      try {
        creationDate = DATE_FORMAT.parse(strDate);
      } catch (ParseException e) {
        e.printStackTrace();
        return;
      }

      outHour.set(creationDate.getHours());

      outTuple.set(1, text.length());

      context.write(outHour, outTuple);
    }
  }

  public static class AverageReducer extends Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {

    private CountAverageTuple result = new CountAverageTuple();

    public void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {

      int sum = 0;
      float count = 0;

      for (CountAverageTuple val : values) {
        sum += val.getCount() * val.getAverage();
        count += val.getCount();
      }

      result.setCount(sum);
      result.setAverage(sum / count);

      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AverageCommentLengthByHour(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: AverageCommentLengthByHour <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "StackOverflow Average Comment Length By Hour");
    job.setJarByClass(AverageCommentLengthByHour.class);
    job.setMapperClass(AverageMapper.class);
    job.setCombinerClass(AverageReducer.class);
    job.setReducerClass(AverageReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(CountAverageTuple.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
