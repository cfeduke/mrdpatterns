package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.DATE_FORMAT;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isNullOrEmpty;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

import com.deploymentzone.mrdpatterns.io.MedianStandardDeviationTuple;

public class MedianAndStandardDeviationCommentLengthByHour extends Configured implements Tool {
  public static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private IntWritable outHour = new IntWritable();
    private IntWritable out = new IntWritable();

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

      out.set(text.length());

      context.write(outHour, out);
    }
  }

  public static class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStandardDeviationTuple> {

    private MedianStandardDeviationTuple result = new MedianStandardDeviationTuple();
    private List<Float> commentLengths = new ArrayList<Float>();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      commentLengths.clear();
      result.setStandardDeviation(0);

      for (IntWritable val : values) {
        commentLengths.add((float)val.get());
      }

      result.deriveMedian(commentLengths);
      result.deriveStandardDeviation(commentLengths);

      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MedianAndStandardDeviationCommentLengthByHour(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: MedianAndStandardDeviationCommentLengthByHour <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    Job job = new Job(conf, "StackOverflow Median and Standard Deviation Comment Length By Hour");
    job.setJarByClass(MedianAndStandardDeviationCommentLengthByHour.class);
    job.setMapperClass(MedianStdDevMapper.class);
    job.setReducerClass(MedianStdDevReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
