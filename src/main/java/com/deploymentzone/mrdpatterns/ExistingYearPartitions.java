package com.deploymentzone.mrdpatterns;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.deploymentzone.mrdpatterns.utils.MRDPUtils;

/**
 * Reports all of the years for a given date attribute in the input data.
 *
 * @author cfeduke
 *
 */
public class ExistingYearPartitions extends Configured implements Tool {
  public enum ExceptionCounters {
    FIELD_NOT_FOUND, INVALID_DATE
  }

  public static final String DATE_ATTRIBUTE_NAME = "dateAttributeName";

  public static class DateMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private static String fieldName;
    private static final IntWritable ONE = new IntWritable(1);
    private IntWritable year = new IntWritable();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      fieldName = context.getConfiguration().get(DATE_ATTRIBUTE_NAME);
      if (MRDPUtils.isNullOrEmpty(fieldName)) {
        throw new IllegalArgumentException(DATE_ATTRIBUTE_NAME);
      }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
      if (!parsed.containsKey(fieldName)) {
        context.getCounter(ExceptionCounters.FIELD_NOT_FOUND).increment(1L);
        return;
      }
      String unparsed = parsed.get(fieldName);
      Date date = null;
      try {
        date = MRDPUtils.DATE_FORMAT.parse(unparsed);
      } catch (ParseException e) {
        context.getCounter(ExceptionCounters.INVALID_DATE).increment(1L);
        return;
      }
      year.set(date.getYear() + 1900);
      context.write(year, ONE);
    }
  }

  public static class YearReducer extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable> {
    private LongWritable countWritable = new LongWritable();
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException,
        InterruptedException {
      long count = 0L;
      Iterator<IntWritable> iterator = value.iterator();
      while (iterator.hasNext()) {
        count++;
        iterator.next();
      }
      countWritable.set(count);
      context.write(key, countWritable);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExistingYearPartitions(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: ExistingYearPartitions <date_attribute_name> <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }
    Job job = new Job(conf, "Existing Year Partitions");
    job.setJarByClass(ExistingYearPartitions.class);
    job.setMapperClass(DateMapper.class);
    job.getConfiguration().set(DATE_ATTRIBUTE_NAME, otherArgs[0]);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    // combiners defer to the setMapOutputValueClass
    //job.setCombinerClass(YearReducer.class);
    job.setReducerClass(YearReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
