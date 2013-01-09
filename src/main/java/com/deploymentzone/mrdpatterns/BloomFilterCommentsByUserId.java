package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isInteger;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.isNullOrEmpty;
import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.Key;

/**
 * Although this is an HBase example in the book I don't want to setup/configure HBase so transformed this into
 * a Hadoop example.
 * @author cfeduke
 *
 */
public class BloomFilterCommentsByUserId extends Configured implements Tool {
  public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
    private org.apache.hadoop.util.bloom.BloomFilter filter = new org.apache.hadoop.util.bloom.BloomFilter();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      System.out.println("Reading Bloom filter from: " + files[0]);

      DataInputStream stream = new DataInputStream(new FileInputStream(files[0].toString()));
      filter.readFields(stream);
      stream.close();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String,String> parsed = transformXmlToMap(value.toString());

      String body = parsed.get("Text");
      if (isNullOrEmpty(body)) {
        return;
      }
      String userId = parsed.get("UserId");
      if (isNullOrEmpty(userId) || !isInteger(userId)) {
        return;
      }

      // even though the map/reduce job which creates the input for the bloom filter uses IntWritable its still
      // just text on disk - and the bloom filter was created using those string values (inefficient as it may be)
      // so we just use the string values here - for optimization the bloom filter should be changed to use the
      // bytes of the integer value
      if (filter.membershipTest(new Key(userId.getBytes()))) {
        context.write(value, NullWritable.get());
      }

    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BloomFilterCommentsByUserId(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: BloomFilterCommentsByUserId <bloom_filter_file> <in> <out>");
      ToolRunner.printGenericCommandUsage(System.err);
      System.exit(2);
    }

    DistributedCache.addCacheFile(new URI(otherArgs[0]), conf);
    Job job = new Job(conf, "Bloom Filter Comments by User ID");
    job.setJarByClass(BloomFilterCommentsByUserId.class);
    job.setMapperClass(BloomFilterMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }
}
