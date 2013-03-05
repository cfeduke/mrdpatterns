package com.deploymentzone.mrdpatterns;

import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.deploymentzone.mrdpatterns.ExistingYearPartitions.DateMapper;
import com.deploymentzone.mrdpatterns.ExistingYearPartitions.YearReducer;

@RunWith(MockitoJUnitRunner.class)
public class ExistingYearPartitionsTest {
  @Mock
  private Mapper<Object, Text, IntWritable, IntWritable>.Context mapperContext;
  @Mock
  private Reducer<IntWritable, IntWritable, IntWritable, LongWritable>.Context reducerContext;
  @Mock
  private Configuration configuration;
  @Mock
  private Counter invalidDateCounter;


  @Before
  public void setUp() throws Exception {
    stub(mapperContext.getConfiguration()).toReturn(configuration);
    stub(configuration.get(ExistingYearPartitions.DATE_ATTRIBUTE_NAME)).toReturn("LastAccessDate");
    stub(mapperContext.getCounter(ExistingYearPartitions.ExceptionCounters.INVALID_DATE)).toReturn(invalidDateCounter);
  }

  @Test
  public void testWithValidDateOutputsOne() throws Exception {
    final String LINE = "  <row Id=\"185341\" Reputation=\"65\" CreationDate=\"2009-09-15T10:52:56.767\" DisplayName=\"anirudha\" LastAccessDate=\"2009-10-10T17:44:21.030\" WebsiteUrl=\"http://anirudhagupta.blogspot.com\" Location=\"new delhi\" Age=\"24\" AboutMe=\"I am Asp.net MVC Web Developer&#xD;&#xA;&lt;br/&gt;&#xD;&#xA;I currently Focused on MVC or Jquery &#xD;&#xA;&lt;br/&gt;&#xD;&#xA;You can contact us :-&#xD;&#xA;&lt;br/&gt;&#xD;&#xA;Anirudhakumar.gupta@gmail.com\" Views=\"495\" UpVotes=\"6\" DownVotes=\"0\" />";

    DateMapper mapper = new DateMapper();
    mapper.setup(mapperContext);
    mapper.map(new Object(), new Text(LINE), mapperContext);

    verify(mapperContext).getConfiguration();
    verify(mapperContext).write(new IntWritable(2009), new IntWritable(1));
    verifyNoMoreInteractions(invalidDateCounter);
    verifyNoMoreInteractions(mapperContext);
  }

  @Test
  public void reduceWithValidDate() throws Exception {
    YearReducer reducer = new YearReducer();
    List<IntWritable> iterable = Collections.<IntWritable>nCopies(7, new IntWritable(1));
    reducer.reduce(new IntWritable(2009), iterable, reducerContext);

    verify(reducerContext).write(new IntWritable(2009), new LongWritable(7));
    verifyNoMoreInteractions(reducerContext);
  }

}
