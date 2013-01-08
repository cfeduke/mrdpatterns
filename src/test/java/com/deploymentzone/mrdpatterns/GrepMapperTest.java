package com.deploymentzone.mrdpatterns;

import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.deploymentzone.mrdpatterns.DistributedGrep.GrepMapper;

@RunWith(MockitoJUnitRunner.class)
public class GrepMapperTest {
  private static final String LINE = "Mr. and Mrs. Stimpson. Stimpson was a Civil Servant, but his life-work";

  @Mock
  private Mapper<Object, Text, NullWritable, Text>.Context context;
  @Mock
  private Configuration configuration;

  private GrepMapper mapper;

  @Before
  public void setUp() throws Exception {
    mapper = new GrepMapper();
    stub(context.getConfiguration()).toReturn(configuration);
  }

  @Test
  public void testWithRegexMatchingLine() throws Exception {
    stub(configuration.get(DistributedGrep.REGEX_KEY, "")).toReturn("Stimpson");

    mapper.setup(context);
    mapper.map(new Object(), new Text(LINE), context);

    verify(context).getConfiguration();
    verify(context).write(NullWritable.get(), new Text(LINE));
    verifyNoMoreInteractions(context);
  }

  @Test
  public void testWithRegexNoMatchingLine() throws Exception {
    stub(configuration.get(DistributedGrep.REGEX_KEY, "")).toReturn("\\d+");

    mapper.setup(context);
    mapper.map(new Object(), new Text(LINE), context);

    verify(context).getConfiguration();
    verifyNoMoreInteractions(context);
  }

}
