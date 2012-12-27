package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.ResourceHelper.getFirstLineFromResourceFile;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.deploymentzone.mrdpatterns.NumberOfUsersByState.CountUsersByStateMapper;

@RunWith(MockitoJUnitRunner.class)
public class CountUsersByStateMapperTest {
  @Mock
  private Mapper<Object, Text, NullWritable, NullWritable>.Context context;
  @Mock
  private Counter stateCounter;
  @Mock
  private Counter nullOrEmptyCounter;
  @Mock
  private Counter unknownCounter;

  private CountUsersByStateMapper mapper;

  @Before
  public void setUp() throws Exception {
    mapper = new CountUsersByStateMapper();
    stub(context.getCounter(CountUsersByStateMapper.STATE_COUNTER_GROUP, "VA")).toReturn(stateCounter);
    stub(context.getCounter(CountUsersByStateMapper.STATE_COUNTER_GROUP, CountUsersByStateMapper.NULL_OR_EMPTY_COUNTER))
        .toReturn(nullOrEmptyCounter);
    stub(context.getCounter(CountUsersByStateMapper.STATE_COUNTER_GROUP, CountUsersByStateMapper.UNKNOWN_COUNTER))
        .toReturn(unknownCounter);
  }

  @Test
  public void testWithValidState() throws Exception {
    String line = getFirstLineFromResourceFile(this.getClass(), "users_with_us_state.xml");
    mapper.map(new Object(), new Text(line), context);
    verify(stateCounter).increment(1);

    verifyNoMore();
  }

  @Test
  public void testWithNullState() throws Exception {
    mapper.map(new Object(), new Text("<row Id=\"3\" Location=\"\" />"), context);
    verify(nullOrEmptyCounter).increment(1);

    verifyNoMore();
  }

  @Test
  public void testWithUnknownState() throws Exception {
    mapper.map(new Object(), new Text("<row Id=\"3\" Location=\"Ireland\" />"), context);
    verify(unknownCounter).increment(1);

    verifyNoMore();
  }

  private void verifyNoMore() {
    verifyNoMoreInteractions(stateCounter, nullOrEmptyCounter, unknownCounter);
  }

}
