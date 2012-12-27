package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.ResourceHelper.getLinesFromResourceFile;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.deploymentzone.mrdpatterns.WikipediaExtractor.WikipediaUrlMapper;

@RunWith(MockitoJUnitRunner.class)
public class WikipediaUrlMapperTest {

  @Mock
  private Mapper<Object,Text,Text,Text>.Context context;
  private WikipediaUrlMapper mapper;
  private String inputXml;

  @Before
  public void setUp() throws IOException {
    mapper = new WikipediaUrlMapper();
    inputXml = getLinesFromResourceFile(this.getClass(), "posts_with_wikipedia_urls.xml").get(0);
  }

  @Test
  public void testMap() throws IOException, InterruptedException {
    mapper.map(new Object(), new Text(inputXml), context);

    verify(context).write(new Text("http://en.wikipedia.org/wiki/avl_tree"), new Text("20839"));
    verify(context).write(new Text("http://en.wikipedia.org/wiki/birthday_paradox"), new Text("20839"));

    verifyNoMoreInteractions(context);
  }

}
