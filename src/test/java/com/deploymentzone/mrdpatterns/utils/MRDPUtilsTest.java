package com.deploymentzone.mrdpatterns.utils;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;

public class MRDPUtilsTest {
  private static final String FIRST_COMMENT = "<row Id=\"1\" PostId=\"35314\" Score=\"1\" Text=\"not sure why this is getting downvoted -- it is correct! Double check it in your compiler if you don't believe him!\" CreationDate=\"2008-09-06T08:07:10.730\" UserId=\"1\" />";
  @Test
  public void transformXmlToMap_WhenValidXML_CreatesTheExpectedMap() {
    Map<String,String> subject = MRDPUtils.transformXmlToMap(FIRST_COMMENT);
    assertThat(subject, hasEntry("Id", "1"));
    assertThat(subject, hasEntry("PostId", "35314"));
    assertThat(subject, hasEntry("Score", "1"));
    assertThat(subject, hasEntry("Text", "not sure why this is getting downvoted -- it is correct! Double check it in your compiler if you don't believe him!"));
    assertThat(subject, hasEntry("UserId", "1"));
  }
}
