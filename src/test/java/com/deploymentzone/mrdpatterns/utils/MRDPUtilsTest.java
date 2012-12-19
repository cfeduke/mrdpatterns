package com.deploymentzone.mrdpatterns.utils;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;

public class MRDPUtilsTest {
  private static final String FIRST_COMMENT = "<row Id=\"1\" PostId=\"35314\" Score=\"1\" Text=\"not sure why this is getting downvoted -- it is correct! Double check it in your compiler if you don't believe him!\" CreationDate=\"2008-09-06T08:07:10.730\" UserId=\"1\" />";
  private static final String EMBEDDED_QUOTE_COMMENT = "<row Id=\"1350664\" PostId=\"1497954\" Text=\"is there any way to debug in &quot;GIT&quot; ? i.e. trace or something ? how to get this into the output ?\" CreationDate=\"2009-09-30T13:24:13.350\" UserId=\"181811\" />";

  @Test
  public void transformXmlToMap_WhenValidXML_CreatesTheExpectedMap() {
    Map<String,String> subject = MRDPUtils.transformXmlToMap(FIRST_COMMENT);
    assertThat(subject, hasEntry("Id", "1"));
    assertThat(subject, hasEntry("PostId", "35314"));
    assertThat(subject, hasEntry("Score", "1"));
    assertThat(subject, hasEntry("Text", "not sure why this is getting downvoted -- it is correct! Double check it in your compiler if you don't believe him!"));
    assertThat(subject, hasEntry("UserId", "1"));
  }

  @Test
  public void transformXmlToMap_WhenValidXMLWithEscapedHTML_RemovesHTML() {
    Map<String,String> subject = MRDPUtils.transformXmlToMap(EMBEDDED_QUOTE_COMMENT);
    assertThat(subject, hasEntry("Text", "is there any way to debug in \"GIT\" ? i.e. trace or something ? how to get this into the output ?"));
  }
}
