package com.deploymentzone.mrdpatterns;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.transformXmlToMap;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class WikipediaExtractor extends Mapper<Object, Text, Text, Text> {
  private Text outLink = new Text();
  private Text outKey = new Text();
  Pattern HOSTNAME_MATCHER = Pattern.compile("\\.?wikipedia\\.org/.*", Pattern.CASE_INSENSITIVE);

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Map<String, String> parsed = transformXmlToMap(value.toString());

    String body = parsed.get("Body");
    String posttype = parsed.get("PostTypeId");
    String rowId = parsed.get("Id");

    if (body == null || (posttype != null && posttype.equals("1"))) {
      return;
    }

    body = body.toLowerCase();
    outKey.set(rowId);

    parseAndWriteWikipediaUrls(body, context);
  }

  private void parseAndWriteWikipediaUrls(String body, Context context) throws IOException, InterruptedException {
    Document doc = Jsoup.parse(body);
    for (Element link : doc.select("a")) {
      if (!link.hasAttr("href") || link.attr("href") == null || link.attr("href").length() == 0) {
        continue;
      }
      Matcher matcher = HOSTNAME_MATCHER.matcher(body);
      if (!matcher.matches()) {
        continue;
      }
      outLink.set(link.attr("href"));
      context.write(outLink, outKey);
    }
  }

}
