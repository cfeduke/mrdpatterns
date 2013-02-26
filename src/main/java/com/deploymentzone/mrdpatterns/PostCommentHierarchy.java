package com.deploymentzone.mrdpatterns;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.deploymentzone.mrdpatterns.utils.MRDPUtils;

public class PostCommentHierarchy extends Configured implements Tool {
  public enum ExceptionCounters { INVALID_XML, NO_ID_FIELD, UNRECORDABLE_DATA }

  public static void main(String[] args) throws Exception {
    System.exit(new PostCommentHierarchy().run(args));
  }
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "PostCommentHeirarchy");
    job.setJarByClass(PostCommentHierarchy.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentMapper.class);

    job.setReducerClass(PostCommentHierarchyReducer.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(args[2]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    return job.waitForCompletion(true) ? 0 : 2;
  }

  public static abstract class HierarchyMapper extends Mapper<Object, Text, Text, Text> {
    private Text outKey = new Text(), outValue = new Text();

    private final String fieldName;
    private final String valuePrefix;

    protected HierarchyMapper(String fieldName, String valuePrefix) {
      this.fieldName = fieldName;
      this.valuePrefix = valuePrefix;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
      String parsedId = parsed.get(fieldName);
      if (parsedId == null) {
        context.getCounter(ExceptionCounters.NO_ID_FIELD).increment(1);
        return;
      }

      outKey.set(parsedId);
      outValue.set(valuePrefix + value.toString());
      context.write(outKey, outValue);
    }
  }

  public static class PostMapper extends HierarchyMapper {
    public PostMapper() {
      super("Id", "P");
    }
  }

  public static class CommentMapper extends HierarchyMapper {
    public CommentMapper() {
      super("PostId", "C");
    }
  }

  public static class PostCommentHierarchyReducer extends Reducer<Text, Text, Text, NullWritable> {
    private List<String> comments = new LinkedList<String>();
    private String post = null;

    public enum DataTypeCounters { TOTAL_POSTS, TOTAL_COMMENTS }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      reset();

      for (Text t : values) {
        PostCommentDiscriminator pcd = PostCommentDiscriminator.parse(t.toString());
        if (pcd.isUnrecordable()) {
          context.getCounter(ExceptionCounters.UNRECORDABLE_DATA).increment(1);
          return;
        }
        if (pcd.getType() == PostCommentDiscriminator.Type.POST) {
          post = pcd.getData();
          context.getCounter(DataTypeCounters.TOTAL_POSTS).increment(1);
        } else {
          comments.add(pcd.getData());
          context.getCounter(DataTypeCounters.TOTAL_COMMENTS).increment(1);
        }
      }
      PostCommentsXmlDocument doc;
      try {
        doc = new PostCommentsXmlDocument(post, comments);
      } catch (Exception e) {
        e.printStackTrace();
        context.getCounter(ExceptionCounters.INVALID_XML).increment(1);
        return;
      }
      context.write(new Text(doc.toString()), NullWritable.get());
    }

    private void reset() {
      post = null;
      comments.clear();
    }

    private static class PostCommentsXmlDocument {
      private static final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      private final Document doc;

      public PostCommentsXmlDocument(String post, List<String> comments) throws ParserConfigurationException,
          SAXException, IOException {
        DocumentBuilder builder = dbf.newDocumentBuilder();
        doc = builder.newDocument();

        Element postEl = getXmlElementFromString(post);
        Element toAddPostEl = doc.createElement("post");

        copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

        for (String commentXml : comments) {
          Element commentEl = getXmlElementFromString(commentXml);
          Element toAddCommentEl = doc.createElement("comments");

          copyAttributesToElement(commentEl.getAttributes(), toAddCommentEl);
          toAddPostEl.appendChild(toAddCommentEl);
        }
        doc.appendChild(toAddPostEl);
      }

      private Element getXmlElementFromString(String xml) throws ParserConfigurationException, SAXException,
          IOException {
        DocumentBuilder builder = dbf.newDocumentBuilder();

        return builder.parse(new InputSource(new StringReader(xml))).getDocumentElement();
      }

      private void copyAttributesToElement(NamedNodeMap attributes, Element element) {
        for (int i = 0; i < attributes.getLength(); ++i) {
          Attr toCopy = (Attr)attributes.item(i);
          element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
      }

      private String memoizedToString = null;
      @Override
      public String toString() {
        if (memoizedToString != null) {
          return memoizedToString;
        }
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer;
        try {
          transformer = tf.newTransformer();
          transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
          StringWriter writer = new StringWriter();
          transformer.transform(new DOMSource(doc), new StreamResult(writer));
          memoizedToString = writer.getBuffer().toString().replaceAll("\n|\r", "");
        } catch (Exception e) {
          e.printStackTrace();
          memoizedToString = "";
        }

        return memoizedToString;
      }
    }

    private static class PostCommentDiscriminator {
      public enum Type {
        POST, COMMENT, UNKNOWN, INVALID;

        public static Type translate(char symbol) {
          if (symbol == 'P')
            return POST;
          if (symbol == 'C')
            return COMMENT;
          return UNKNOWN;
        }
      }

      private final Type type;

      public Type getType() {
        return type;
      }

      public String getData() {
        return data;
      }

      public boolean isUnrecordable() {
        return type == Type.INVALID || type == Type.UNKNOWN;
      }

      private final String data;

      private PostCommentDiscriminator(Type type, String data) {
        this.type = type;
        this.data = data;
      }

      public static PostCommentDiscriminator parse(String data) {
        if (MRDPUtils.isNullOrEmpty(data)) {
          return new PostCommentDiscriminator(Type.INVALID, "");
        }
        char symbol = data.charAt(0);
        if (data.length() == 1) {
          return new PostCommentDiscriminator(Type.translate(symbol), "");
        }

        data = data.substring(1).trim();
        return new PostCommentDiscriminator(Type.translate(symbol), data);
      }
    }
  }
}
