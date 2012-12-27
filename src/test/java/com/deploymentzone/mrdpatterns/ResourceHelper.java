package com.deploymentzone.mrdpatterns;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ResourceHelper {
  public static List<String> getLinesFromResourceFile(Class clazz, String resourceFileName) throws IOException {
    InputStream is = null;
    InputStreamReader isr = null;
    BufferedReader br = null;
    List<String> result = new ArrayList<String>();
    try {
      is = clazz.getResourceAsStream(resourceFileName);
      isr = new InputStreamReader(is);
      br = new BufferedReader(isr);
      String line;
      while ((line = br.readLine()) != null) {
        result.add(line);
      }
    }
    finally {
      if (br != null) br.close();
      if (isr != null) isr.close();
      if (is != null) is.close();
    }

    return result;
  }
}
