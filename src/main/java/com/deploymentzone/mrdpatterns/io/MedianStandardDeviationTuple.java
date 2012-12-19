package com.deploymentzone.mrdpatterns.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class MedianStandardDeviationTuple implements Writable {
  private float median;
  private float standardDeviation;

  public float getMedian() {
    return median;
  }

  public void setMedian(float median) {
    this.median = median;
  }

  public float getStandardDeviation() {
    return standardDeviation;
  }

  public void setStandardDeviation(float standardDeviation) {
    this.standardDeviation = standardDeviation;
  }

  public void set(float median, float standardDeviation) {
    setMedian(median);
    setStandardDeviation(standardDeviation);
  }

  public void deriveMedian(List<Float> collection) {
    Collections.sort(collection);

    if (collection.size() % 2 == 0) {
      Float lowerMedian = collection.get(collection.size() / 2 - 1);
      Float upperMedian = collection.get(collection.size() / 2);
      setMedian((lowerMedian + upperMedian) / 2.0f);
    } else {
      setMedian(collection.get(collection.size() / 2));
    }
  }

  public void deriveStandardDeviation(List<Float> collection) {
    float sum = 0;
    for (Float val : collection) {
      sum += val;
    }

    float mean = sum / collection.size();
    float sumOfSquares = 0.0f;
    for (Float f : collection) {
      sumOfSquares += (f - mean) * (f - mean);
    }

    setStandardDeviation((float) Math.sqrt(sumOfSquares / (collection.size() -1)));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    set(in.readFloat(), in.readFloat());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(getMedian());
    out.writeFloat(getStandardDeviation());
  }

  @Override
  public String toString() {
    return getMedian() + "\t" + getStandardDeviation();
  }

}
