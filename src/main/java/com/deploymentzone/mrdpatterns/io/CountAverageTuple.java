package com.deploymentzone.mrdpatterns.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable {
  private int count;
  private float average;

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public float getAverage() {
    return average;
  }

  public void setAverage(float average) {
    this.average = average;
  }

  public void set(int count, float average) {
    setCount(count);
    setAverage(average);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    set(in.readInt(), in.readFloat());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getCount());
    out.writeFloat(getAverage());
  }

  @Override
  public String toString() {
    return getCount() + "\t" + getAverage();
  }

}
