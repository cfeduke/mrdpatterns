package com.deploymentzone.mrdpatterns.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public abstract class MinMaxCountTuple<T extends Comparable<T>> implements Writable {

  private T min;
  private T max;
  private long count = 0;

  public T getMin() {
    return min;
  }

  public void setMin(T min) {
    this.min = min;
  }

  public T getMax() {
    return max;
  }

  public void setMax(T max) {
    this.max = max;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public void reset() {
    setMax(null);
    setMin(null);
    setCount(0);
  }

  public void set(T min, T max, long count) {
    setMin(min);
    setMax(max);
    setCount(count);
  }

  public void deriveMinMax(MinMaxCountTuple<T> other) {
    if (this.getMin() == null || other.getMin().compareTo(this.getMin()) < 0) {
      this.setMin(other.getMin());
    }
    if (this.getMax() == null || other.getMax().compareTo(this.getMax()) > 0) {
      this.setMax(other.getMax());
    }
  }

  @Override
  public abstract void readFields(DataInput in) throws IOException;

  @Override
  public abstract void write(DataOutput out) throws IOException;


}
