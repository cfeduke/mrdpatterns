package com.deploymentzone.mrdpatterns.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class MinMaxCountTupleDate extends MinMaxCountTuple<Date> {

  public final static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss");

  @Override
  public void readFields(DataInput in) throws IOException {
    set(
      new Date(in.readLong()),
      new Date(in.readLong()),
      in.readLong()
    );
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(getMin().getTime());
    out.writeLong(getMax().getTime());
    out.writeLong(getCount());
  }

  @Override
  public String toString() {
    return FORMAT.format(getMin()) + "\t" + FORMAT.format(getMax()) + "\t" + getCount();
  }
}
