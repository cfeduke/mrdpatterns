package com.deploymentzone.mrdpatterns.io;

import static com.deploymentzone.mrdpatterns.utils.MRDPUtils.DATE_FORMAT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public final class MinMaxCountTupleDate extends MinMaxCountTuple<Date> {

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
    return DATE_FORMAT.format(getMin()) + "\t" + DATE_FORMAT.format(getMax()) + "\t" + getCount();
  }
}
