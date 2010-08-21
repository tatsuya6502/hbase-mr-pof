package hfileinput.v2;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public interface KeyValueSet {

    KeyValue2 put(KeyValue2 kv);

    void clear();

    int size();

    ImmutableBytesWritable getRow();

    ImmutableBytesWritable getValueAsBytesWritable(byte[] familyAndQualifier);

    String getValueAsString(byte[] familyAndQualifier);

    short getValueAsShort(byte[] familyAndQualifier);

    int getValueAsInt(byte[] familyAndQualifier);

    long getValueAsLong(byte[] familyAndQualifier);

    byte getValueAsByte(byte[] familyAndQualifier);

}
